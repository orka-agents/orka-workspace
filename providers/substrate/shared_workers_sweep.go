package substrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// sharedProfileShape is the scope and operator pool an opted-in profile's
// template-sized allocations would share.
type sharedProfileShape struct {
	scope  sharedScope
	source *unstructured.Unstructured
	warm   int
}

// enforceSharedBounds runs first in every sweep and uses Kubernetes alone,
// so neither an unavailable native control plane nor slow native discovery
// can postpone it. Each shared pool is strongly retired when its strong
// retirement time arrives or when the install is no longer asserted
// dedicated, and a worker Pod still present when the hard bound nears is
// re-deleted with a one-second grace.
func (r *SpareWorkerReplenisher) enforceSharedBounds(ctx context.Context, d *Lifecycle) error {
	d = d.withSharedPass()
	// Escalations due by the latest bound any provenance could give go out
	// first, while discovery runs alongside under its own deadline.
	discovered := make(chan sharedDiscovery, 1)
	go func() { discovered <- d.discoverShared(ctx, true) }()
	escalatedEarly, earlyErr := d.escalateDueByAnnotations(ctx)
	found := <-discovered
	// The outage state comes first, independent of the discovery budget and
	// of the pool List: it is the journal reads' own record.
	outage := found.provenanceErr != nil && sharedProvenanceHealth.outlasted(sharedNow())
	if found.listErr != nil {
		// Nothing can be retired by name this pass. The loop retries within
		// its 10 s period, and a failed journal read keeps admission closed.
		if outage {
			ctrl.LoggerFrom(ctx).Error(found.listErr, "Substrate shared worker provenance outage outlasted its bound, but the shared pools could not be listed to retire them")
		}
		return errors.Join(earlyErr, found.listErr, found.provenanceErr)
	}
	provenance, provenanceErr, pools := found.provenance, found.provenanceErr, found.pools
	errs := append([]error{earlyErr}, found.errs...)
	if provenanceErr != nil {
		// Unavailable provenance closes admission at once. Inside the outage
		// window the annotations' 24h-capped bounds apply; past it, every
		// pool is bounded by creation plus the minimum and strongly retired
		// (sharedPassBounds, outage).
		errs = append(errs, provenanceErr)
	}
	// A short outage only closes admission. One that outlasts its bound
	// is doubt about every pool, including a strong marker a tampered entry
	// might hide, so every pool is then strongly retired (outage above).
	// Phase 0 decides, reading only Kubernetes and re-reading the clock per
	// pool. Phase 1 issues every due escalation first, each Pod delete on its
	// own short context. Phase 2 issues the pool Deletes concurrently, capped,
	// so one slow DELETE never holds up the others. Phase 3 is the
	// best-effort bookkeeping within its own total budget. Escalation also
	// runs in its own short-period loop (RunSharedEscalation) that never waits
	// on pool deletes.
	// The decision phase's reads (hosted-journal and workspace checks)
	// share one bounded deadline, so a stalled read can delay decisions by
	// at most that bound, never indefinitely.
	decideCtx, cancelDecide := context.WithTimeout(ctx, sharedDiscoveryTimeout)
	defer cancelDecide()
	type retirement struct {
		pool          *unstructured.Unstructured
		state, reason string
		// escalateAt is the pass's hard-bound escalation time for the pool,
		// which caps its worker Pods' deletion grace.
		escalateAt time.Time
	}
	var retirements []retirement
	var escalations []*unstructured.Unstructured
	for i := range pools {
		pool := &pools[i]
		recorded := provenance[pool.GetUID()]
		retireAt, escalateAt, evidence := sharedPassBounds(pool, recorded, provenanceErr, outage)
		now := sharedNow()
		if !now.Before(escalateAt) && !escalatedEarly[pool.GetUID()] {
			escalations = append(escalations, pool)
		}
		if pool.GetDeletionTimestamp() != nil {
			continue
		}
		retire := func(state, reason string) {
			retirements = append(retirements, retirement{pool: pool, state: state, reason: reason, escalateAt: escalateAt})
		}
		switch {
		case outage:
			retire(sharedRetiring, "the shared worker's journal provenance stayed unavailable past its bound")
		case !sharedLabelsIntact(pool):
			retire(sharedRetiring, "the shared worker carries shared evidence without its provider-owned label")
		case !r.Config.DedicatedInstall:
			retire(sharedRetiring, errSharedWorkersNotDedicated.Error())
		case !evidence:
			retire(sharedRetiring, "the shared worker's lifetime evidence is unreadable or inconsistent")
		case recorded.strong:
			// A journal that recorded this pool's UID holds an unreleased
			// strong marker or failure, whatever the mutable entry list now
			// says.
			retire(sharedRetiring, "a journal that recorded this shared worker selected the strong path or failed")
		case recorded.suspect:
			// The workspace of an unreleased journal that recorded this pool's
			// UID is suspect, even if its entry was removed from the ledger.
			retire(sharedRetiring, "a workspace a journal recorded on this shared worker is tainted, quarantined or failed")
		case sharedRetireIntended(pool):
			// A retirement decided earlier whose Delete failed.
			retire(sharedRetiring, pool.GetAnnotations()[sharedRetireIntentAnnotation])
		case sharedRetirementPersisted(pool):
			// A retirement state saved before a crash, or before a native
			// sweep ran out of time, is unconditional deletion intent.
			state, reason := sharedPersistedRetirement(pool)
			retire(state, reason)
		case !now.Before(retireAt):
			// Checked before any hosted read, so a due pool never waits on one.
			retire(sharedRetiring, "the shared worker reached the start of its strong retirement before maxWorkerLifetime")
		case d.sharedStrongMarked(decideCtx, pool):
			// A hosted journal persisted the strong path or a failure
			// (possibly crashing before it deleted the pool), or a hosted
			// workspace became suspect; act on it with no native read.
			retire(sharedRetiring, "a hosted allocation selected the strong path or its workspace is suspect")
		}
	}
	errs = append(errs, d.escalateSharedPools(ctx, escalations, provenance))
	var mu sync.Mutex
	var deleted []retirement
	work := make(chan retirement)
	var group sync.WaitGroup
	for range min(sharedDeleteConcurrency, len(retirements)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for item := range work {
				_, err := d.deleteSharedPool(ctx, item.pool, item.escalateAt)
				if err != nil {
					// The decision survives a failed Delete: admission
					// refuses the pool and the next pass retries.
					// It also records the state, so bookkeeping skips it.
					d.persistSharedRetireDecision(ctx, item.pool, item.state, item.reason, err)
				}
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					deleted = append(deleted, item)
				}
				mu.Unlock()
			}
		}()
	}
	for _, item := range retirements {
		work <- item
	}
	close(work)
	group.Wait()
	if len(deleted) > 0 {
		budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedBookkeepingBudget)
		defer cancel()
		for _, item := range deleted {
			if budget.Err() != nil {
				break
			}
			ctrl.LoggerFrom(ctx).Info("Substrate shared worker retired", "state", item.state, "reason", item.reason)
			writeCtx, cancelWrite := context.WithTimeout(budget, sharedLedgerWriteTimeout)
			d.recordSharedRetirement(writeCtx, item.pool, item.state, item.reason)
			cancelWrite()
		}
	}
	return errors.Join(errs...)
}

// listSharedPools lists every WorkerPool that carries provider-written
// shared-pool evidence: the provider-owned label, the ledger or deadline
// annotation, or a provider journal or archived record that recorded its UID.
// Discovery never depends on a removable label alone, and a name alone is
// never evidence: an operator pool named ate-shared-... is never touched.
func listSharedPools(ctx context.Context, c client.Client, provenance map[types.UID]sharedProvenance) ([]unstructured.Unstructured, error) {
	all := &unstructured.UnstructuredList{}
	all.SetAPIVersion(workerPoolAPIVersion())
	all.SetKind("WorkerPoolList")
	if err := c.List(ctx, all); err != nil {
		return nil, err
	}
	var out []unstructured.Unstructured
	for _, pool := range all.Items {
		_, labeled := pool.GetLabels()[sharedPoolLabel]
		_, ledger := pool.GetAnnotations()[sharedLedgerAnnotation]
		_, deadline := pool.GetAnnotations()[sharedDeadlineAnnotation]
		if labeled || ledger || deadline || provenance[pool.GetUID()].found {
			out = append(out, pool)
		}
	}
	return out, nil
}

// sharedLabelsIntact reports whether a shared pool still carries exactly its
// provider-owned label. Shared evidence without it is doubt.
func sharedLabelsIntact(pool *unstructured.Unstructured) bool {
	label := pool.GetLabels()[sharedPoolLabel]
	return label != "" && maps.Equal(pool.GetLabels(), sharedLabels(label))
}

// sharedProvenance is what the journals that recorded a pool's UID froze
// about it, independently of the pool's own mutable annotations.
type sharedProvenance struct {
	found bool
	// lifetime is the shortest recorded lifetime.
	lifetime time.Duration
	scopes   []sharedScope
	// strong is an unreleased recorded Strong marker or instance failure.
	strong bool
	// podUIDs are the worker Pod UIDs journals fenced on this pool.
	podUIDs map[string]bool
	// workspaces are the workspaces of the unreleased journals that recorded
	// this pool, whatever the pool's mutable entry list says.
	workspaces []sdk.AllocationKey
	// suspect is set when one of those workspaces is tainted, quarantined or
	// Failed (sharedProvenanceSuspects).
	suspect bool
	// profileLifetime bounds a pool no journal recorded (for example an
	// unused warm pool): the maxWorkerLifetime of the profile its ledger
	// scope names, when that profile's UID and hash still match. Zero means
	// unresolved, and the pool is then bounded by the minimum lifetime.
	profileLifetime time.Duration
}

// sharedJournalProvenance reads every journal and archived record (Kubernetes
// only) and groups the frozen scopes they recorded by shared pool UID.
func (d *Lifecycle) sharedJournalProvenance(ctx context.Context) (map[types.UID]sharedProvenance, error) {
	journals := &corev1.ConfigMapList{}
	// Health updates carry the time their read started, so a slow read never
	// overrides a newer one's result.
	started := sharedNow()
	// Once this process has confirmed every shared record carries the shared
	// label, only labelled records are listed: a private-only install then
	// lists and decodes none.
	labelledOnly := sharedRecordsLabelled.Load()
	requirements := []string{ownershipLabel, providerLabel}
	if labelledOnly {
		requirements = append(requirements, sharedRecordLabel+"=true")
	}
	selector, err := klabels.Parse(strings.Join(requirements, ","))
	if err != nil {
		return nil, err
	}
	if err := d.client.List(ctx, journals, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		sharedProvenanceHealth.failed(started)
		return nil, err
	}
	defer sharedProvenanceHealth.succeeded(started)
	d.sharedPass.noteJournals(journals.Items)
	out := map[types.UID]sharedProvenance{}
	unlabelled := false
	for i := range journals.Items {
		record, ok := decodeClaimRecord(&journals.Items[i])
		if ok && record.SharedWorker != nil && journals.Items[i].Labels[sharedRecordLabel] != "true" && !labelledOnly {
			// A shared record written before the label existed.
			if !d.labelSharedRecord(ctx, &journals.Items[i]) {
				unlabelled = true
			}
		}
		if !ok || record.SharedWorker == nil || record.SharedWorker.UID == "" {
			continue
		}
		uid := types.UID(record.SharedWorker.UID)
		entry := out[uid]
		scope := record.SharedWorker.Scope
		if !entry.found || scope.Lifetime.Duration < entry.lifetime {
			entry.lifetime = scope.Lifetime.Duration
		}
		entry.found = true
		entry.scopes = append(entry.scopes, scope)
		fences := append([]workerFence{}, record.RetirementWorkers...)
		if record.Worker != nil {
			fences = append(fences, *record.Worker)
		}
		for _, fence := range fences {
			if fence.PodUID != "" {
				if entry.podUIDs == nil {
					entry.podUIDs = map[string]bool{}
				}
				entry.podUIDs[fence.PodUID] = true
			}
		}
		if !record.SharedWorker.Released && (record.SharedWorker.Strong != "" || record.Failure != "") {
			entry.strong = true
		}
		if !record.SharedWorker.Released {
			entry.workspaces = append(entry.workspaces, record.Request.Key)
		}
		out[uid] = entry
	}
	if !labelledOnly && !unlabelled {
		sharedRecordsLabelled.Store(true)
	}
	return out, nil
}

// sharedProfileProvenance gives every pool no journal recorded an
// independent lifetime from the profile its ledger scope names. The ledger is
// mutable, so the scope counts only if the pool's immutable name commits to
// it; a pool whose name commits to nothing (created before the scheme) gets
// no profile bound. Profiles are immutable and hashed: the profile in the
// scope's namespace with the scope's UID is read (one List per namespace),
// and its maxWorkerLifetime applies only if its hash still equals the
// scope's. A profile that is gone, mismatched, unreadable or no longer opted
// in leaves the pool bounded by the minimum lifetime, so rewriting a pool's
// annotations can never lengthen it.
func (d *Lifecycle) sharedProfileProvenance(ctx context.Context, pools []unstructured.Unstructured, provenance map[types.UID]sharedProvenance) error {
	wanted := map[string][]*unstructured.Unstructured{}
	for i := range pools {
		pool := &pools[i]
		if provenance[pool.GetUID()].found {
			continue
		}
		if ledger, err := readSharedLedger(pool); err == nil {
			if _, bound := sharedNameBinding(pool, ledger.Scope); bound {
				wanted[ledger.Scope.Namespace] = append(wanted[ledger.Scope.Namespace], pool)
			}
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	// One cluster-wide List, whatever the number of namespaces.
	profiles := &unstructured.UnstructuredList{}
	profiles.SetGroupVersionKind(profile.GroupVersion.WithKind("SubstrateWorkspaceProfileList"))
	if err := d.client.List(ctx, profiles); err != nil {
		return err
	}
	lifetimes := map[string]map[types.UID]map[string]time.Duration{}
	for i := range profiles.Items {
		raw := &profiles.Items[i]
		if wanted[raw.GetNamespace()] == nil {
			continue
		}
		hash, err := sdk.ParametersProfileHash(raw)
		if err != nil {
			continue
		}
		item := &profile.SubstrateWorkspaceProfile{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, item); err != nil || item.Validate() != nil || item.Spec.Placement == nil || item.Spec.Placement.SharedWorkers == nil {
			continue
		}
		byUID := lifetimes[raw.GetNamespace()]
		if byUID == nil {
			byUID = map[types.UID]map[string]time.Duration{}
			lifetimes[raw.GetNamespace()] = byUID
		}
		if byUID[raw.GetUID()] == nil {
			byUID[raw.GetUID()] = map[string]time.Duration{}
		}
		byUID[raw.GetUID()][hash] = item.Spec.Placement.SharedWorkers.WorkerLifetime()
	}
	for namespace, namespacePools := range wanted {
		for _, pool := range namespacePools {
			ledger, _ := readSharedLedger(pool)
			if lifetime, found := lifetimes[namespace][types.UID(ledger.Scope.ProfileUID)][ledger.Scope.ProfileHash]; found {
				entry := provenance[pool.GetUID()]
				entry.profileLifetime = lifetime
				provenance[pool.GetUID()] = entry
			}
		}
	}
	return nil
}

// sharedProvenanceSuspects marks every pool whose unreleased journals record
// a workspace that is tainted, quarantined or Failed, under the journal's
// exact workspace UID. It reads the workspaces with one List per namespace,
// so the bounds pass stays cheap, and checks them whatever the pool's mutable
// entry list says: removing an entry cannot hide a suspect workspace. A
// namespace whose List fails is reported and leaves its pools unmarked; their
// entries are still checked one by one.
func (d *Lifecycle) sharedProvenanceSuspects(ctx context.Context, provenance map[types.UID]sharedProvenance) error {
	namespaces := map[string]bool{}
	for _, entry := range provenance {
		for _, key := range entry.workspaces {
			namespaces[key.Namespace] = true
		}
	}
	err := d.prefetchSharedWorkspaces(ctx, namespaces)
	for uid, entry := range provenance {
		for _, key := range entry.workspaces {
			workspace := &api.ExecutionWorkspace{}
			if d.sharedWorkspace(ctx, clientKey(key), workspace) == nil && workspace.UID == key.WorkspaceUID && workspaceSuspect(workspace) {
				entry.suspect = true
			}
		}
		provenance[uid] = entry
	}
	return err
}

// sharedListConcurrency caps a pass's concurrent per-namespace Lists.
const sharedListConcurrency = 8

// prefetchSharedWorkspaces lists each namespace's ExecutionWorkspaces into
// the pass's snapshot concurrently, capped, and reports the failed Lists.
func (d *Lifecycle) prefetchSharedWorkspaces(ctx context.Context, namespaces map[string]bool) error {
	if d.sharedPass == nil {
		return nil
	}
	var mu sync.Mutex
	var errs []error
	work := make(chan string)
	var group sync.WaitGroup
	for range min(sharedListConcurrency, len(namespaces)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for namespace := range work {
				workspaces := &api.ExecutionWorkspaceList{}
				if err := d.client.List(ctx, workspaces, client.InNamespace(namespace)); err != nil {
					d.sharedPass.noteWorkspaceListFailed(namespace)
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					continue
				}
				d.sharedPass.noteWorkspaces(namespace, workspaces.Items)
			}
		}()
	}
	for namespace := range namespaces {
		work <- namespace
	}
	close(work)
	group.Wait()
	return errors.Join(errs...)
}

// sharedRecordsLabelled is set, in process, once a full inventory scan has
// labelled or confirmed the shared label on every shared record. A new
// process (including a newly elected leader) scans in full again once.
var sharedRecordsLabelled atomic.Bool

// labelSharedRecord adds the shared label to one record written before it
// existed, preconditioned on the listed resourceVersion, and reports
// success. A failure leaves the full scan in place for the next pass.
func (d *Lifecycle) labelSharedRecord(ctx context.Context, cm *corev1.ConfigMap) bool {
	labeled := cm.DeepCopy()
	if labeled.Labels == nil {
		labeled.Labels = map[string]string{}
	}
	labeled.Labels[sharedRecordLabel] = "true"
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedLedgerWriteTimeout)
	defer cancel()
	return d.client.Update(writeCtx, labeled) == nil
}

// sharedProvenanceFreshness is how recent a successful journal inventory read
// must be for a shared pool to admit a claim or a Resume. The bounds loop
// reads it every 10 s; admission reads it itself when it is older.
const sharedProvenanceFreshness = 30 * time.Second

// sharedProvenanceOutageBound is how long the journal inventory may stay
// unavailable before every shared pool is strongly retired as doubt.
const sharedProvenanceOutageBound = 60 * time.Second

// provenanceHealth tracks, in process, the leader's journal inventory reads:
// when one last succeeded, and since when they have been failing. A new
// leader starts with neither, so it admits nothing until its own read
// succeeds, and its outage bound starts from its own first failure.
//
// Every update carries the time its read started. A success clears a
// failure only if it started after the latest failure started, so a slow
// success that finishes after a newer failure never reopens admission. A
// failure always counts (fail closed), whenever it started.
type provenanceHealth struct {
	mu sync.Mutex
	// verifiedAt is the start of the latest success that counted.
	verifiedAt time.Time
	// failingSince is the start of the first failure of the current outage;
	// lastFailure the start of the latest failure.
	failingSince, lastFailure time.Time
}

var sharedProvenanceHealth = &provenanceHealth{}

func (h *provenanceHealth) succeeded(started time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.lastFailure.IsZero() && !started.After(h.lastFailure) {
		return
	}
	if started.After(h.verifiedAt) {
		h.verifiedAt = started
	}
	h.failingSince = time.Time{}
}

func (h *provenanceHealth) failed(started time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if started.After(h.lastFailure) {
		h.lastFailure = started
	}
	if h.failingSince.IsZero() || started.Before(h.failingSince) {
		h.failingSince = started
	}
}

// fresh reports a recent successful read with no failure since.
func (h *provenanceHealth) fresh(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failingSince.IsZero() && !h.verifiedAt.IsZero() && now.Sub(h.verifiedAt) < sharedProvenanceFreshness
}

// outlasted reports reads failing for at least the outage bound.
func (h *provenanceHealth) outlasted(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.failingSince.IsZero() && now.Sub(h.failingSince) >= sharedProvenanceOutageBound
}

func (h *provenanceHealth) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.verifiedAt, h.failingSince, h.lastFailure = time.Time{}, time.Time{}, time.Time{}
}

// requireSharedProvenance closes admission (a new claim, a new pool, or a
// Resume onto a shared pool) unless the journal inventory was read
// successfully within the freshness window. Without it the pool's lifetime
// rests on its mutable annotations alone, so admission fails closed; the pool
// is not retired for that until the outage bound passes.
func (d *Lifecycle) requireSharedProvenance(ctx context.Context) error {
	if sharedProvenanceHealth.fresh(sharedNow()) {
		return nil
	}
	if _, err := d.sharedJournalProvenance(ctx); err != nil {
		return fmt.Errorf("shared worker provenance is unavailable, so shared admission is closed: %w", err)
	}
	// A successful read admits only if it established fresh health: a read
	// that started before a newer failure does not count, and one that took
	// longer than the freshness window is already stale.
	if !sharedProvenanceHealth.fresh(sharedNow()) {
		return errors.New("shared worker provenance is not freshly verified, so shared admission is closed")
	}
	return nil
}

// sharedDeleteConcurrency caps concurrent pool Deletes in one bounds pass.
const sharedDeleteConcurrency = 8

// sharedEscalationTimeout bounds each escalation call: a pool's Pod list or
// one escalating Pod delete.
const sharedEscalationTimeout = 10 * time.Second

// sharedEscalationConcurrency caps concurrent per-pool escalations. With one
// worker Pod per pool, a pool takes at most three calls (its Pod List, a
// pool read and one Pod delete, 3 x 10s), so within the 5-minute margin 16
// workers cover 16 x 300s / 30s = 160 due pools even when every call stalls
// to its timeout.
const sharedEscalationConcurrency = 16

// escalateSharedPools escalates the worker Pods of every due pool
// concurrently, capped, each call on its own short context, so stalled Pod
// DELETEs cannot push later escalations past the lifetime margin.
func (d *Lifecycle) escalateSharedPools(ctx context.Context, pools []*unstructured.Unstructured, provenance map[types.UID]sharedProvenance) error {
	var mu sync.Mutex
	var errs []error
	work := make(chan *unstructured.Unstructured)
	var group sync.WaitGroup
	for range min(sharedEscalationConcurrency, len(pools)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for pool := range work {
				if err := d.deleteSharedWorkerPods(ctx, pool, provenance[pool.GetUID()].podUIDs, sharedEscalationGrace, "the worker Pod outlived its strong retirement"); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
	for _, pool := range pools {
		work <- pool
	}
	close(work)
	group.Wait()
	return errors.Join(errs...)
}

// sharedEscalationPeriod is how often the escalation-only loop runs.
const sharedEscalationPeriod = 10 * time.Second

// EnforceSharedEscalation runs one Kubernetes-only escalation pass: every
// shared pool past its escalation time has its worker Pods re-deleted with a
// one-second grace. It never issues or waits on a pool Delete.
func (r *SpareWorkerReplenisher) EnforceSharedEscalation(ctx context.Context) error {
	d := New(r.Client, nil, r.Config).withSharedPass()
	discovered := make(chan sharedDiscovery, 1)
	go func() { discovered <- d.discoverShared(ctx, false) }()
	escalatedEarly, earlyErr := d.escalateDueByAnnotations(ctx)
	found := <-discovered
	if found.listErr != nil {
		return errors.Join(earlyErr, found.listErr)
	}
	errs := append([]error{earlyErr}, found.errs...)
	if found.provenanceErr != nil {
		errs = append(errs, found.provenanceErr)
	}
	outage := found.provenanceErr != nil && sharedProvenanceHealth.outlasted(sharedNow())
	// Provenance can only bring an escalation earlier than the bound the
	// first step used, so this adds the escalations it made due.
	var due []*unstructured.Unstructured
	for i := range found.pools {
		pool := &found.pools[i]
		if escalatedEarly[pool.GetUID()] {
			continue
		}
		if _, escalateAt, _ := sharedPassBounds(pool, found.provenance[pool.GetUID()], found.provenanceErr, outage); !sharedNow().Before(escalateAt) {
			due = append(due, pool)
		}
	}
	errs = append(errs, d.escalateSharedPools(ctx, due, found.provenance))
	return errors.Join(errs...)
}

// sharedDiscoveryTimeout bounds one pass's provenance discovery: the journal
// inventory, the pool List, the profile List and the workspace Lists. A
// discovery that runs out of it reports unavailable provenance.
var sharedDiscoveryTimeout = 60 * time.Second

// sharedDiscovery is one pass's provenance and pool inventory.
type sharedDiscovery struct {
	provenance    map[types.UID]sharedProvenance
	provenanceErr error
	pools         []unstructured.Unstructured
	listErr       error
	errs          []error
}

// discoverShared reads the pass's provenance under its own deadline: the
// journal inventory, the shared pools, each journal-less pool's profile and,
// for the bounds pass (suspects), each recorded workspace.
//
// The pool List has its own deadline, never the provenance reads' remainder:
// a journal List that used up the discovery deadline must still leave the
// pools listed, so the unavailable-provenance bound and the outage policy
// are applied to them.
func (d *Lifecycle) discoverShared(parent context.Context, suspects bool) sharedDiscovery {
	ctx, cancel := context.WithTimeout(parent, sharedDiscoveryTimeout)
	defer cancel()
	var out sharedDiscovery
	out.provenance, out.provenanceErr = d.sharedJournalProvenance(ctx)
	listCtx, cancelList := context.WithTimeout(parent, sharedDiscoveryTimeout)
	out.pools, out.listErr = listSharedPools(listCtx, d.client, out.provenance)
	cancelList()
	if out.listErr != nil || out.provenanceErr != nil {
		return out
	}
	if err := d.sharedProfileProvenance(ctx, out.pools, out.provenance); err != nil {
		out.errs = append(out.errs, err)
	}
	if suspects {
		// Each recorded workspace's suspicion, independent of the entries.
		if err := d.sharedProvenanceSuspects(ctx, out.provenance); err != nil {
			out.errs = append(out.errs, err)
		}
	}
	return out
}

// sharedPassBounds is a pass's bounds for a pool. With provenance, they are
// sharedBoundsOf's. While the journal inventory is unavailable but the
// outage has not outlasted its bound, they are the annotations' bounds under
// the 24h ceiling (admission is already closed), so one transient failed List
// never retires or escalates a healthy pool. Only an outage past its bound
// applies the creation-plus-3h ceiling, together with the strong retirement
// of every shared pool.
func sharedPassBounds(pool *unstructured.Unstructured, provenance sharedProvenance, provenanceErr error, outage bool) (time.Time, time.Time, bool) {
	if provenanceErr != nil && !outage {
		return sharedBoundsOf(pool, sharedProvenance{found: true, lifetime: profile.MaxSharedWorkerLifetime}, true)
	}
	return sharedBoundsOf(pool, provenance, provenanceErr == nil)
}

// sharedPoolListTimeout bounds the escalation step's own pool List.
var sharedPoolListTimeout = 30 * time.Second

// sharedLatestEscalateAt is the latest escalation time any provenance could
// give a pool: its annotations' bound under the 24h ceiling. Journal and
// profile provenance can only bring it earlier, never later. It is not the
// unavailable-provenance bound of creation plus 3h, which would escalate a
// healthy longer-lived pool before its retirement and eviction.
func sharedLatestEscalateAt(pool *unstructured.Unstructured) time.Time {
	_, escalateAt, _ := sharedBoundsOf(pool, sharedProvenance{found: true, lifetime: profile.MaxSharedWorkerLifetime}, true)
	return escalateAt
}

// escalateDueByAnnotations escalates every pool already due by
// sharedLatestEscalateAt, without waiting for provenance, and returns them.
func (d *Lifecycle) escalateDueByAnnotations(ctx context.Context) (map[types.UID]bool, error) {
	listCtx, cancel := context.WithTimeout(ctx, sharedPoolListTimeout)
	pools, err := listSharedPools(listCtx, d.client, nil)
	cancel()
	if err != nil {
		return nil, err
	}
	escalated := map[types.UID]bool{}
	var due []*unstructured.Unstructured
	for i := range pools {
		pool := &pools[i]
		if !sharedNow().Before(sharedLatestEscalateAt(pool)) {
			due = append(due, pool)
			escalated[pool.GetUID()] = true
		}
	}
	return escalated, d.escalateSharedPools(ctx, due, nil)
}

// RunSharedEscalation runs EnforceSharedEscalation every period until ctx
// ends, independently of the bounds loop's pool deletes.
func (r *SpareWorkerReplenisher) RunSharedEscalation(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		if err := r.EnforceSharedEscalation(ctx); err != nil && ctx.Err() == nil {
			ctrl.LoggerFrom(ctx).Error(err, "Substrate shared worker escalation failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sharedBookkeepingBudget bounds the total time one bounds pass spends on
// best-effort retirement bookkeeping, after all mandatory work.
const sharedBookkeepingBudget = 30 * time.Second

// sharedBoundsOf returns when a pool's strong retirement starts and when its
// worker Pods are escalated, and whether its lifetime evidence is consistent.
// The deadline never exceeds the server-set creation time plus the maximum
// lifetime, nor the creation time plus the shortest lifetime any journal
// recorded for the pool. A ledger or deadline annotation that disagrees with
// those journals (or with each other, or is unreadable) is doubt: retirement
// starts at once and escalation follows the shortest bound, never longer.
//
// When the journal inventory itself is unavailable (known is false), the
// ceiling is the creation time plus the minimum lifetime.
func sharedBoundsOf(pool *unstructured.Unstructured, provenance sharedProvenance, known bool) (time.Time, time.Time, bool) {
	created := pool.GetCreationTimestamp().Time
	ceiling := created.Add(profile.MaxSharedWorkerLifetime)
	if !known {
		ceiling = created.Add(profile.MinSharedWorkerLifetime)
	}
	switch {
	case provenance.found:
		ceiling = minTime(ceiling, created.Add(provenance.lifetime))
	case provenance.profileLifetime > 0:
		// No journal recorded the pool: its profile bounds it.
		ceiling = minTime(ceiling, created.Add(provenance.profileLifetime))
	default:
		// Neither a journal nor a matching profile bounds it.
		ceiling = minTime(ceiling, created.Add(profile.MinSharedWorkerLifetime))
	}
	ledger, err := readSharedLedger(pool)
	recorded, recordedOK := sharedRecordedDeadline(pool)
	consistent := err == nil && recordedOK && recorded.Equal(ledger.BornAt.Add(ledger.Scope.Lifetime.Duration))
	if consistent {
		// A name that commits to another scope is doubt.
		if committed, bound := sharedNameBinding(pool, ledger.Scope); committed && !bound {
			consistent = false
		}
	}
	if consistent {
		for _, scope := range provenance.scopes {
			if scope != ledger.Scope {
				consistent = false
			}
		}
	}
	if consistent {
		deadline := minTime(sharedDeadline(pool, ledger), ceiling)
		return deadline.Add(-workerPodTerminationGrace - sharedRetireMargin), deadline.Add(-sharedRetireMargin), true
	}
	deadline := minTime(created.Add(profile.MinSharedWorkerLifetime), ceiling)
	if recordedOK {
		deadline = minTime(deadline, recorded)
	}
	return time.Time{}, deadline.Add(-sharedRetireMargin), false
}

// deleteSharedWorkerPods deletes the pool's worker Pods with grace, skipping
// any whose deletion already carries a grace no longer than that. Escalation
// re-deletes them with a one-second grace once the hard bound nears. By then
// the worker has had its
// full drain and Orka core's eviction window has ended, so every neighbour was
// suspended or failed without replay. A one-second grace keeps the absence
// proof kubelet-confirmed, which a zero grace would not.
//
// It does not wait for the pool's deletion to have started: a deletion that
// failed must not postpone the bound.
//
// Every Pod it deletes is tied to this pool's UID, never to its name alone,
// so a pool replaced under the same name never loses its Pods. The Pods are
// listed by the pool-name label, then the pool is read again: the pass's
// List saw this UID before the Pod List and the read sees it after, and a
// UID is never reused, so the name held this pool throughout and every
// listed Pod is this pool's or older. If the pool is gone or replaced, only
// Pods whose UID the pool's ledger or a journal fenced on this pool are
// deleted. Each delete is preconditioned on the listed Pod's UID.
//
// Retirement uses the same deletion, first, with the grace the time left
// allows (deleteSharedPool).
func (d *Lifecycle) deleteSharedWorkerPods(ctx context.Context, pool *unstructured.Unstructured, recorded map[string]bool, grace int64, reason string) error {
	pods := &corev1.PodList{}
	listCtx, cancelList := context.WithTimeout(context.WithoutCancel(ctx), sharedEscalationTimeout)
	err := d.client.List(listCtx, pods, client.InNamespace(pool.GetNamespace()), client.MatchingLabels{workerPoolLabel: pool.GetName()})
	cancelList()
	if err != nil {
		return err
	}
	if len(pods.Items) == 0 {
		return nil
	}
	current := poolObject()
	getCtx, cancelGet := context.WithTimeout(context.WithoutCancel(ctx), sharedEscalationTimeout)
	err = d.client.Get(getCtx, client.ObjectKeyFromObject(pool), current)
	cancelGet()
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	held := err == nil && current.GetUID() == pool.GetUID()
	if !held {
		if ledger, err := readSharedLedger(pool); err == nil && ledger.Worker != nil && ledger.Worker.PodUID != "" {
			recorded = maps.Clone(recorded)
			if recorded == nil {
				recorded = map[string]bool{}
			}
			recorded[ledger.Worker.PodUID] = true
		}
	}
	var errs []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !held && !recorded[string(pod.UID)] {
			continue
		}
		if pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds <= grace {
			continue
		}
		if grace == sharedEscalationGrace {
			ctrl.LoggerFrom(ctx).Info("Substrate shared worker Pod escalated before its hard lifetime bound", "reason", reason)
		} else {
			ctrl.LoggerFrom(ctx).Info("Substrate shared worker Pod deleted with a grace bounded by its hard lifetime bound", "graceSeconds", grace, "reason", reason)
		}
		deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedEscalationTimeout)
		err := d.client.Delete(deleteCtx, pod, client.Preconditions{UID: &pod.UID}, client.GracePeriodSeconds(grace))
		cancel()
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// sweepShared maintains every shared pool on the elected leader: it
// quarantines a pool hosting an Actor outside its ledger or reachable by a
// foreign template, strongly retires a pool whose worker was replaced or
// restarted or that hosts a tainted, quarantined or failed workspace, closes
// admission when the pool's lifetime, profile or operator pool no longer
// permits it, collects orphaned entries and idle pools, and keeps each
// profile's requested warm slots. With no opted-in profile and no shared pool
// it makes no native call.
func (r *SpareWorkerReplenisher) sweepShared(ctx context.Context, d *Lifecycle, profiles []profile.SubstrateWorkspaceProfile, workers *spareWorkerCache) error {
	// One snapshot of journals and workspaces is shared by every pool.
	d = d.withSharedPass()
	discovered, err := listSharedPools(ctx, r.Client, nil)
	if err != nil {
		return err
	}
	// Pools without their intact provider-owned label are retired by the
	// bounds loop; native maintenance never touches them.
	pools := &unstructured.UnstructuredList{}
	for _, pool := range discovered {
		if sharedLabelsIntact(&pool) {
			pools.Items = append(pools.Items, pool)
		}
	}
	shapes := map[string]*sharedProfileShape{}
	var errs []error
	for i := range profiles {
		item := &profiles[i]
		if item.Spec.Placement == nil || item.Spec.Placement.SharedWorkers == nil || item.DeletionTimestamp != nil || item.Validate() != nil {
			continue
		}
		if !r.Config.DedicatedInstall {
			ctrl.LoggerFrom(ctx).Error(errSharedWorkersNotDedicated, "Substrate shared-worker profile refused", "profileNamespace", item.Namespace, "profileName", item.Name)
			continue
		}
		shape, err := d.profileSharedShape(ctx, item)
		if err != nil {
			ctrl.LoggerFrom(ctx).Info("Substrate shared workers paused for profile", "profileNamespace", item.Namespace, "profileName", item.Name, "reason", err.Error())
			continue
		}
		shape.warm, _ = spareWorkerCount(item.Annotations)
		shapes[string(item.UID)] = shape
	}
	if len(pools.Items) == 0 && len(shapes) == 0 {
		// Nothing shared remains, so the in-process template record is
		// pruned; a later shared check rereads it from Kubernetes.
		recordedTemplates.replace(nil, recordedTemplates.begin())
		return nil
	}
	now := sharedNow()
	// Maintenance and every pool creation need a complete, successful template
	// inventory; a failed inventory creates nothing.
	templates, templatesErr := d.listAllActorTemplates(ctx)
	began := recordedTemplates.begin()
	all, shared, oursErr := d.providerTemplates(ctx)
	var ours map[string]string
	if oursErr == nil {
		// Every sweep refreshes the in-process record from Kubernetes, and
		// checks against it merged with anything recorded since.
		ours = recordedTemplates.refresh(all, shared, began)
	}
	if templatesErr == nil && oursErr != nil {
		// Without the provider's own template record, no same-Atespace
		// template can be told apart; nothing is verified or created.
		templatesErr = oursErr
	}
	free := map[string]int{}
	for i := range pools.Items {
		pool := &pools.Items[i]
		ledger, err := readSharedLedger(pool)
		if err != nil {
			continue
		}
		if sharedCountsAsFree(pool, ledger, shapes[ledger.Scope.ProfileUID], now) {
			free[ledger.Scope.ProfileUID] += int(ledger.Scope.MaxActors) - len(ledger.Entries)
		}
	}
	for i := range pools.Items {
		errs = append(errs, r.maintainSharedPool(ctx, d, workers, &pools.Items[i], shapes, free, now, templates, ours, templatesErr))
	}
	if templatesErr != nil {
		// Without a verified template inventory nothing new is created.
		return errors.Join(append(errs, templatesErr)...)
	}
	for uid, shape := range shapes {
		for free[uid] < shape.warm {
			if err := d.createWarmSharedPool(ctx, shape, templates, ours); err != nil {
				errs = append(errs, err)
				break
			}
			free[uid] += int(shape.scope.MaxActors)
		}
	}
	return errors.Join(errs...)
}

// profileSharedShape derives the scope a template-sized allocation of this
// profile would request, exactly as allocation does.
func (d *Lifecycle) profileSharedShape(ctx context.Context, item *profile.SubstrateWorkspaceProfile) (*sharedProfileShape, error) {
	raw := &unstructured.Unstructured{}
	raw.SetGroupVersionKind(profile.GroupVersion.WithKind("SubstrateWorkspaceProfile"))
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: item.Namespace, Name: item.Name}, raw); err != nil {
		return nil, err
	}
	if raw.GetUID() != item.UID {
		return nil, sdk.ErrStaleIdentity
	}
	hash, err := sdk.ParametersProfileHash(raw)
	if err != nil {
		return nil, err
	}
	spare, err := d.profileSpareShape(ctx, item)
	if err != nil {
		return nil, err
	}
	ref, err := item.Spec.TemplateRef.Resolve(item.Namespace)
	if err != nil {
		return nil, err
	}
	policy := item.Spec.Placement.SharedWorkers
	scope := sharedScope{Namespace: item.Namespace, Atespace: ref.Namespace, ProfileUID: string(item.UID), ProfileHash: hash, SourceUID: string(spare.source.GetUID()), Shape: spare.digest, CPUMillicores: spare.cpu, MemoryBytes: spare.memory, MaxActors: policy.MaxActorsPerWorker, Lifetime: metav1.Duration{Duration: policy.WorkerLifetime()}}
	if !scope.valid() {
		return nil, fmt.Errorf("shared worker policy is invalid")
	}
	return &sharedProfileShape{scope: scope, source: spare.source}, nil
}

// createWarmSharedPool creates an empty admitting shared pool.
func (d *Lifecycle) createWarmSharedPool(ctx context.Context, shape *sharedProfileShape, templates []*pb.ActorTemplate, ours map[string]string) error {
	name, err := newSharedPoolName(shape.scope)
	if err != nil {
		return err
	}
	label, err := randomLabel()
	if err != nil {
		return err
	}
	// Checked against this pool's real labels: an empty selector, or one the
	// generated label satisfies, still fails closed.
	if templatesReach(templates, shape.scope.Atespace, sharedLabels(label), ours) {
		return errForeignTemplate
	}
	spec, err := buildSharedPoolSpec(shape.source, shape.scope, label)
	if err != nil {
		return err
	}
	pool := poolObject()
	pool.SetNamespace(shape.source.GetNamespace())
	pool.SetName(name)
	// Only provider-owned labels: no operator label a foreign template might
	// select reaches the shared worker.
	pool.SetLabels(sharedLabels(label))
	var object map[string]any
	if err := json.Unmarshal(spec, &object); err != nil {
		return err
	}
	pool.Object["spec"] = object
	ledger := &sharedLedger{Version: sharedLedgerVersion, Scope: shape.scope, BornAt: metav1.NewTime(sharedNow().UTC().Truncate(time.Second)), State: sharedAdmitting, Entries: []sharedLedgerEntry{}}
	if err := setSharedLedger(pool, ledger); err != nil {
		return err
	}
	setSharedDeadline(pool, ledger)
	if err := d.client.Create(ctx, pool); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("Substrate shared worker created for warm capacity", "workspaceNamespace", shape.scope.Namespace)
	return nil
}

//nolint:gocyclo // Every pool-wide rule is evaluated in one pass, in priority order.
func (r *SpareWorkerReplenisher) maintainSharedPool(ctx context.Context, d *Lifecycle, workers *spareWorkerCache, pool *unstructured.Unstructured, shapes map[string]*sharedProfileShape, free map[string]int, now time.Time, templates []*pb.ActorTemplate, ours map[string]string, templatesErr error) error {
	if pool.GetDeletionTimestamp() != nil {
		return nil
	}
	ledger, err := readSharedLedger(pool)
	if err != nil {
		// Unreadable lifetime evidence is doubt: retire strongly at once.
		return d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker ledger is unreadable")
	}
	if ledger.State == sharedRetiring || ledger.State == sharedQuarantined {
		return d.retireSharedPool(ctx, pool, ledger.State, ledger.Reason)
	}
	if !now.Before(sharedRetireAt(pool, ledger)) || !r.Config.DedicatedInstall {
		// enforceSharedBounds already retired it, without native reads.
		return nil
	}
	// A strong trigger already known acts before any fallible read, so an
	// unrelated read failure can never suppress it: the pass's template
	// inventory is in hand, so a foreign template reaching the pool comes
	// first.
	if templatesErr == nil && templatesReach(templates, ledger.Scope.Atespace, pool.GetLabels(), ours) {
		return d.retireSharedPool(ctx, pool, sharedQuarantined, errForeignTemplate.Error())
	}
	// Kubernetes-only suspicion comes before any native read in this pass. A
	// failed check is held back, not returned, so the worker checks below
	// still run; only the non-strong maintenance after them is skipped.
	suspect, hostedErr := d.sharedPoolHostsSuspect(ctx, pool, ledger)
	if hostedErr == nil && suspect {
		return d.retireSharedPool(ctx, pool, sharedRetiring, "a hosted allocation selected the strong path or its workspace is suspect")
	}
	poolWorkers, err := workers.pool(ctx, pool)
	if err != nil {
		return errors.Join(hostedErr, err)
	}
	if len(poolWorkers) > 1 {
		return d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker pool has more than one worker")
	}
	// Each worker's assignments are listed once and shared by both checks.
	assignments := map[string][]*pb.ActorAssignment{}
	for _, worker := range poolWorkers {
		if ledger.Worker != nil && (worker.GetMetadata().GetUid() != ledger.Worker.UID || worker.GetWorkerPodUid() != ledger.Worker.PodUID) {
			return d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker was replaced")
		}
		if ledger.Worker != nil && worker.GetStatus().GetRegisteredEpoch() != ledger.Worker.Epoch {
			// A restarted worker process lost every sandbox; its Actors fail
			// and retire through the strong path, which this anticipates.
			return d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker process restarted")
		}
		listed, err := d.listWorkerAssignments(ctx, worker.GetMetadata().GetName())
		if err != nil {
			return errors.Join(hostedErr, err)
		}
		assignments[worker.GetMetadata().GetName()] = listed
		foreign, err := sharedForeignAssignment(ctx, d, pool, ledger, worker.GetMetadata().GetName(), listed)
		if err != nil {
			return errors.Join(hostedErr, err)
		}
		if foreign {
			current := poolObject()
			if err := d.client.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
				return client.IgnoreNotFound(err)
			}
			if current.GetUID() != pool.GetUID() {
				return nil
			}
			return d.retireSharedPool(ctx, current, sharedQuarantined, "the shared worker hosts an Actor outside its ledger")
		}
	}
	if hostedErr != nil {
		// Closing, collection and ledger writes need a complete hosted check.
		return hostedErr
	}
	closing := ""
	shape := shapes[ledger.Scope.ProfileUID]
	// Only a pool the sweep counted toward the warm reserve gives its free
	// slots back when it closes or is collected.
	counted := sharedCountsAsFree(pool, ledger, shape, now)
	switch {
	case !now.Before(sharedAdmissionEnds(pool, ledger)):
		closing = "the shared worker's admission window ended"
	case shape == nil || shape.scope.Namespace != ledger.Scope.Namespace:
		closing = "the shared worker's profile is gone or no longer opts in"
	case !sharedScopeCurrent(shape, ledger.Scope) || shape.source.GetNamespace() != pool.GetNamespace():
		closing = "the shared worker's operator pool or profile changed"
	}
	hostedSuspect, orphans, err := d.sharedLedgerHosts(ctx, pool, ledger, poolWorkers, assignments)
	if err != nil {
		return err
	}
	if hostedSuspect {
		// Neighbours are suspended through Orka core's eviction drain of the
		// terminating worker; the suspect Actor retires through the strong path.
		return d.retireSharedPool(ctx, pool, sharedRetiring, "a hosted workspace is tainted, quarantined or failed")
	}
	if closing == "" && templatesErr != nil {
		closing = "the install's ActorTemplates could not be verified"
	}
	changed := false
	if closing != "" && ledger.State == sharedAdmitting {
		ledger.State, ledger.Reason, changed = sharedDraining, closing, true
		if counted {
			free[ledger.Scope.ProfileUID] -= int(ledger.Scope.MaxActors) - len(ledger.Entries)
			counted = false
		}
	}
	if len(orphans) > 0 {
		ledger.Entries = slices.DeleteFunc(ledger.Entries, func(entry sharedLedgerEntry) bool { return slices.Contains(orphans, entry.Journal) })
		changed = true
	}
	switch {
	case len(ledger.Entries) == 0 && ledger.State != sharedAdmitting:
		if changed {
			if err := r.writeLedger(ctx, pool, ledger); err != nil {
				return err
			}
		}
		return deleteIdlePool(ctx, d, workers, pool)
	case len(ledger.Entries) == 0 && ledger.EmptySince == nil:
		emptySince := metav1.NewTime(now.UTC().Truncate(time.Second))
		ledger.EmptySince, changed = &emptySince, true
	case len(ledger.Entries) == 0 && now.Sub(ledger.EmptySince.Time) >= sharedIdleGrace:
		warmNeeded := counted && free[ledger.Scope.ProfileUID]-int(ledger.Scope.MaxActors) < shape.warm
		if !warmNeeded {
			if changed {
				if err := r.writeLedger(ctx, pool, ledger); err != nil {
					return err
				}
			}
			if counted {
				free[ledger.Scope.ProfileUID] -= int(ledger.Scope.MaxActors)
			}
			return deleteIdlePool(ctx, d, workers, pool)
		}
	case len(ledger.Entries) > 0 && ledger.EmptySince != nil:
		ledger.EmptySince, changed = nil, true
	}
	if changed {
		return r.writeLedger(ctx, pool, ledger)
	}
	return nil
}

func (r *SpareWorkerReplenisher) writeLedger(ctx context.Context, pool *unstructured.Unstructured, ledger *sharedLedger) error {
	if err := setSharedLedger(pool, ledger); err != nil {
		return err
	}
	err := r.Update(ctx, pool)
	if apierrors.IsConflict(err) {
		// A concurrent claim or release won; the next sweep re-evaluates.
		return nil
	}
	return err
}

// sharedForeignAssignment lists a worker's assignments, then re-reads the
// pool's ledger, and reports an assigned Actor that is not a ledger entry with
// the same UID and that a second listing still reports.
//
// The worker's assignments are passed in, listed after the pass's pool
// snapshot (snapshot) was read. Every assignment that snapshot already
// holds, by Actor and UID, is not foreign, so the pool is re-read only when
// some assignment is missing from it.
func sharedForeignAssignment(ctx context.Context, d *Lifecycle, pool *unstructured.Unstructured, snapshot *sharedLedger, worker string, assignments []*pb.ActorAssignment) (bool, error) {
	if len(assignments) == 0 || !slices.ContainsFunc(assignments, func(assignment *pb.ActorAssignment) bool { return !ledgerHolds(snapshot, assignment) }) {
		return false, nil
	}
	current := poolObject()
	if err := d.client.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if current.GetUID() != pool.GetUID() {
		return false, nil
	}
	ledger, err := readSharedLedger(current)
	if err != nil {
		return true, nil
	}
	return d.foreignAssignment(ctx, worker, assignments, ledger)
}

// sharedLedgerHosts reads each entry's exact journal record. It reports
// whether any hosted workspace is tainted, quarantined or failed, and which
// entries are orphans: their exact record positively disowns the slot and
// their Actor is not assigned. A missing or replaced journal is not proof.
func (d *Lifecycle) sharedLedgerHosts(ctx context.Context, pool *unstructured.Unstructured, ledger *sharedLedger, poolWorkers []*pb.Worker, listed map[string][]*pb.ActorAssignment) (bool, []string, error) {
	assigned := map[string]bool{}
	for _, worker := range poolWorkers {
		assignments, found := listed[worker.GetMetadata().GetName()]
		if !found {
			var err error
			if assignments, err = d.listWorkerAssignments(ctx, worker.GetMetadata().GetName()); err != nil {
				return false, nil, err
			}
		}
		if len(assignments) > sharedAssignmentPage {
			// More than a page of Actors on a worker of at most 16 slots is
			// itself foreign; collect nothing this pass.
			return false, nil, nil
		}
		for _, assignment := range assignments {
			assigned[assignment.GetActor().GetAtespace()+"/"+assignment.GetActor().GetName()] = true
		}
	}
	suspect := false
	var orphans []string
	for _, entry := range ledger.Entries {
		record, journalFound, err := d.sharedEntryRecordConfirmed(ctx, pool, entry)
		if errors.Is(err, errSharedJournalUndecodable) {
			// Never collected, and doubt: its markers cannot be read.
			suspect = true
			continue
		}
		if err != nil {
			return false, nil, err
		}
		if !journalFound || record == nil {
			continue
		}
		if !sharedEntryHeld(record, pool, entry) {
			// An assigned or booted entry its exact journal disowns hides
			// whose markers apply: doubt, never silently kept or collected,
			// once a fresh pool read confirms it is not a concurrent release.
			if assigned[entry.Actor] || entry.ActorUID != "" {
				persists, err := d.sharedEntryPersists(ctx, pool, entry)
				if err != nil {
					return false, nil, err
				}
				suspect = suspect || persists
			} else {
				orphans = append(orphans, entry.Journal)
			}
			continue
		}
		workspace := &api.ExecutionWorkspace{}
		if err := d.sharedWorkspace(ctx, clientKey(record.Request.Key), workspace); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, nil, err
		}
		if record.Failure != "" || workspace.UID == record.Request.Key.WorkspaceUID && workspaceSuspect(workspace) {
			suspect = true
		}
	}
	return suspect, orphans, nil
}

// sharedPersistedRetirement returns the pool's saved retirement state, if it
// is Retiring or Quarantined.
func sharedPersistedRetirement(pool *unstructured.Unstructured) (string, string) {
	ledger, err := readSharedLedger(pool)
	if err != nil || (ledger.State != sharedRetiring && ledger.State != sharedQuarantined) {
		return "", ""
	}
	return ledger.State, ledger.Reason
}

func sharedRetirementPersisted(pool *unstructured.Unstructured) bool {
	state, _ := sharedPersistedRetirement(pool)
	return state != ""
}

// sharedEntryRecordConfirmed reads an entry's exact journal record from the
// pass's snapshot and, unless that shows the entry held, confirms it with a
// live read of that one journal before anything is decided. The snapshot is
// taken before the pools are listed, so an allocation that saved its Actor
// UID or moved to a new sequence in between would otherwise look like it
// disowns a valid entry, and a healthy pool would be strongly retired or its
// entry collected. Only a mismatch costs a live read, so they stay rare.
func (d *Lifecycle) sharedEntryRecordConfirmed(ctx context.Context, pool *unstructured.Unstructured, entry sharedLedgerEntry) (*journalRecord, bool, error) {
	record, found, err := d.sharedEntryRecord(ctx, entry.Journal)
	if d.sharedPass == nil || err == nil && found && record != nil && sharedEntryHeld(record, pool, entry) {
		return record, found, err
	}
	live := *d
	live.sharedPass = nil
	return live.sharedEntryRecord(ctx, entry.Journal)
}

// sharedAssignmentPage is one native assignment page.
const sharedAssignmentPage = 1000

// errSharedJournalUndecodable marks a hosted entry whose exact journal (or
// archived) record exists but cannot be decoded. Its Strong and Failure
// markers and its workspace are unreadable, so it is doubt.
var errSharedJournalUndecodable = errors.New("a hosted shared worker journal cannot be decoded")

// sharedEntryRecord decodes the exact journal record a ledger marker names.
// found is false when the journal is missing or replaced; record is nil when
// it exists but does not decode.
func (d *Lifecycle) sharedEntryRecord(ctx context.Context, marker string) (*journalRecord, bool, error) {
	parts := strings.Split(marker, "/")
	if len(parts) != 4 {
		return nil, false, nil
	}
	sequence, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || sequence < 1 {
		return nil, false, nil
	}
	journal := &corev1.ConfigMap{}
	if err := d.sharedConfigMap(ctx, types.NamespacedName{Namespace: parts[0], Name: parts[1]}, journal); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if string(journal.UID) != parts[2] {
		return nil, false, nil
	}
	record, ok := decodeClaimRecord(journal)
	if !ok {
		return nil, true, errSharedJournalUndecodable
	}
	if record.Request.Sequence < sequence {
		// The journal has not reached that sequence: not proof of anything.
		return nil, true, nil
	}
	if record.Request.Sequence > sequence {
		archived := &corev1.ConfigMap{}
		if err := d.sharedConfigMap(ctx, types.NamespacedName{Namespace: parts[0], Name: historyName(parts[1], sequence)}, archived); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, false, nil
			}
			return nil, false, err
		}
		if record, ok = decodeClaimRecord(archived); !ok || record.Request.Sequence != sequence {
			return nil, true, errSharedJournalUndecodable
		}
	}
	return record, true, nil
}

// sharedStrongMarked reports, reading only Kubernetes, whether a hosted
// allocation requires the pool's strong retirement (see
// sharedPoolHostsSuspect). An unreadable journal or workspace is reported on
// the next pass; it never blocks the other bounds.
func (d *Lifecycle) sharedStrongMarked(ctx context.Context, pool *unstructured.Unstructured) bool {
	ledger, err := readSharedLedger(pool)
	if err != nil {
		return false
	}
	suspect, err := d.sharedPoolHostsSuspect(ctx, pool, ledger)
	return err == nil && suspect
}

// sharedScopeCurrent reports whether a pool's scope still matches its profile
// and the current operator pool. A runtime may override the template's Actor
// size, so the pool is checked at the Actor size its ledger records: the
// current operator pool is rebuilt at that size and its shape compared. The
// template-sized shape is used only for warm provisioning.
func sharedScopeCurrent(shape *sharedProfileShape, scope sharedScope) bool {
	expected := shape.scope
	expected.CPUMillicores, expected.MemoryBytes, expected.Shape = scope.CPUMillicores, scope.MemoryBytes, scope.Shape
	if expected != scope {
		return false
	}
	if scope.CPUMillicores == shape.scope.CPUMillicores && scope.MemoryBytes == shape.scope.MemoryBytes {
		return scope.Shape == shape.scope.Shape
	}
	spec, err := buildPrivatePoolSpec(shape.source, scope.CPUMillicores, scope.MemoryBytes, spareBirth(spareNamePrefix+"shape"))
	if err != nil {
		return false
	}
	digest, err := runtimePoolRestoreDigest(spec, false)
	return err == nil && digest == scope.Shape
}

// sharedCountsAsFree reports whether a pool's free slots count toward its
// profile's warm reserve: an admitting pool of the profile's current
// template-sized scope, in the operator pool's namespace.
func sharedCountsAsFree(pool *unstructured.Unstructured, ledger *sharedLedger, shape *sharedProfileShape, now time.Time) bool {
	return shape != nil && ledger.Scope == shape.scope && pool.GetNamespace() == shape.source.GetNamespace() && sharedAdmits(pool, ledger, now)
}

// sharedPassReads is one bounds or sweep pass's snapshot of the Kubernetes
// reads every pool's checks need: the labeled journal and archived-record
// ConfigMaps (from the pass's one inventory List) and each namespace's
// ExecutionWorkspaces (one List per namespace, on first use). A pass then
// reads each once instead of once per pool or entry. A lookup the snapshot
// cannot answer (an object outside the labeled inventory, or a namespace
// whose List failed) falls back to a live Get, so the snapshot never turns a
// present object into an absent one. Admission and retirement of one
// allocation never use it; they read live.
type sharedPassReads struct {
	mu         sync.Mutex
	journals   map[types.NamespacedName]*corev1.ConfigMap
	workspaces map[string]map[string]*api.ExecutionWorkspace
	// listed marks a namespace whose List succeeded, failed one whose List
	// failed this pass (its lookups then use live Gets, never a re-List).
	listed, failed map[string]bool
}

func newSharedPassReads() *sharedPassReads {
	return &sharedPassReads{workspaces: map[string]map[string]*api.ExecutionWorkspace{}, listed: map[string]bool{}, failed: map[string]bool{}}
}

// noteWorkspaceListFailed records a namespace whose List failed this pass.
func (p *sharedPassReads) noteWorkspaceListFailed(namespace string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed[namespace] = true
}

// withSharedPass returns a Lifecycle that shares one pass's reads.
func (d *Lifecycle) withSharedPass() *Lifecycle {
	pass := *d
	pass.sharedPass = newSharedPassReads()
	return &pass
}

// noteJournals records the pass's labeled journal inventory.
func (p *sharedPassReads) noteJournals(items []corev1.ConfigMap) {
	if p == nil {
		return
	}
	journals := make(map[types.NamespacedName]*corev1.ConfigMap, len(items))
	for i := range items {
		journals[client.ObjectKeyFromObject(&items[i])] = &items[i]
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.journals = journals
}

// noteWorkspaces records one namespace's complete workspace List.
func (p *sharedPassReads) noteWorkspaces(namespace string, items []api.ExecutionWorkspace) {
	if p == nil {
		return
	}
	byName := make(map[string]*api.ExecutionWorkspace, len(items))
	for i := range items {
		byName[items[i].Name] = &items[i]
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workspaces[namespace], p.listed[namespace] = byName, true
}

// sharedConfigMap reads a journal or archived record, from the pass's
// inventory when it holds it.
func (d *Lifecycle) sharedConfigMap(ctx context.Context, key types.NamespacedName, cm *corev1.ConfigMap) error {
	if p := d.sharedPass; p != nil {
		p.mu.Lock()
		cached, found := p.journals[key]
		p.mu.Unlock()
		if found {
			cached.DeepCopyInto(cm)
			return nil
		}
	}
	return d.client.Get(ctx, key, cm)
}

// sharedWorkspace reads an ExecutionWorkspace, from the pass's List of its
// namespace (made once, on first use) when one succeeded.
func (d *Lifecycle) sharedWorkspace(ctx context.Context, key types.NamespacedName, workspace *api.ExecutionWorkspace) error {
	p := d.sharedPass
	if p == nil {
		return d.client.Get(ctx, key, workspace)
	}
	p.mu.Lock()
	listed, failed := p.listed[key.Namespace], p.failed[key.Namespace]
	p.mu.Unlock()
	if failed {
		return d.client.Get(ctx, key, workspace)
	}
	if !listed {
		workspaces := &api.ExecutionWorkspaceList{}
		if err := d.client.List(ctx, workspaces, client.InNamespace(key.Namespace)); err != nil {
			p.noteWorkspaceListFailed(key.Namespace)
			return d.client.Get(ctx, key, workspace)
		}
		p.noteWorkspaces(key.Namespace, workspaces.Items)
	}
	p.mu.Lock()
	cached, found := p.workspaces[key.Namespace][key.Name]
	p.mu.Unlock()
	if !found {
		return apierrors.NewNotFound(api.GroupVersion.WithResource("executionworkspaces").GroupResource(), key.Name)
	}
	cached.DeepCopyInto(workspace)
	return nil
}
