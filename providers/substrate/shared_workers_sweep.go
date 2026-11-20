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
	"syscall"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	d = d.withSharedLoopPass("bounds")
	defer d.flushOutageMarks(ctx)
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
	var deleting []poolDeadline
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
			// A deleting pool's worker Pods still get the capped delete
			// retried every pass, before the hard bound's escalation.
			deleting = append(deleting, poolDeadline{pool: pool, escalateAt: escalateAt})
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
		case r.Config.DedicatedInstall && sharedStampedOutagePast(pool, now) != "":
			// An outage stamped durably by the sweep, enforced here whatever
			// the sweep's progress or a restart.
			retire(sharedRetiring, sharedStampedOutagePast(pool, now))
		case r.Config.DedicatedInstall && d.nativeDegradedPast(ctx, pool):
			// Native control could not be set up at startup, so no native
			// sweep verifies the pool's templates or assignments.
			retire(sharedRetiring, "native control has been unavailable to this provider since startup, past its bound: "+NativeDegradedReason())
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
	errs = append(errs, d.retryCappedPodDeletes(ctx, deleting)...)
	var mu sync.Mutex
	var deleted []retirement
	work := make(chan retirement)
	var group sync.WaitGroup
	for range min(sharedDeleteConcurrency, len(retirements)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for item := range work {
				issued, err := d.deleteSharedPool(ctx, item.pool, item.escalateAt)
				if err != nil && !issued {
					// The decision survives a failed Delete: admission
					// refuses the pool and the next pass retries.
					// It also records the state, so bookkeeping skips it.
					d.persistSharedRetireDecision(ctx, item.pool, item.state, item.reason, err)
				}
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				}
				if issued || err == nil {
					deleted = append(deleted, item)
				}
				mu.Unlock()
			}
		}()
	}
	feed(ctx, work, retirements)
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
		d.sharedPass.noteJournalsFailed(err)
		return nil, err
	}
	defer sharedProvenanceHealth.succeeded(started)
	d.sharedPass.noteJournals(journals.Items)
	out := map[types.UID]sharedProvenance{}
	unlabelled := false
	for i := range journals.Items {
		record, ok := decodeClaimRecord(&journals.Items[i])
		if ok && record.SharedWorker != nil && journals.Items[i].Labels[sharedRecordLabel] != "true" {
			// A shared record written before the label existed: it counts
			// as shared here, and keeps the full scan until the separate
			// label backfill has labelled it. Enforcement never writes it.
			unlabelled = true
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
					d.sharedPass.noteWorkspaceListFailed(namespace, err)
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
		if rootCancelled(ctx) {
			break
		}
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

// sharedLabelBackfillTimeout bounds one label backfill pass, and
// sharedLabelBackfillConcurrency its concurrent label writes.
var sharedLabelBackfillTimeout = 60 * time.Second

const sharedLabelBackfillConcurrency = 8

// sharedLabelBackfillPeriod spaces label backfill passes.
const sharedLabelBackfillPeriod = 30 * time.Second

// EnforceSharedLabelBackfill runs one leader-only label backfill pass, off
// the discovery and enforcement path: within its own overall bound, it lists
// every provider record and labels each shared record written before the
// label existed, concurrently and capped, each write preconditioned on the
// listed resourceVersion. Discovery treats unlabelled shared records as
// shared, with a full scan, until a scan finds none left; nothing in the
// bounds, escalation or admission paths waits on this pass.
func (r *SpareWorkerReplenisher) EnforceSharedLabelBackfill(ctx context.Context) error {
	if sharedRecordsLabelled.Load() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, sharedLabelBackfillTimeout)
	defer cancel()
	journals := &corev1.ConfigMapList{}
	if err := r.List(ctx, journals, client.HasLabels{ownershipLabel, providerLabel}); err != nil {
		return err
	}
	var pending []*corev1.ConfigMap
	for i := range journals.Items {
		if record, ok := decodeClaimRecord(&journals.Items[i]); ok && record.SharedWorker != nil && journals.Items[i].Labels[sharedRecordLabel] != "true" {
			pending = append(pending, &journals.Items[i])
		}
	}
	var mu sync.Mutex
	var errs []error
	work := make(chan *corev1.ConfigMap)
	var group sync.WaitGroup
	for range min(sharedLabelBackfillConcurrency, len(pending)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for cm := range work {
				labeled := cm.DeepCopy()
				if labeled.Labels == nil {
					labeled.Labels = map[string]string{}
				}
				labeled.Labels[sharedRecordLabel] = "true"
				writeCtx, cancelWrite := context.WithTimeout(ctx, sharedLedgerWriteTimeout)
				err := r.Update(writeCtx, labeled)
				cancelWrite()
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
feed:
	for _, cm := range pending {
		select {
		case work <- cm:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	group.Wait()
	if len(errs) == 0 && ctx.Err() == nil {
		sharedRecordsLabelled.Store(true)
	}
	return errors.Join(append(errs, ctx.Err())...)
}

// RunSharedLabelBackfill runs EnforceSharedLabelBackfill every period until
// the backfill is confirmed or ctx ends.
func (r *SpareWorkerReplenisher) RunSharedLabelBackfill(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for !sharedRecordsLabelled.Load() {
		if err := r.EnforceSharedLabelBackfill(ctx); err != nil && ctx.Err() == nil {
			ctrl.LoggerFrom(ctx).Info("Substrate shared journal label backfill incomplete; it is retried", "reason", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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

// sharedTemplateInventoryHealth tracks the provider's recorded-template
// inventory reads (every journal, archive and checkpoint catalog) with the
// same rule and bound. It is a separate instance because the bounds loop's
// labelled journal List succeeds independently, and would otherwise clear an
// outage of this inventory that never ends.
var sharedTemplateInventoryHealth = &provenanceHealth{}

// sharedProfileInventoryHealth tracks the sweep's profile List by the same
// outage rule. While it fails, shared admission is closed; the sweep still
// runs every shared-pool safety check without profiles.
var sharedProfileInventoryHealth = &provenanceHealth{}

// sharedNativeTemplateHealth tracks the native ActorTemplate inventory
// (listAllActorTemplates) with the same rule and bound. Only a complete,
// successful inventory refreshes it.
var sharedNativeTemplateHealth = &provenanceHealth{}

// sharedNativeOutageAnnotation durably records, on each shared pool, when
// the provider first saw the native ActorTemplate inventory fail, so a
// restarting provider cannot reset the outage clock. It is cleared by the
// next successful inventory.
const sharedNativeOutageAnnotation = "substrate.workspace.orka.ai/native-outage-since"

// sharedControlPlaneDownGrace is the total outage after which a native
// ActorTemplate inventory outage retires shared pools even while the control
// plane looks down from the provider. A provider-side transport failure does
// not prove other clients cannot place, so the exception covers short
// outages only.
const sharedControlPlaneDownGrace = 5 * time.Minute

// nativeTemplateOutage decides, per pool, the strong retirement a native
// ActorTemplate inventory outage requires. Its age runs from the earlier of
// this process's first failure and the pool's durable outage annotation.
// Past the outage bound the pool is retired, unless the control plane looks
// down from the provider through a transport or availability failure; that
// defers retirement only until the outage reaches
// sharedControlPlaneDownGrace. A rejection of the provider's own credentials
// never defers it.
type nativeTemplateOutage struct {
	d       *Lifecycle
	failing bool
	// rejected is the credential rejection that failed this pass's
	// inventory, if any.
	rejected string
	probed   bool
	down     bool
}

func (o *nativeTemplateOutage) retires(ctx context.Context, pool *unstructured.Unstructured) string {
	if !o.failing {
		// The stamp is cleared only by a success that cleared the tracked
		// outage: a newer failure (for example an admission List that
		// started after this pass's) keeps it, by the same started-at order.
		if !sharedNativeTemplateHealth.failing() {
			o.d.clearOutageMark(ctx, pool, sharedNativeOutageAnnotation)
		}
		return ""
	}
	start := o.d.recordOutageSince(ctx, pool, sharedNativeOutageAnnotation, sharedNativeTemplateHealth.since())
	elapsed := sharedNow().Sub(start)
	if start.IsZero() || elapsed < sharedProvenanceOutageBound {
		return ""
	}
	if !o.probed {
		o.down, o.probed = o.d.sharedPass.nativeDown(ctx, o.d), true
	}
	rejected := o.rejected
	if rejected == "" {
		rejected = NativeCredentialsRejected()
	}
	switch {
	case rejected != "":
		// A rejection of the provider's own credentials never takes the
		// control-plane-down grace, whatever the worker probe returns.
		return "the native ActorTemplate inventory stayed unavailable past its bound: " + rejected
	case !o.down:
		return "the native ActorTemplate inventory stayed unavailable past its bound while the control plane still places Actors"
	case elapsed >= sharedControlPlaneDownGrace:
		return "the native ActorTemplate inventory stayed unavailable past the control-plane-down grace; a provider-side outage does not prove other clients cannot place"
	}
	return ""
}

// recordOutageSince returns an outage's start for the pool: the earlier of
// this process's start and the pool's durable annotation key. It never waits
// on a write: a missing annotation is written at once in the background,
// concurrently and best effort (startOutageMark), so a stalled write can
// never hold up a quarantine or retirement, and a restart during the pass
// still finds the stamp.
func (d *Lifecycle) recordOutageSince(ctx context.Context, pool *unstructured.Unstructured, key string, start time.Time) time.Time {
	if raw, ok := pool.GetAnnotations()[key]; ok {
		if recorded, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			if start.IsZero() || recorded.Before(start) {
				start = recorded
			}
			if sharedDecidedStamps.last(pool.GetUID(), key) == decidedClear {
				// This process last decided a clear, which may still be in
				// flight: the newer failure is recorded so it outranks it.
				d.startOutageMark(ctx, pool, key, start.UTC().Format(time.RFC3339Nano))
			}
			return start
		}
	}
	if !start.IsZero() {
		d.startOutageMark(ctx, pool, key, start.UTC().Format(time.RFC3339Nano))
	}
	return start
}

// clearOutageMark removes the pool's outage annotation key after the
// success that covers it, in the background. It records the clear decision
// whenever the snapshot shows the stamp or this process itself decided one
// for the pool, whose write may still be in flight: the newer clear then
// makes that stale write skip.
func (d *Lifecycle) clearOutageMark(ctx context.Context, pool *unstructured.Unstructured, key string) {
	if _, ok := pool.GetAnnotations()[key]; ok || sharedDecidedStamps.last(pool.GetUID(), key) == decidedSet {
		d.startOutageMark(ctx, pool, key, "")
	}
}

// decidedStamps records, in process, the last decision (set or clear) this
// process made for each pool's stamp key. A decision whose write may still be
// in flight is outranked by recording the next opposite decision even when
// the pool's snapshot already looks right; repeating the same decision on a
// snapshot that reflects it costs no write.
type decidedStamps struct {
	mu   sync.Mutex
	keys map[types.UID]map[string]string
}

const (
	decidedSet   = "set"
	decidedClear = "clear"
)

var sharedDecidedStamps = &decidedStamps{}

func (s *decidedStamps) record(uid types.UID, key, decision string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = map[types.UID]map[string]string{}
	}
	if s.keys[uid] == nil {
		s.keys[uid] = map[string]string{}
	}
	s.keys[uid][key] = decision
}

func (s *decidedStamps) last(uid types.UID, key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys[uid][key]
}

func (s *decidedStamps) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = nil
}

// outageMark is one durable outage annotation change: a value to set when
// missing, or "" to remove.
type outageMark struct {
	namespace, name string
	uid             types.UID
	key, value      string
	// overwrite replaces a present value instead of keeping it.
	overwrite bool
	// decidedAt is when the decision this write represents was made. A
	// write is applied only if the pool's record for its key was not
	// decided later, so a delayed write never undoes a newer decision.
	decidedAt time.Time
}

// outageDecidedKey is the annotation recording when a stamp key's current
// state was decided.
func outageDecidedKey(key string) string { return key + "-decided" }

// sharedOutageMarkConcurrency caps a pass's concurrent outage annotation
// writes, and sharedOutageMarkBudget bounds how long the pass's end waits for
// the writes still in flight.
const sharedOutageMarkConcurrency = 8

var sharedOutageMarkBudget = 30 * time.Second

// startOutageMark writes one outage annotation change in the background, at
// most sharedOutageMarkConcurrency at a time, each pool read fresh and
// written only if it still has the same UID, on its own short bound.
func (d *Lifecycle) startOutageMark(ctx context.Context, pool *unstructured.Unstructured, key, value string) {
	d.startOutageMarks(ctx, outageMark{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: key, value: value})
}

// startOutageMarks writes one pool's annotation changes together, in one
// Update, in the background.
func (d *Lifecycle) startOutageMarks(ctx context.Context, marks ...outageMark) {
	if len(marks) == 0 {
		return
	}
	decided := sharedNow()
	for i := range marks {
		if marks[i].decidedAt.IsZero() {
			marks[i].decidedAt = decided
		}
		decision := decidedClear
		if marks[i].value != "" {
			decision = decidedSet
		}
		sharedDecidedStamps.record(marks[i].uid, marks[i].key, decision)
	}
	p := d.sharedPass
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.markSlots == nil {
		p.markSlots = make(chan struct{}, sharedOutageMarkConcurrency)
	}
	slots := p.markSlots
	p.mu.Unlock()
	p.markWrites.Add(1)
	go func() {
		defer p.markWrites.Done()
		select {
		case slots <- struct{}{}:
		case <-rootContext(ctx).Done():
			return
		}
		defer func() { <-slots }()
		if p.superseded() {
			// A later pass of the same loop decides afresh.
			return
		}
		writeCtx, cancel := freshBudget(ctx, sharedLedgerWriteTimeout)
		defer cancel()
		d.writeOutageMarks(writeCtx, marks)
	}()
}

// flushOutageMarks waits, within its own budget, for the pass's outage
// annotation writes still in flight.
func (d *Lifecycle) flushOutageMarks(ctx context.Context) {
	p := d.sharedPass
	if p == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		p.markWrites.Wait()
		close(done)
	}()
	budget, cancel := freshBudget(ctx, sharedOutageMarkBudget)
	defer cancel()
	select {
	case <-done:
	case <-budget.Done():
	}
}

// writeOutageMarks applies one pool's outage annotation changes, retrying a
// write conflict.
func (d *Lifecycle) writeOutageMarks(ctx context.Context, marks []outageMark) {
	d.writeOutageMarksRetry(ctx, marks, 3)
}

func (d *Lifecycle) writeOutageMarksRetry(ctx context.Context, marks []outageMark, retries int) {
	writeCtx, cancel := context.WithTimeout(ctx, sharedLedgerWriteTimeout)
	defer cancel()
	current := poolObject()
	if err := d.client.Get(writeCtx, types.NamespacedName{Namespace: marks[0].namespace, Name: marks[0].name}, current); err != nil || current.GetUID() != marks[0].uid || current.GetDeletionTimestamp() != nil {
		return
	}
	annotations := maps.Clone(current.GetAnnotations())
	if annotations == nil {
		annotations = map[string]string{}
	}
	changed := false
	for _, mark := range marks {
		decidedKey := outageDecidedKey(mark.key)
		if recorded, err := time.Parse(time.RFC3339Nano, annotations[decidedKey]); err == nil && !mark.decidedAt.IsZero() && recorded.After(mark.decidedAt) {
			// A newer decision already wrote this key.
			continue
		}
		_, present := annotations[mark.key]
		switch {
		case mark.value == "" && present:
			delete(annotations, mark.key)
			changed = true
		case mark.value != "" && (!present || mark.overwrite && annotations[mark.key] != mark.value):
			annotations[mark.key] = mark.value
			changed = true
		}
		if decided := mark.decidedAt.UTC().Format(time.RFC3339Nano); !mark.decidedAt.IsZero() && annotations[decidedKey] != decided {
			annotations[decidedKey] = decided
			changed = true
		}
	}
	if !changed {
		return
	}
	current.SetAnnotations(annotations)
	if err := d.client.Update(writeCtx, current); apierrors.IsConflict(err) && retries > 0 {
		// A concurrent write on the same pool won; reread and retry.
		d.writeOutageMarksRetry(ctx, marks, retries-1)
	}
}

// sharedNativeProbeTimeout bounds the reachability probe that gates
// retirement for a native template inventory outage.
const sharedNativeProbeTimeout = 10 * time.Second

// nativeControlDown reports whether the native control plane is down, from a
// cheap worker read on its own short context detached from the sweep's
// deadline. A template inventory outage can let a foreign Actor be placed only
// while the control plane still places; when it is down, nothing is placed,
// and strongly retiring every shared pool would only strand its Actors' data,
// which cannot be suspended without it.
//
// Only a transport or availability failure counts as down. Any other
// failure, notably the provider's own credentials being rejected
// (Unauthenticated or PermissionDenied), does not: other clients may still
// place Actors, so retirement is not suppressed.
func (d *Lifecycle) nativeControlDown(ctx context.Context) bool {
	probeCtx, cancel := freshBudget(ctx, sharedNativeProbeTimeout)
	defer cancel()
	_, err := d.control.ListWorkers(probeCtx, &pb.ListWorkersRequest{Options: &pb.ListOptions{PageSize: 1}})
	noteNativeCredentials(ctx, err)
	return err != nil && nativeUnavailable(err)
}

// nativeUnavailable reports a transport or availability failure: gRPC
// Unavailable, DeadlineExceeded or Canceled, a context timeout or
// cancellation, or a refused or reset connection.
func nativeUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

// nativeCredentialsRejected reports the control plane rejecting the
// provider's own credentials.
func nativeCredentialsRejected(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	}
	return false
}

// sharedNativeCredentials holds, in process, why the native control plane
// last rejected the provider's own credentials ("" once a native read
// succeeds again). The provider reconciler surfaces it on the provider.
var sharedNativeCredentials atomic.Pointer[string]

// noteNativeCredentials records the outcome of a native read for
// credential reporting: a rejection is logged and kept until a later read
// succeeds; other failures leave the record unchanged.
func noteNativeCredentials(ctx context.Context, err error) {
	switch {
	case err == nil:
		sharedNativeCredentials.Store(nil)
	case nativeCredentialsRejected(err):
		reason := "provider native credentials rejected: " + status.Code(err).String()
		sharedNativeCredentials.Store(&reason)
		ctrl.LoggerFrom(ctx).Error(err, "Substrate provider native credentials rejected; shared workers whose templates cannot be checked are retired once this outlasts its bound", "code", status.Code(err).String())
	}
}

// sharedUnverifiedGraceAnnotation classifies a pool's unverified stamp,
// written in the same Update as the stamp: sharedUnverifiedGraceDown when
// its checks last failed while native looked down from the provider (the
// control-plane-down grace applies), sharedUnverifiedGraceNone otherwise (the
// outage bound applies). A stamp whose classification is missing is treated
// as under the grace until the sweep classifies it, so the bounds loop never
// retires early.
const (
	sharedUnverifiedGraceAnnotation = "substrate.workspace.orka.ai/unverified-grace"
	sharedUnverifiedGraceDown       = "control-plane-down"
	sharedUnverifiedGraceNone       = "none"
)

// stampedTime parses a pool's durable outage annotation key.
func stampedTime(pool *unstructured.Unstructured, key string) (time.Time, bool) {
	raw, ok := pool.GetAnnotations()[key]
	if !ok {
		return time.Time{}, false
	}
	stamped, err := time.Parse(time.RFC3339Nano, raw)
	return stamped, err == nil
}

// sharedStampedOutagePast reports, from the pool's durable stamps alone (so
// the Kubernetes-only bounds loop can enforce them whatever the sweep's
// progress), an unverified pool past its bound (60 s, or the
// control-plane-down grace when so stamped) or a native template inventory
// outage past the control-plane-down grace, the longest bound it can carry.
func sharedStampedOutagePast(pool *unstructured.Unstructured, now time.Time) string {
	annotations := pool.GetAnnotations()
	if raw, ok := annotations[sharedUnverifiedAnnotation]; ok {
		if since, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			// Only a stamp classified as outside the grace takes the outage
			// bound; a missing classification keeps the grace.
			bound := sharedControlPlaneDownGrace
			if annotations[sharedUnverifiedGraceAnnotation] == sharedUnverifiedGraceNone {
				bound = sharedProvenanceOutageBound
			}
			if !now.Before(since.Add(bound)) {
				return "the shared worker's isolation checks have not completed since its durable stamp, past their bound"
			}
		}
	}
	if raw, ok := annotations[sharedNativeOutageAnnotation]; ok {
		if since, err := time.Parse(time.RFC3339Nano, raw); err == nil && !now.Before(since.Add(sharedControlPlaneDownGrace)) {
			return "the native ActorTemplate inventory has stayed unavailable since its durable stamp, past the control-plane-down grace"
		}
	}
	return ""
}

// sharedUnverifiedAnnotation durably records, on a pool, when its isolation
// checks (hosted workspaces, workers and assignments, ledger records) first
// failed to complete, including on a timeout, so a restarting provider cannot
// reset that clock.
const sharedUnverifiedAnnotation = "substrate.workspace.orka.ai/unverified-since"

// poolRejections tracks, per pool UID and in process, when each pool's
// isolation checks first failed to complete. A pool's entry is cleared only
// by that pool's own complete, successful checks, so another pool's success,
// or a pool with no worker to read, never resets it.
type poolRejections struct {
	mu    sync.Mutex
	since map[types.UID]time.Time
}

var sharedUnverifiedPools = &poolRejections{}

func (p *poolRejections) failed(uid types.UID, now time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.since == nil {
		p.since = map[types.UID]time.Time{}
	}
	if start, ok := p.since[uid]; ok {
		return start
	}
	p.since[uid] = now
	return now
}

func (p *poolRejections) succeeded(uid types.UID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.since, uid)
}

func (p *poolRejections) pending(uid types.UID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.since[uid]
	return ok
}

func (p *poolRejections) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.since = nil
}

// sharedPoolUnverified reports a pool whose isolation checks have not
// completed since they last failed; admission refuses it.
func sharedPoolUnverified(pool *unstructured.Unstructured) bool {
	_, stamped := pool.GetAnnotations()[sharedUnverifiedAnnotation]
	return stamped || sharedUnverifiedPools.pending(pool.GetUID())
}

// verificationFailed handles an isolation check that failed or did not
// finish within its budget. The pool is unverified: admission refuses it at
// once, and it is strongly retired once that has lasted past the outage
// bound (60 s). Only a native check's transport or availability failure, while
// the control plane looks down from the provider, takes the longer
// control-plane-down grace instead; a rejection of the provider's own
// credentials never does. The error is returned for the next pass.
func (d *Lifecycle) verificationFailed(ctx context.Context, pool *unstructured.Unstructured, native bool, err error) error {
	if native {
		noteNativeCredentials(ctx, err)
	}
	grace := native && nativeUnavailable(err) && !nativeCredentialsRejected(err) && NativeCredentialsRejected() == "" && d.sharedPass.nativeDown(ctx, d)
	bound, class := sharedProvenanceOutageBound, sharedUnverifiedGraceNone
	if grace {
		bound, class = sharedControlPlaneDownGrace, sharedUnverifiedGraceDown
	}
	start := sharedUnverifiedPools.failed(pool.GetUID(), sharedNow())
	if recorded, ok := stampedTime(pool, sharedUnverifiedAnnotation); ok && recorded.Before(start) {
		start = recorded
	}
	// The class is stamped durably beside the age, in the same write, so the
	// Kubernetes-only bounds loop, which cannot probe native, applies the
	// same bound.
	marks := []outageMark{{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedAnnotation, value: start.UTC().Format(time.RFC3339Nano)}}
	outranks := sharedDecidedStamps.last(pool.GetUID(), sharedUnverifiedAnnotation) == decidedClear
	if pool.GetAnnotations()[sharedUnverifiedGraceAnnotation] != class || pool.GetAnnotations()[sharedUnverifiedAnnotation] == "" || outranks {
		marks = append(marks, outageMark{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedGraceAnnotation, value: class, overwrite: true})
	}
	if pool.GetAnnotations()[sharedUnverifiedAnnotation] == "" || len(marks) > 1 {
		d.startOutageMarks(ctx, marks...)
	}
	if sharedNow().Sub(start) < bound {
		return err
	}
	reason := "the shared worker's isolation checks did not complete past their bound"
	if rejected := NativeCredentialsRejected(); native && rejected != "" {
		reason = "the shared worker's assignments cannot be checked: " + rejected + ", past its bound"
	}
	return errors.Join(err, d.retireSharedPool(ctx, pool, sharedRetiring, reason))
}

// verificationSucceeded clears a pool's unverified record after all of its
// own isolation checks completed.
func (d *Lifecycle) verificationSucceeded(ctx context.Context, pool *unstructured.Unstructured) {
	sharedUnverifiedPools.succeeded(pool.GetUID())
	decided := sharedDecidedStamps.last(pool.GetUID(), sharedUnverifiedAnnotation) == decidedSet || sharedDecidedStamps.last(pool.GetUID(), sharedUnverifiedGraceAnnotation) == decidedSet
	if annotations := pool.GetAnnotations(); decided || annotations[sharedUnverifiedAnnotation] != "" || annotations[sharedUnverifiedGraceAnnotation] != "" {
		d.startOutageMarks(ctx,
			outageMark{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedAnnotation},
			outageMark{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedGraceAnnotation})
	}
}

// nativeDown probes, once per pass, whether the native control plane looks
// down from the provider (nativeControlDown).
func (p *sharedPassReads) nativeDown(ctx context.Context, d *Lifecycle) bool {
	if p == nil {
		return d.nativeControlDown(ctx)
	}
	// Single flight: concurrent checks wait for the pass's one probe.
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	p.mu.Lock()
	probed, down := p.probed, p.down
	p.mu.Unlock()
	if probed {
		return down
	}
	down = d.nativeControlDown(ctx)
	p.mu.Lock()
	p.probed, p.down = true, down
	p.mu.Unlock()
	return down
}

// nativeDegradation records that native control could not be set up when
// this process started, and why.
type nativeDegradation struct {
	since  time.Time
	reason string
}

var sharedNativeDegraded atomic.Pointer[nativeDegradation]

// SetNativeDegraded records that native control could not be set up at
// startup (an invalid configuration, or a dial that failed). The native sweep
// then never runs, so the Kubernetes-only bounds loop enforces the native
// outage bound on existing shared pools itself. Such a failure is always this
// provider's own local misconfiguration (unreadable or invalid TLS material,
// token or endpoint), since the dial does not connect, so it takes the
// credential bound of 60 s, not the control-plane-down grace.
func SetNativeDegraded(reason string) {
	sharedNativeDegraded.Store(&nativeDegradation{since: sharedNow(), reason: reason})
}

// NativeDegradedReason returns why native control was unavailable at
// startup, or "".
func NativeDegradedReason() string {
	if degraded := sharedNativeDegraded.Load(); degraded != nil {
		return degraded.reason
	}
	return ""
}

// nativeDegradedPast reports a degraded start that has lasted past the
// outage bound for the pool, durably stamping its start on the pool.
func (d *Lifecycle) nativeDegradedPast(ctx context.Context, pool *unstructured.Unstructured) bool {
	degraded := sharedNativeDegraded.Load()
	if degraded == nil {
		return false
	}
	start := d.recordOutageSince(ctx, pool, sharedNativeOutageAnnotation, degraded.since)
	return !sharedNow().Before(start.Add(sharedProvenanceOutageBound))
}

// NativeCredentialsRejected returns why the native control plane rejects the
// provider's own credentials, or "" when it does not.
func NativeCredentialsRejected() string {
	if reason := sharedNativeCredentials.Load(); reason != nil {
		return *reason
	}
	return ""
}

// outsideAtespaceReaches reports a template outside the pool's Atespace whose
// selector, empty or not, the pool's labels satisfy: foreign whatever the
// provider's records say.
func outsideAtespaceReaches(templates []*pb.ActorTemplate, atespace string, poolLabels map[string]string) bool {
	return slices.ContainsFunc(templates, func(template *pb.ActorTemplate) bool {
		return template.GetMetadata().GetAtespace() != atespace && selectorMatches(template.GetWorkerSelector().GetMatchLabels(), poolLabels)
	})
}

// classifyWithoutInventory classifies the templates reaching a pool while the
// recorded-template inventory is unavailable. One outside the pool's Atespace
// is foreign. One inside it is unclassified unless the in-process record knows
// it by its exact UID.
func classifyWithoutInventory(templates []*pb.ActorTemplate, atespace string, poolLabels, cached map[string]string) (foreign, unclassified bool) {
	for _, template := range templates {
		if !selectorMatches(template.GetWorkerSelector().GetMatchLabels(), poolLabels) {
			continue
		}
		meta := template.GetMetadata()
		if meta.GetAtespace() != atespace {
			foreign = true
			continue
		}
		if uid, recorded := cached[meta.GetAtespace()+"/"+meta.GetName()]; !recorded || uid == "" || uid != meta.GetUid() {
			unclassified = true
		}
	}
	return foreign, unclassified
}

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

// failing reports a failure not yet cleared by a later success.
func (h *provenanceHealth) failing() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.failingSince.IsZero()
}

// since returns when the current outage began, or the zero time.
func (h *provenanceHealth) since() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failingSince
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
	if sharedProfileInventoryHealth.failing() {
		return errors.New("shared worker profiles cannot be listed, so shared admission is closed")
	}
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
var sharedEscalationTimeout = 10 * time.Second

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
				if err := d.deleteSharedWorkerPods(ctx, pool, provenance[pool.GetUID()].podUIDs, sharedEscalationGrace, "the worker Pod outlived its strong retirement", true); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
	feed(ctx, work, pools)
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
	d := New(r.Client, nil, r.Config).withSharedLoopPass("escalation")
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
var sharedBookkeepingBudget = 30 * time.Second

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
//
// escalation marks the hard bound's best-effort re-delete, which always
// re-deletes; a retirement's capped delete skips a Pod whose deletion already
// ends by its own deadline.
func (d *Lifecycle) deleteSharedWorkerPods(ctx context.Context, pool *unstructured.Unstructured, recorded map[string]bool, grace int64, reason string, escalation bool) error {
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
		// A failed read never drops a decided deletion: it falls through to
		// deleting only the Pods fenced on this pool, by UID.
		ctrl.LoggerFrom(ctx).Info("Substrate shared worker pool could not be re-read; deleting only its fenced worker Pods", "reason", err.Error())
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
		// A graceful delete sets the Pod's deletionTimestamp to when its
		// grace ends. A Pod whose deletion already ends by this delete's
		// deadline needs no re-delete, so retries never shorten a grace pass
		// by pass.
		if !escalation && pod.DeletionTimestamp != nil && !pod.DeletionTimestamp.Time.After(sharedNow().Add(time.Duration(grace)*time.Second)) {
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
//
// When the profiles could not be listed (profilesKnown false), every safety
// check still runs; only the work that needs profile shapes (closing on a
// changed scope, collection, ledger writes and warm creation) is skipped.
func (r *SpareWorkerReplenisher) sweepShared(ctx context.Context, d *Lifecycle, profiles []profile.SubstrateWorkspaceProfile, workers *spareWorkerCache, profilesKnown bool) error {
	// One snapshot of journals and workspaces is shared by every pool.
	d = d.withSharedLoopPass("sweep")
	// Durable outage stamps are written after every safety action.
	defer d.flushOutageMarks(ctx)
	listCtx, cancelList := isolationContext(ctx)
	discovered, err := listSharedPools(listCtx, r.Client, nil)
	cancelList()
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
	// Non-isolation work (profile discovery, closing, collection, ledger
	// writes and warm creation) has its own budget, started after every
	// isolation check, so a slow profile lookup can never fail one and a long
	// isolation scan never leaves it an expired context.
	var shapes map[string]*sharedProfileShape
	if len(pools.Items) == 0 {
		// No pool to isolate: only discovery decides whether anything is
		// shared, on a budget of its own.
		discoverCtx, cancelDiscover := freshBudget(ctx, sharedMaintenanceTimeout)
		shapes = r.sharedProfileShapes(discoverCtx, d, profiles)
		cancelDiscover()
		if len(shapes) == 0 {
			// Nothing shared remains, so the in-process template record is
			// pruned; a later shared check rereads it from Kubernetes.
			recordedTemplates.replace(nil, recordedTemplates.begin())
			return nil
		}
	}
	now := sharedNow()
	var errs []error
	// The isolation inventories, each on a budget of its own. Maintenance and
	// every pool creation need a complete, successful template inventory; a
	// failed inventory creates nothing.
	templatesCtx, cancelTemplates := freshBudget(ctx, sharedInventoryTimeout)
	templates, templatesErr := d.listAllActorTemplates(templatesCtx)
	cancelTemplates()
	if templatesErr == nil {
		// A reaching template outside a pool's Atespace is foreign from the
		// native inventory alone: those quarantines go out before the
		// fallible read of the provider's records, which only same-Atespace
		// templates need.
		var foreign []*sharedAction
		remaining := pools.Items[:0:0]
		for i := range pools.Items {
			pool := &pools.Items[i]
			if ledger, err := readSharedLedger(pool); err == nil && pool.GetDeletionTimestamp() == nil && r.Config.DedicatedInstall && outsideAtespaceReaches(templates, ledger.Scope.Atespace, pool.GetLabels()) {
				foreign = append(foreign, &sharedAction{pool: pool, state: sharedQuarantined, reason: errForeignTemplate.Error()})
				continue
			}
			remaining = append(remaining, *pool)
		}
		errs = append(errs, d.issueSharedActions(ctx, foreign)...)
		pools.Items = remaining
	}
	began := recordedTemplates.begin()
	recordsCtx, cancelRecords := freshBudget(ctx, sharedInventoryTimeout)
	all, shared, oursErr := d.providerTemplates(recordsCtx)
	cancelRecords()
	var ours map[string]string
	if oursErr == nil {
		// Every sweep refreshes the in-process record from Kubernetes, and
		// checks against it merged with anything recorded since.
		ours = recordedTemplates.refresh(all, shared, began)
	}
	// Without the provider's own template record (inventoryErr), a
	// same-Atespace template the in-process record does not know cannot be
	// told apart: its pool closes at once and is retired once the outage
	// outlasts its bound. Nothing new is created meanwhile.
	inventoryErr := oursErr
	// A native ActorTemplate inventory that has stayed unavailable past the
	// outage bound leaves foreign selectors unchecked: shared pools are then
	// strongly retired (nativeTemplateOutage).
	outage := &nativeTemplateOutage{d: d, failing: templatesErr != nil}
	if nativeCredentialsRejected(templatesErr) {
		outage.rejected = "provider native credentials rejected: " + status.Code(templatesErr).String()
	}
	// Every known strong trigger first, for every pool, before any pool's
	// fallible checks.
	var candidates []*sharedCandidate
	var actions []*sharedAction
	for i := range pools.Items {
		pool := &pools.Items[i]
		if pool.GetDeletionTimestamp() == nil {
			if reason := outage.retires(ctx, pool); reason != "" {
				actions = append(actions, &sharedAction{pool: pool, state: sharedRetiring, reason: reason})
				continue
			}
		}
		candidate, action := r.sharedKnownTriggers(ctx, d, pool, now, templates, ours, templatesErr, inventoryErr)
		if action != nil {
			actions = append(actions, action)
		}
		if candidate != nil {
			candidates = append(candidates, candidate)
		}
	}
	// The decided retirements, quarantines first, concurrently and capped.
	errs = append(errs, d.issueSharedActions(ctx, actions)...)
	// Then each pool's fallible checks, concurrently and capped, each on
	// budgets of its own.
	results := make([]*sharedIsolation, len(candidates))
	checkErrs := make([]error, len(candidates))
	work := make(chan int)
	var group sync.WaitGroup
	for range min(sharedCheckConcurrency, len(candidates)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := range work {
				results[i], checkErrs[i] = r.isolateSharedPool(ctx, d, workers, candidates[i])
			}
		}()
	}
	indexes := make([]int, len(candidates))
	for i := range indexes {
		indexes[i] = i
	}
	feed(ctx, work, indexes)
	close(work)
	group.Wait()
	errs = append(errs, checkErrs...)
	var verified []*sharedIsolation
	for _, isolated := range results {
		if isolated != nil {
			verified = append(verified, isolated)
		}
	}
	maintainCtx, cancelMaintain := freshBudget(ctx, sharedMaintenanceTimeout)
	defer cancelMaintain()
	if shapes == nil {
		shapes = r.sharedProfileShapes(maintainCtx, d, profiles)
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
	if profilesKnown {
		// Without the profiles every safety check above still ran; closing,
		// collection and ledger writes need their shapes.
		for _, isolated := range verified {
			if rootCancelled(ctx) {
				break
			}
			errs = append(errs, r.maintainSharedPool(maintainCtx, d, workers, isolated, shapes, free, now, templatesErr))
		}
	}
	if err := errors.Join(templatesErr, inventoryErr); err != nil {
		// Without a verified template inventory nothing new is created.
		return errors.Join(append(errs, err)...)
	}
	for uid, shape := range shapes {
		for free[uid] < shape.warm && !rootCancelled(ctx) {
			if err := d.createWarmSharedPool(maintainCtx, shape, templates, ours); err != nil {
				errs = append(errs, err)
				break
			}
			free[uid] += int(shape.scope.MaxActors)
		}
	}
	return errors.Join(errs...)
}

// sharedInventoryTimeout is the budget of each of the pass's isolation
// inventories (the native ActorTemplate List and the provider's records), and
// sharedMaintenanceTimeout that of its non-isolation work.
var (
	sharedInventoryTimeout   = 30 * time.Second
	sharedMaintenanceTimeout = 60 * time.Second
)

// sharedProfileShapes derives each opted-in profile's shape, for
// maintenance and warm creation only.
func (r *SpareWorkerReplenisher) sharedProfileShapes(ctx context.Context, d *Lifecycle, profiles []profile.SubstrateWorkspaceProfile) map[string]*sharedProfileShape {
	shapes := map[string]*sharedProfileShape{}
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
	return shapes
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

// sharedIsolation is what a pool's completed isolation checks leave for its
// non-isolation maintenance.
//
//nolint:gocyclo // Every pool-wide rule is evaluated in one pass, in priority order.
type sharedIsolation struct {
	pool         *unstructured.Unstructured
	ledger       *sharedLedger
	workers      []*pb.Worker
	orphans      []string
	unclassified bool
}

// sharedIsolationCheckTimeout is each isolation check's own budget: a pool's
// hosted-workspace check, its worker and assignment checks, and its ledger
// check each run on a fresh context, never on a deadline another step spent.
var sharedIsolationCheckTimeout = 15 * time.Second

// isolationContext is a fresh bounded context for one isolation check,
// detached from any deadline the pass already spent.
func isolationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return freshBudget(ctx, sharedIsolationCheckTimeout)
}

// freshBudget returns a context with a whole budget of its own: it drops any
// deadline the caller's context already spent, but still ends when the
// sweep's original context is cancelled (for example on shutdown or lost
// leadership), even after a deadline derived from it has expired.
func freshBudget(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	root := ctx
	if sweep, ok := ctx.Value(sweepBudgetKey{}).(sweepBudget); ok && sweep.root != nil {
		root = sweep.root
	}
	fresh, cancel := context.WithTimeout(context.WithoutCancel(ctx), passBudget(ctx, budget))
	stop := context.AfterFunc(root, func() {
		if errors.Is(root.Err(), context.Canceled) {
			cancel()
		}
	})
	return fresh, func() {
		stop()
		cancel()
	}
}

// sweepBudgetKey carries a sweep's own deadline, which caps each of its
// fresh budgets (every step still gets a whole budget of its own, never one
// another step spent, but none is longer than the sweep's configured
// deadline), and the sweep's original context, whose cancellation ends them.
type sweepBudgetKey struct{}

type sweepBudget struct {
	limit time.Duration
	root  context.Context
}

// withSweepBudget records ctx, which must not yet carry the sweep's own
// deadline, as the cancellation root.
func withSweepBudget(ctx context.Context, limit time.Duration) context.Context {
	return context.WithValue(ctx, sweepBudgetKey{}, sweepBudget{limit: limit, root: ctx})
}

func passBudget(ctx context.Context, budget time.Duration) time.Duration {
	if sweep, ok := ctx.Value(sweepBudgetKey{}).(sweepBudget); ok && sweep.limit > 0 && sweep.limit < budget {
		return sweep.limit
	}
	return budget
}

// isolateSharedPool runs a candidate pool's fallible isolation checks:
// hosted suspicion, its workers and assignments, and its ledger's hosted
// records. A strong trigger retires the pool. A check
// that fails or does not finish within its own budget makes the pool
// unverified (verificationFailed); only a pool whose checks all completed is
// returned for maintenance.
// sharedAction is a strong retirement a pass decided from data in hand.
type sharedAction struct {
	pool          *unstructured.Unstructured
	state, reason string
}

// rootCancelled reports the loop's own context cancelled (shutdown or lost
// leadership). A deadline the pass already spent does not count: every loop
// checks this between iterations, so the provider stops promptly when the
// manager stops, while work in flight finishes within its own short bound.
func rootCancelled(ctx context.Context) bool {
	return errors.Is(rootContext(ctx).Err(), context.Canceled)
}

// rootContext is the loop's own context, without any deadline the pass
// derived from it.
func rootContext(ctx context.Context) context.Context {
	if sweep, ok := ctx.Value(sweepBudgetKey{}).(sweepBudget); ok && sweep.root != nil {
		return sweep.root
	}
	return ctx
}

// feed hands items to workers until the loop's own context is cancelled,
// including while a hand-off is waiting for a free worker.
func feed[T any](ctx context.Context, work chan<- T, items []T) {
	done := rootContext(ctx).Done()
	for _, item := range items {
		if rootCancelled(ctx) {
			return
		}
		select {
		case work <- item:
		case <-done:
			if rootCancelled(ctx) {
				return
			}
			work <- item
		}
	}
}

// poolDeadline is a pool with the escalation time its worker Pods' grace is
// capped by.
type poolDeadline struct {
	pool       *unstructured.Unstructured
	escalateAt time.Time
}

// retryCappedPodDeletes retries the capped worker Pod delete of deleting
// pools, concurrently and capped, each on its own short bound.
func (d *Lifecycle) retryCappedPodDeletes(ctx context.Context, pools []poolDeadline) []error {
	errs := make([]error, len(pools))
	work := make(chan int)
	var group sync.WaitGroup
	for range min(sharedRetireConcurrency, len(pools)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := range work {
				_, errs[i] = d.deleteSharedPool(ctx, pools[i].pool, pools[i].escalateAt)
			}
		}()
	}
	indexes := make([]int, len(pools))
	for i := range indexes {
		indexes[i] = i
	}
	feed(ctx, work, indexes)
	close(work)
	group.Wait()
	return errs
}

// sharedRetireConcurrency caps a pass's concurrent decided retirements.
const sharedRetireConcurrency = 8

// issueSharedActions issues decided strong retirements concurrently,
// capped, quarantines first, each on its own short bound (strong cleanup),
// and stops issuing new ones once the loop is cancelled.
func (d *Lifecycle) issueSharedActions(ctx context.Context, actions []*sharedAction) []error {
	slices.SortStableFunc(actions, func(a, b *sharedAction) int {
		switch {
		case a.state == sharedQuarantined && b.state != sharedQuarantined:
			return -1
		case a.state != sharedQuarantined && b.state == sharedQuarantined:
			return 1
		}
		return 0
	})
	errs := make([]error, len(actions))
	work := make(chan int)
	var group sync.WaitGroup
	for range min(sharedRetireConcurrency, len(actions)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := range work {
				errs[i] = d.retireSharedPool(ctx, actions[i].pool, actions[i].state, actions[i].reason)
			}
		}()
	}
	indexes := make([]int, len(actions))
	for i := range indexes {
		indexes[i] = i
	}
	feed(ctx, work, indexes)
	close(work)
	group.Wait()
	return errs
}

// sharedCandidate is a pool whose known strong triggers did not retire it,
// awaiting its fallible isolation checks.
type sharedCandidate struct {
	pool         *unstructured.Unstructured
	ledger       *sharedLedger
	unclassified bool
}

// sharedKnownTriggers evaluates, without any read, the strong triggers the
// pass already knows for a pool: its persisted state, its lifetime and the
// pass's template inventory. It runs for every pool before any pool's
// fallible checks, so stalled reads on other pools can never postpone a
// known quarantine. It returns the pool as a candidate for its fallible
// checks, or nil when it was retired or needs none.
func (r *SpareWorkerReplenisher) sharedKnownTriggers(ctx context.Context, d *Lifecycle, pool *unstructured.Unstructured, now time.Time, templates []*pb.ActorTemplate, ours map[string]string, templatesErr, inventoryErr error) (*sharedCandidate, *sharedAction) {
	if pool.GetDeletionTimestamp() != nil {
		return nil, nil
	}
	ledger, err := readSharedLedger(pool)
	if err != nil {
		// Unreadable lifetime evidence is doubt: retire strongly at once.
		return nil, &sharedAction{pool: pool, state: sharedRetiring, reason: "the shared worker ledger is unreadable"}
	}
	if ledger.State == sharedRetiring || ledger.State == sharedQuarantined {
		return nil, &sharedAction{pool: pool, state: ledger.State, reason: ledger.Reason}
	}
	if !now.Before(sharedRetireAt(pool, ledger)) || !r.Config.DedicatedInstall {
		// enforceSharedBounds already retired it, without native reads.
		return nil, nil
	}
	unclassified := false
	if templatesErr == nil && inventoryErr == nil && templatesReach(templates, ledger.Scope.Atespace, pool.GetLabels(), ours) {
		return nil, &sharedAction{pool: pool, state: sharedQuarantined, reason: errForeignTemplate.Error()}
	}
	if templatesErr == nil && inventoryErr != nil {
		var foreign bool
		foreign, unclassified = classifyWithoutInventory(templates, ledger.Scope.Atespace, pool.GetLabels(), recordedTemplates.snapshot())
		if foreign {
			return nil, &sharedAction{pool: pool, state: sharedQuarantined, reason: errForeignTemplate.Error()}
		}
		if unclassified && sharedTemplateInventoryHealth.outlasted(sharedNow()) {
			return nil, &sharedAction{pool: pool, state: sharedRetiring, reason: "an unrecorded ActorTemplate reaches the shared worker and the provider's template inventory stayed unavailable past its bound"}
		}
	}
	return &sharedCandidate{pool: pool, ledger: ledger, unclassified: unclassified}, nil
}

// sharedCheckConcurrency caps the pools whose fallible isolation checks run
// at once, so stalled reads do not add up across pools.
const sharedCheckConcurrency = 8

func (r *SpareWorkerReplenisher) isolateSharedPool(ctx context.Context, d *Lifecycle, workers *spareWorkerCache, candidate *sharedCandidate) (*sharedIsolation, error) {
	pool, ledger, unclassified := candidate.pool, candidate.ledger, candidate.unclassified
	// Kubernetes-only suspicion comes before any native read, on its own
	// budget. A failed check is held back, not returned, so the worker checks
	// below still run, each on a fresh budget of its own.
	hostedCtx, cancelHosted := isolationContext(ctx)
	suspect, hostedErr := d.sharedPoolHostsSuspect(hostedCtx, pool, ledger)
	cancelHosted()
	if hostedErr == nil && suspect {
		return nil, d.retireSharedPool(ctx, pool, sharedRetiring, "a hosted allocation selected the strong path or its workspace is suspect")
	}
	nativeCtx, cancelNative := isolationContext(ctx)
	defer cancelNative()
	poolWorkers, err := workers.pool(nativeCtx, pool)
	if err != nil {
		return nil, d.verificationFailed(ctx, pool, true, errors.Join(hostedErr, err))
	}
	if len(poolWorkers) > 1 {
		return nil, d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker pool has more than one worker")
	}
	// Each worker's assignments are listed once and shared by both checks.
	assignments := map[string][]*pb.ActorAssignment{}
	for _, worker := range poolWorkers {
		if ledger.Worker != nil && (worker.GetMetadata().GetUid() != ledger.Worker.UID || worker.GetWorkerPodUid() != ledger.Worker.PodUID) {
			return nil, d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker was replaced")
		}
		if ledger.Worker != nil && worker.GetStatus().GetRegisteredEpoch() != ledger.Worker.Epoch {
			// A restarted worker process lost every sandbox; its Actors fail
			// and retire through the strong path, which this anticipates.
			return nil, d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker process restarted")
		}
		listed, err := d.listWorkerAssignments(nativeCtx, worker.GetMetadata().GetName())
		if err != nil {
			return nil, d.verificationFailed(ctx, pool, true, errors.Join(hostedErr, err))
		}
		assignments[worker.GetMetadata().GetName()] = listed
		foreign, err := sharedForeignAssignment(nativeCtx, d, pool, ledger, worker.GetMetadata().GetName(), listed)
		if err != nil {
			return nil, d.verificationFailed(ctx, pool, true, errors.Join(hostedErr, err))
		}
		if foreign {
			// Confirmed on two listings: no later read can drop it. The
			// retirement runs on its own cleanup context, and its Delete is
			// preconditioned on this pool's UID, so a replaced pool is never
			// touched.
			return nil, d.retireSharedPool(ctx, pool, sharedQuarantined, "the shared worker hosts an Actor outside its ledger")
		}
	}
	if hostedErr != nil {
		// The hosted check did not complete: the pool is unverified.
		return nil, d.verificationFailed(ctx, pool, false, hostedErr)
	}
	if len(poolWorkers) == 0 && slices.ContainsFunc(ledger.Entries, func(entry sharedLedgerEntry) bool { return entry.ActorUID != "" }) {
		// A pool hosting booted Actors whose worker cannot be listed cannot
		// be checked: it is unverified, never "nothing to check". A claim
		// still booting, or an empty warm pool, stays neutral.
		return nil, d.verificationFailed(ctx, pool, true, errors.New("the shared worker hosts booted Actors but no native worker is listed for it"))
	}
	ledgerCtx, cancelLedger := isolationContext(ctx)
	hostedSuspect, orphans, err := d.sharedLedgerHosts(ledgerCtx, pool, ledger, poolWorkers, assignments)
	cancelLedger()
	if err != nil {
		return nil, d.verificationFailed(ctx, pool, false, err)
	}
	// Every isolation check completed. A pool with a worker, or with no
	// hosted entry to verify, is verified; only that clears its record.
	if len(poolWorkers) > 0 || len(ledger.Entries) == 0 {
		d.verificationSucceeded(ctx, pool)
	}
	if hostedSuspect {
		// Neighbours are suspended through Orka core's eviction drain of the
		// terminating worker; the suspect Actor retires through the strong path.
		return nil, d.retireSharedPool(ctx, pool, sharedRetiring, "a hosted workspace is tainted, quarantined or failed")
	}
	return &sharedIsolation{pool: pool, ledger: ledger, workers: poolWorkers, orphans: orphans, unclassified: unclassified}, nil
}

// maintainSharedPool is a verified pool's non-isolation maintenance, after
// profile discovery: closing admission, collecting orphans and idle pools,
// and ledger writes. It needs the profiles' shapes, so it is skipped when
// they could not be listed.
func (r *SpareWorkerReplenisher) maintainSharedPool(ctx context.Context, d *Lifecycle, workers *spareWorkerCache, isolated *sharedIsolation, shapes map[string]*sharedProfileShape, free map[string]int, now time.Time, templatesErr error) error {
	pool, ledger, orphans := isolated.pool, isolated.ledger, isolated.orphans
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
	if closing == "" && (templatesErr != nil || isolated.unclassified) {
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
	if d.sharedPass == nil || err == nil && found && record != nil && sharedEntryHeld(record, pool, entry) || errors.Is(err, errPassInventoryFailed) {
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
	// failed this pass; failedErr and journalsErr cache those failures, so
	// no lookup re-requests them.
	listed, failed map[string]bool
	failedErr      map[string]error
	journalsErr    error
	// markWrites and markSlots track and cap the pass's background outage
	// annotation writes.
	markWrites sync.WaitGroup
	markSlots  chan struct{}
	// loop and generation identify the pass among its loop's passes; a
	// writer of a superseded pass stops.
	loop       *atomic.Uint64
	generation uint64
	// probed and down cache the pass's native control-plane probe, and
	// probeMu makes it a single flight.
	probed, down bool
	probeMu      sync.Mutex
	// namespaces serializes each namespace's first List across concurrent
	// per-pool checks, so a namespace is still listed once per pass.
	namespaces keyedMutex
}

// keyedMutex is one mutex per key.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*sync.Mutex{}
	}
	m, ok := k.locks[key]
	if !ok {
		m = &sync.Mutex{}
		k.locks[key] = m
	}
	k.mu.Unlock()
	m.Lock()
	return m.Unlock
}

func newSharedPassReads() *sharedPassReads {
	return &sharedPassReads{workspaces: map[string]map[string]*api.ExecutionWorkspace{}, listed: map[string]bool{}, failed: map[string]bool{}}
}

// noteWorkspaceListFailed records a namespace whose List failed this pass.
func (p *sharedPassReads) noteWorkspaceListFailed(namespace string, err error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed[namespace] = true
	if p.failedErr == nil {
		p.failedErr = map[string]error{}
	}
	p.failedErr[namespace] = fmt.Errorf("%w: the ExecutionWorkspaces of namespace %s could not be listed: %w", errPassInventoryFailed, namespace, err)
}

// noteJournalsFailed records that the pass's journal inventory List failed.
func (p *sharedPassReads) noteJournalsFailed(err error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.journalsErr = fmt.Errorf("%w: the journal inventory could not be listed: %w", errPassInventoryFailed, err)
}

// errPassInventoryFailed marks a lookup answered from a pass inventory List
// that failed. The failure is cached for the rest of the pass (a negative
// cache): no pool re-requests it, and every pool that needed it is unverified
// at once, so its outage bound keeps running.
var errPassInventoryFailed = errors.New("a pass inventory List failed")

// withSharedPass returns a Lifecycle that shares one pass's reads.
func (d *Lifecycle) withSharedPass() *Lifecycle {
	pass := *d
	pass.sharedPass = newSharedPassReads()
	return &pass
}

// sharedLoopPasses counts each loop's passes, so a pass's background writes
// stop once a later pass of the same loop has started.
var sharedLoopPasses = map[string]*atomic.Uint64{"sweep": {}, "bounds": {}, "escalation": {}}

// withSharedLoopPass is withSharedPass for a pass of the named loop.
func (d *Lifecycle) withSharedLoopPass(loop string) *Lifecycle {
	pass := d.withSharedPass()
	if counter := sharedLoopPasses[loop]; counter != nil {
		pass.sharedPass.loop, pass.sharedPass.generation = counter, counter.Add(1)
	}
	return pass
}

// superseded reports a later pass of the same loop.
func (p *sharedPassReads) superseded() bool {
	return p != nil && p.loop != nil && p.loop.Load() != p.generation
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
		journalsErr := p.journalsErr
		p.mu.Unlock()
		if found {
			cached.DeepCopyInto(cm)
			return nil
		}
		if journalsErr != nil {
			return journalsErr
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
	unlock := p.namespaces.lock(key.Namespace)
	p.mu.Lock()
	listed, failed := p.listed[key.Namespace], p.failed[key.Namespace]
	p.mu.Unlock()
	if listed || failed {
		unlock()
	} else {
		defer unlock()
	}
	if failed {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.failedErr[key.Namespace]
	}
	if !listed {
		workspaces := &api.ExecutionWorkspaceList{}
		if err := d.client.List(ctx, workspaces, client.InNamespace(key.Namespace)); err != nil {
			p.noteWorkspaceListFailed(key.Namespace, err)
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.failedErr[key.Namespace]
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
