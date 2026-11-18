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
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	provenance, provenanceErr := d.sharedJournalProvenance(ctx)
	pools, err := listSharedPools(ctx, r.Client, provenance)
	if err != nil {
		return err
	}
	now := sharedNow()
	var errs []error
	if provenanceErr != nil {
		// Unavailable provenance is doubt: the mutable annotations cannot be
		// checked, so every pool is bounded by its server-set creation time
		// plus the minimum lifetime, never by the annotations' value.
		errs = append(errs, provenanceErr)
	}
	for i := range pools {
		pool := &pools[i]
		recorded := provenance[pool.GetUID()]
		retireAt, escalateAt, evidence := sharedBoundsOf(pool, recorded, provenanceErr == nil)
		if pool.GetDeletionTimestamp() == nil {
			switch {
			case !sharedLabelsIntact(pool):
				errs = append(errs, d.hardRetireSharedPool(ctx, pool, sharedRetiring, "the shared worker carries shared evidence without its provider-owned label"))
			case !r.Config.DedicatedInstall:
				errs = append(errs, d.hardRetireSharedPool(ctx, pool, sharedRetiring, errSharedWorkersNotDedicated.Error()))
			case !evidence:
				errs = append(errs, d.hardRetireSharedPool(ctx, pool, sharedRetiring, "the shared worker's lifetime evidence is unreadable or inconsistent"))
			case recorded.strong:
				// A journal that recorded this pool's UID holds an unreleased
				// strong marker or failure, whatever the mutable entry list
				// now says.
				errs = append(errs, d.hardRetireSharedPool(ctx, pool, sharedRetiring, "a journal that recorded this shared worker selected the strong path or failed"))
			case sharedRetirementPersisted(pool):
				// A retirement state saved before a crash, or before a native
				// sweep ran out of time, is unconditional deletion intent.
				state, reason := sharedPersistedRetirement(pool)
				errs = append(errs, d.hardRetireSharedPool(ctx, pool, state, reason))
			case d.sharedStrongMarked(ctx, pool):
				// A hosted journal persisted the strong path or a failure
				// (possibly crashing before it deleted the pool), or a hosted
				// workspace became suspect; act on it with no native read.
				errs = append(errs, d.retireSharedPool(ctx, pool, sharedRetiring, "a hosted allocation selected the strong path or its workspace is suspect"))
			case !now.Before(retireAt):
				errs = append(errs, d.hardRetireSharedPool(ctx, pool, sharedRetiring, "the shared worker reached the start of its strong retirement before maxWorkerLifetime"))
			}
		}
		if !now.Before(escalateAt) {
			errs = append(errs, d.escalateSharedWorkerPods(ctx, pool))
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
}

// sharedJournalProvenance reads every journal and archived record (Kubernetes
// only) and groups the frozen scopes they recorded by shared pool UID.
func (d *Lifecycle) sharedJournalProvenance(ctx context.Context) (map[types.UID]sharedProvenance, error) {
	journals := &corev1.ConfigMapList{}
	if err := d.client.List(ctx, journals, client.HasLabels{ownershipLabel, providerLabel}); err != nil {
		return nil, err
	}
	out := map[types.UID]sharedProvenance{}
	for i := range journals.Items {
		record, ok := decodeClaimRecord(&journals.Items[i])
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
		if !record.SharedWorker.Released && (record.SharedWorker.Strong != "" || record.Failure != "") {
			entry.strong = true
		}
		out[uid] = entry
	}
	return out, nil
}

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
	if provenance.found {
		ceiling = minTime(ceiling, created.Add(provenance.lifetime))
	}
	ledger, err := readSharedLedger(pool)
	recorded, recordedOK := sharedRecordedDeadline(pool)
	consistent := err == nil && recordedOK && recorded.Equal(ledger.BornAt.Add(ledger.Scope.Lifetime.Duration))
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

// escalateSharedWorkerPods re-deletes the pool's worker Pods with a
// one-second grace once the hard bound nears. By then the worker has had its
// full drain and Orka core's eviction window has ended, so every neighbour was
// suspended or failed without replay. A one-second grace keeps the absence
// proof kubelet-confirmed, which a zero grace would not.
//
// It does not wait for the pool's deletion to have started: a deletion that
// failed must not postpone the bound.
func (d *Lifecycle) escalateSharedWorkerPods(ctx context.Context, pool *unstructured.Unstructured) error {
	pods := &corev1.PodList{}
	if err := d.client.List(ctx, pods, client.InNamespace(pool.GetNamespace()), client.MatchingLabels{workerPoolLabel: pool.GetName()}); err != nil {
		return err
	}
	var errs []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds <= sharedEscalationGrace {
			continue
		}
		grace := sharedEscalationGrace
		ctrl.LoggerFrom(ctx).Info("Substrate shared worker Pod escalated before its hard lifetime bound", "reason", "the worker Pod outlived its strong retirement")
		err := d.client.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}, client.GracePeriodSeconds(grace))
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
		return nil
	}
	now := sharedNow()
	// Maintenance and every pool creation need a complete, successful template
	// inventory; a failed inventory creates nothing.
	templates, templatesErr := d.listAllActorTemplates(ctx)
	ours, oursErr := d.providerTemplates(ctx)
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
	name, err := randomName(sharedPoolPrefix)
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
	// Kubernetes-only suspicion comes before any native read in this pass.
	if suspect, err := d.sharedPoolHostsSuspect(ctx, pool, ledger); err != nil {
		return err
	} else if suspect {
		return d.retireSharedPool(ctx, pool, sharedRetiring, "a hosted allocation selected the strong path or its workspace is suspect")
	}
	if templatesErr == nil && templatesReach(templates, ledger.Scope.Atespace, pool.GetLabels(), ours) {
		return d.retireSharedPool(ctx, pool, sharedQuarantined, errForeignTemplate.Error())
	}
	poolWorkers, err := workers.pool(ctx, pool)
	if err != nil {
		return err
	}
	if len(poolWorkers) > 1 {
		return d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker pool has more than one worker")
	}
	for _, worker := range poolWorkers {
		if ledger.Worker != nil && (worker.GetMetadata().GetUid() != ledger.Worker.UID || worker.GetWorkerPodUid() != ledger.Worker.PodUID) {
			return d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker was replaced")
		}
		if ledger.Worker != nil && worker.GetStatus().GetRegisteredEpoch() != ledger.Worker.Epoch {
			// A restarted worker process lost every sandbox; its Actors fail
			// and retire through the strong path, which this anticipates.
			return d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker process restarted")
		}
		foreign, err := sharedForeignAssignment(ctx, d, pool, worker.GetMetadata().GetName())
		if err != nil {
			return err
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
	hostedSuspect, orphans, err := d.sharedLedgerHosts(ctx, pool, ledger, poolWorkers)
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
func sharedForeignAssignment(ctx context.Context, d *Lifecycle, pool *unstructured.Unstructured, worker string) (bool, error) {
	assignments, err := d.listWorkerAssignments(ctx, worker)
	if err != nil || len(assignments) == 0 {
		return false, err
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
func (d *Lifecycle) sharedLedgerHosts(ctx context.Context, pool *unstructured.Unstructured, ledger *sharedLedger, poolWorkers []*pb.Worker) (bool, []string, error) {
	assigned := map[string]bool{}
	for _, worker := range poolWorkers {
		response, err := d.control.ListWorkerActorAssignments(ctx, &pb.ListWorkerActorAssignmentsRequest{Worker: &pb.ObjectRef{Name: worker.GetMetadata().GetName()}, Options: &pb.ListOptions{PageSize: 1000}})
		if err != nil {
			return false, nil, err
		}
		for _, assignment := range response.GetActorAssignments() {
			assigned[assignment.GetActor().GetAtespace()+"/"+assignment.GetActor().GetName()] = true
		}
		if response.GetNextPageToken() != "" {
			// More than a page of Actors on a worker of at most 16 slots is
			// itself foreign; collect nothing this pass.
			return false, nil, nil
		}
	}
	suspect := false
	var orphans []string
	for _, entry := range ledger.Entries {
		record, journalFound, err := d.sharedEntryRecord(ctx, entry.Journal)
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
		if err := d.client.Get(ctx, clientKey(record.Request.Key), workspace); err != nil {
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
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: parts[0], Name: parts[1]}, journal); err != nil {
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
		if err := d.client.Get(ctx, types.NamespacedName{Namespace: parts[0], Name: historyName(parts[1], sequence)}, archived); err != nil {
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
