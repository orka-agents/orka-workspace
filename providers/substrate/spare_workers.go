package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

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
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Spare workers take private worker birth off the allocation critical path.
// A spare is a private single-replica WorkerPool built by buildPrivatePoolSpec,
// exactly as an allocation would build its own, but born with unique spare
// labels that no template selects. An allocation claims an ACTIVE, empty,
// exactly-sized spare with one optimistic-lock write and then treats it as its
// ordinary private pool; anything doubtful falls back to a fresh pool.
const (
	// spareWorkersAnnotation is the mutable per-profile spare count. It is
	// not part of the profile hash, which covers only apiVersion, kind, spec.
	spareWorkersAnnotation = "substrate.workspace.orka.ai/spare-workers"
	maxSpareWorkers        = 8
	// spareProfileLabel marks an unclaimed spare with its profile UID. The
	// claim removes it, so a claimed pool never re-enters the spare set.
	spareProfileLabel = "substrate.workspace.orka.ai/spare-profile-uid"
	// spareSourceAnnotation and spareShapeAnnotation record the operator pool
	// UID and the spec digest without birth labels the spare was built from.
	spareSourceAnnotation  = "substrate.workspace.orka.ai/spare-source-uid"
	spareShapeAnnotation   = "substrate.workspace.orka.ai/spare-shape"
	spareProfileAnnotation = "substrate.workspace.orka.ai/spare-profile"
	// spareClaimAnnotation names the exact journal lifetime and sequence that
	// claimed the pool: <namespace>/<name>/<uid>/<sequence>.
	spareClaimAnnotation = "substrate.workspace.orka.ai/spare-claim"
	spareNamePrefix      = "ate-spare-"
	// spareResyncPeriod bounds how long a claimed spare takes to be replaced.
	spareResyncPeriod = 10 * time.Second
	// spareOrphanPeriod bounds how often claimed pools are checked against
	// their journals.
	spareOrphanPeriod = time.Minute
)

// spareClaim is the journal's record of a selected spare WorkerPool. The
// journal is authoritative: a pool binds to this record only when it carries
// this record's own claim marker, and RuntimePool.UID is saved only then.
type spareClaim struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
	// ProfileUID and Birth reproduce the spare's unclaimed labels.
	ProfileUID string `json:"profileUID"`
	Birth      string `json:"birth"`
	// ClaimIssued is saved before the claim write, like CreateIssued.
	ClaimIssued bool `json:"claimIssued,omitempty"`
	// Released disclaims a spare whose claim write never proved to have
	// applied, so a late-applied claim is an orphan, never this record's.
	Released bool `json:"released,omitempty"`
}

// spareWorkerCount parses the annotation strictly: absent is 0, and anything
// other than one decimal digit from 0 to 8 is invalid (and treated as 0).
func spareWorkerCount(annotations map[string]string) (int, error) {
	raw, found := annotations[spareWorkersAnnotation]
	if !found {
		return 0, nil
	}
	if len(raw) != 1 || raw[0] < '0' || raw[0] > '0'+maxSpareWorkers {
		return 0, fmt.Errorf("annotation %s must be an integer from 0 to %d", spareWorkersAnnotation, maxSpareWorkers)
	}
	return int(raw[0] - '0'), nil
}

func spareBirth(name string) map[string]string {
	return map[string]string{workerAllocationLabel: name, workerInstanceLabel: name}
}

// spareLabels are an unclaimed spare's exact WorkerPool labels. Agent
// Substrate copies WorkerPool labels onto its Workers, and these share no key
// with any operator selector, so no template can place an Actor on a spare.
func spareLabels(profileUID, birth string) map[string]string {
	out := spareBirth(birth)
	out[spareProfileLabel] = profileUID
	return out
}

func claimMarker(cm *corev1.ConfigMap, record *journalRecord) string {
	return cm.Namespace + "/" + cm.Name + "/" + string(cm.UID) + "/" + strconv.FormatInt(record.Request.Sequence, 10)
}

// withBirthLabels replaces the worker birth label values in a private pool
// specification, leaving every other byte of its canonical form unchanged.
func withBirthLabels(spec json.RawMessage, birth map[string]string) (json.RawMessage, error) {
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(spec))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("private worker specification is invalid")
	}
	podLabels, found, err := unstructured.NestedStringMap(object, "template", "labels")
	if err != nil || !found {
		return nil, fmt.Errorf("private worker specification has no birth labels")
	}
	maps.Copy(podLabels, birth)
	if err := unstructured.SetNestedStringMap(object, podLabels, "template", "labels"); err != nil {
		return nil, err
	}
	return json.Marshal(object)
}

func poolSpecDigest(pool *unstructured.Unstructured) (string, bool) {
	spec, found, err := unstructured.NestedMap(pool.Object, "spec")
	if err != nil || !found {
		return "", false
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return "", false
	}
	return digest(data), true
}

// unclaimedSpare reports whether a pool is exactly an unclaimed spare of this
// profile and namespace: spare labels only, no owner, no claim marker.
func unclaimedSpare(pool *unstructured.Unstructured, profileUID string) bool {
	return pool.GetUID() != "" && pool.GetDeletionTimestamp() == nil && strings.HasPrefix(pool.GetName(), spareNamePrefix) &&
		len(pool.GetOwnerReferences()) == 0 && pool.GetAnnotations()[spareClaimAnnotation] == "" &&
		maps.Equal(pool.GetLabels(), spareLabels(profileUID, pool.GetName()))
}

// namespaceWorkers lists native Workers once and groups those in a namespace
// by WorkerPool.
func (d *Lifecycle) namespaceWorkers(ctx context.Context, namespace string) (map[string][]*pb.Worker, error) {
	out := map[string][]*pb.Worker{}
	token := ""
	seen := map[string]bool{}
	for pages := 0; ; pages++ {
		if pages == 100 {
			return nil, fmt.Errorf("native worker listing exceeded bound")
		}
		workers, err := d.control.ListWorkers(ctx, &pb.ListWorkersRequest{Options: &pb.ListOptions{PageSize: 1000, PageToken: token}})
		if err != nil {
			return nil, err
		}
		for _, worker := range workers.GetWorkers() {
			if worker.GetWorkerNamespace() == namespace {
				out[worker.GetWorkerPool()] = append(out[worker.GetWorkerPool()], worker)
			}
		}
		token = workers.GetNextPageToken()
		if token == "" {
			return out, nil
		}
		if seen[token] {
			return nil, fmt.Errorf("native worker pagination repeated a token")
		}
		seen[token] = true
	}
}

// spareEligible checks the spare's only worker is ACTIVE, registered, gVisor,
// sized exactly for this record's Actor, labeled only as a spare, hosting no
// Actor, and running in its exact unterminated Pod. prepareWorker repeats the
// startup checks after the claim.
func (d *Lifecycle) spareEligible(ctx context.Context, record *journalRecord, pool *unstructured.Unstructured, workers []*pb.Worker, profileUID string) (bool, error) {
	if len(workers) != 1 {
		return false, nil
	}
	worker := workers[0]
	allocated := worker.GetStatus().GetAllocated()
	if worker.GetMetadata().GetUid() == "" || worker.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_ACTIVE || worker.GetStatus().GetRegisteredEpoch() <= 0 || allocated.GetActors() != 0 || allocated.GetCpuMillicores() != 0 || allocated.GetMemoryBytes() != 0 {
		return false, nil
	}
	if verifySingleActorWorker(record, worker) != nil || !maps.Equal(worker.GetLabels(), spareLabels(profileUID, pool.GetName())) {
		return false, nil
	}
	if hosts, err := d.workerHostsActor(ctx, worker.GetMetadata().GetName()); err != nil || hosts {
		return false, err
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: worker.GetWorkerNamespace(), Name: worker.GetWorkerPod()}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if string(pod.UID) != worker.GetWorkerPodUid() || pod.DeletionTimestamp != nil || pod.Labels[workerPoolLabel] != pool.GetName() {
		return false, nil
	}
	for key, value := range spareBirth(pool.GetName()) {
		if pod.Labels[key] != value {
			return false, nil
		}
	}
	return true, nil
}

// selectSpare picks an eligible spare for a record whose fresh private pool
// was just prepared, and rebinds the record to it. Nothing is written: the
// claim is journaled and issued by ensureRuntimePool. Any doubt keeps the
// fresh pool.
func (d *Lifecycle) selectSpare(ctx context.Context, record *journalRecord, profileUID types.UID) error {
	log := ctrl.LoggerFrom(ctx)
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion(workerPoolAPIVersion())
	pools.SetKind("WorkerPoolList")
	if err := d.client.List(ctx, pools, client.InNamespace(record.Placement.Namespace), client.MatchingLabels{spareProfileLabel: string(profileUID)}); err != nil {
		log.Info("Substrate spare workers unavailable; creating a fresh private worker", "reason", err.Error())
		return nil
	}
	shape, err := runtimePoolRestoreDigest(record.RuntimePoolSpec, false)
	if err != nil {
		return err
	}
	var candidates []*unstructured.Unstructured
	for i := range pools.Items {
		pool := &pools.Items[i]
		if unclaimedSpare(pool, string(profileUID)) && pool.GetAnnotations()[spareSourceAnnotation] == string(record.Placement.UID) && pool.GetAnnotations()[spareShapeAnnotation] == shape {
			candidates = append(candidates, pool)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	// Concurrent allocations start from different spares, so they rarely
	// race for one; a lost race falls back to a fresh pool.
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	workers, err := d.namespaceWorkers(ctx, record.Placement.Namespace)
	if err != nil {
		log.Info("Substrate spare workers unavailable; creating a fresh private worker", "reason", err.Error())
		return nil
	}
	for _, pool := range candidates {
		expected, err := withBirthLabels(record.RuntimePoolSpec, spareBirth(pool.GetName()))
		if err != nil {
			return err
		}
		if actual, ok := poolSpecDigest(pool); !ok || actual != digest(expected) {
			continue
		}
		eligible, err := d.spareEligible(ctx, record, pool, workers[pool.GetName()], string(profileUID))
		if err != nil {
			log.Info("Substrate spare workers unavailable; creating a fresh private worker", "reason", err.Error())
			return nil
		}
		if !eligible {
			continue
		}
		operator := operatorPoolLabels(record)
		record.SpareWorker = &spareClaim{Name: pool.GetName(), UID: string(pool.GetUID()), ProfileUID: string(profileUID), Birth: pool.GetName()}
		record.RuntimePoolSpec = expected
		return bindPrivatePool(record, pool.GetName(), operator)
	}
	return nil
}

// ensureSpareRuntimePool claims the selected spare for this record, or falls
// back to a fresh pool. The journal records ClaimIssued before the claim and
// the pool UID only after this record's own marker is observed on the pool.
func (d *Lifecycle) ensureSpareRuntimePool(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	spare := record.SpareWorker
	key := client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}
	pool := poolObject()
	err := d.client.Get(ctx, key, pool)
	if record.RuntimePool.UID != "" {
		// Bound: from here on this is an ordinary private pool.
		if apierrors.IsNotFound(err) {
			return sdk.ErrStaleIdentity
		}
		if err != nil {
			return err
		}
		if string(pool.GetUID()) != record.RuntimePool.UID || pool.GetDeletionTimestamp() != nil || !runtimePoolMatches(pool, record) {
			return sdk.ErrStaleIdentity
		}
		return nil
	}
	if apierrors.IsNotFound(err) {
		return d.abandonSpare(ctx, cm, record, "the spare disappeared")
	}
	if err != nil {
		return err
	}
	marker := claimMarker(cm, record)
	if string(pool.GetUID()) != spare.UID || pool.GetDeletionTimestamp() != nil {
		return d.abandonSpare(ctx, cm, record, "the spare was replaced or is deleting")
	}
	switch claim := pool.GetAnnotations()[spareClaimAnnotation]; {
	case claim == marker:
	case claim != "":
		return d.abandonSpare(ctx, cm, record, "another allocation claimed the spare")
	default:
		if !unclaimedSpare(pool, spare.ProfileUID) || pool.GetAnnotations()[spareSourceAnnotation] != string(record.Placement.UID) {
			return d.abandonSpare(ctx, cm, record, "the spare changed")
		}
		if actual, ok := poolSpecDigest(pool); !ok || actual != record.RuntimePool.Digest {
			return d.abandonSpare(ctx, cm, record, "the spare specification changed")
		}
		workers, err := d.namespaceWorkers(ctx, record.Placement.Namespace)
		if err != nil {
			return d.abandonSpare(ctx, cm, record, "its worker could not be listed")
		}
		eligible, err := d.spareEligible(ctx, record, pool, workers[pool.GetName()], spare.ProfileUID)
		if err != nil || !eligible {
			return d.abandonSpare(ctx, cm, record, "its worker is not active and empty")
		}
		if !spare.ClaimIssued {
			spare.ClaimIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		claimed := pool.DeepCopy()
		claimed.SetLabels(maps.Clone(record.RuntimePoolLabels))
		claimed.SetOwnerReferences([]metav1.OwnerReference{anchorOwner(record)})
		annotations := maps.Clone(claimed.GetAnnotations())
		annotations[spareClaimAnnotation] = marker
		claimed.SetAnnotations(annotations)
		// The read resourceVersion is the precondition: two claims, or a
		// claim and the replenisher's delete, can never both apply.
		if err := d.client.Update(ctx, claimed); err == nil {
			pool = claimed
		} else {
			current := poolObject()
			if getErr := d.client.Get(ctx, key, current); getErr != nil {
				if apierrors.IsNotFound(getErr) {
					return d.abandonSpare(ctx, cm, record, "the spare disappeared")
				}
				return getErr
			}
			if string(current.GetUID()) != spare.UID || current.GetAnnotations()[spareClaimAnnotation] != marker {
				return d.abandonSpare(ctx, cm, record, "the claim did not apply")
			}
			pool = current
		}
	}
	if pool.GetDeletionTimestamp() != nil || !runtimePoolMatches(pool, record) {
		return sdk.ErrStaleIdentity
	}
	record.RuntimePool.UID = string(pool.GetUID())
	stamp(&record.Timing.WorkerPoolCreatedAt)
	return d.save(ctx, cm, record)
}

// abandonSpare rebinds the record to the fresh pool it would have created
// without spares and continues on that path. It is only possible before the
// pool, template, or Actor exists. A claim write that still applies later
// leaves an orphan the replenisher collects; this record never adopts it.
func (d *Lifecycle) abandonSpare(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, reason string) error {
	if record.RuntimePool.UID != "" || record.RuntimePool.CreateIssued || record.Template.CreateIssued || record.Actor.CreateIssued {
		return fmt.Errorf("a spare claim cannot be abandoned after native creation")
	}
	operator := operatorPoolLabels(record)
	spec, err := withBirthLabels(record.RuntimePoolSpec, map[string]string{workerAllocationLabel: record.Observation.Identity.AllocationID, workerInstanceLabel: record.Observation.Identity.InstanceID})
	if err != nil {
		return err
	}
	record.SpareWorker = nil
	record.RuntimePoolSpec = spec
	if err := bindPrivatePool(record, record.Actor.Name+"-workers", operator); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("Substrate spare worker not claimed; creating a fresh private worker", "workspaceNamespace", record.Request.Key.Namespace, "workspaceName", record.Request.Key.Name, "sequence", record.Request.Sequence, "reason", reason)
	if err := d.save(ctx, cm, record); err != nil {
		return err
	}
	return d.ensureRuntimePool(ctx, cm, record)
}

// bindRetiringSpare resolves an issued but unbound claim during retirement.
// Only this record's own marker binds the pool for deletion; otherwise the
// record releases the spare and there is nothing of its own to delete.
func (d *Lifecycle) bindRetiringSpare(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (bool, error) {
	spare := record.SpareWorker
	pool := poolObject()
	err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if err == nil && string(pool.GetUID()) == spare.UID && pool.GetAnnotations()[spareClaimAnnotation] == claimMarker(cm, record) {
		if !runtimePoolMatches(pool, record) {
			return false, sdk.ErrStaleIdentity
		}
		record.RuntimePool.UID = string(pool.GetUID())
		return true, d.save(ctx, cm, record)
	}
	spare.Released = true
	return false, d.save(ctx, cm, record)
}

// spareWorkerLabelsPending reports a claimed spare's Worker that still has
// its unclaimed labels: Agent Substrate copies WorkerPool labels onto the
// Worker asynchronously after the claim.
func spareWorkerLabelsPending(record *journalRecord, worker *pb.Worker) bool {
	spare := record.SpareWorker
	return spare != nil && maps.Equal(worker.GetLabels(), spareLabels(spare.ProfileUID, spare.Birth))
}

// SpareWorkerReplenisher keeps each profile's unclaimed spares at its
// annotation's count, deletes stale spares, and collects orphaned claims.
// Unclaimed spares host nothing, so deleting them is safe; claimed pools are
// deleted only when their journal disowns them and no Actor is assigned.
type SpareWorkerReplenisher struct {
	client.Client
	Control pb.ControlClient
	Config  Config

	// SweepDeadline bounds one sweep's native work; zero selects
	// defaultSweepDeadline. Shared worker bounds run in their own loop and
	// are never behind it.
	SweepDeadline time.Duration

	mu         sync.Mutex
	lastOrphan time.Time
	invalid    map[types.UID]string
}

// defaultSweepDeadline bounds a sweep whose native calls stall.
const defaultSweepDeadline = time.Minute

// sharedBoundsPeriod is how often shared worker lifetime bounds are enforced.
const sharedBoundsPeriod = 10 * time.Second

// SharedWorkerBounds runs the Kubernetes-only shared worker bounds loop on
// the elected leader. It needs no native control, so it is registered even
// when native configuration is missing or invalid, and existing shared pools
// keep their lifetime and persisted-retirement enforcement.
type SharedWorkerBounds struct {
	client.Client
	Config Config
}

// SetupWithManager registers the bounds loop as a leader-elected runnable.
func (b *SharedWorkerBounds) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		(&SpareWorkerReplenisher{Client: b.Client, Config: b.Config}).RunSharedBounds(ctx, sharedBoundsPeriod)
		return nil
	})); err != nil {
		return err
	}
	// Escalation runs in its own loop, so slow pool deletes in a bounds pass
	// can never push a worker Pod's escalation past its deadline.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		(&SpareWorkerReplenisher{Client: b.Client, Config: b.Config}).RunSharedEscalation(ctx, sharedEscalationPeriod)
		return nil
	})); err != nil {
		return err
	}
	// Labelling shared journals written before the shared label runs in its
	// own loop too, off every enforcement path.
	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		(&SpareWorkerReplenisher{Client: b.Client, Config: b.Config}).RunSharedLabelBackfill(ctx, sharedLabelBackfillPeriod)
		return nil
	}))
}

// SetupWithManager runs the replenisher on the elected leader. The shared
// worker bounds loop is registered separately (SharedWorkerBounds).
func (r *SpareWorkerReplenisher) SetupWithManager(mgr ctrl.Manager) error {
	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		ticker := time.NewTicker(spareResyncPeriod)
		defer ticker.Stop()
		for {
			if err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
				ctrl.LoggerFrom(ctx).Error(err, "Substrate spare worker sweep failed")
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}))
}

type spareShape struct {
	source      *unstructured.Unstructured
	cpu, memory int64
	digest      string
}

// RunSharedBounds enforces shared worker lifetime bounds every period, using
// Kubernetes alone, independently of the native sweep, until ctx ends.
func (r *SpareWorkerReplenisher) RunSharedBounds(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		if err := r.EnforceSharedBounds(ctx); err != nil && ctx.Err() == nil {
			ctrl.LoggerFrom(ctx).Error(err, "Substrate shared worker bounds enforcement failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// EnforceSharedBounds runs one Kubernetes-only shared worker bounds pass.
func (r *SpareWorkerReplenisher) EnforceSharedBounds(ctx context.Context) error {
	return r.enforceSharedBounds(ctx, New(r.Client, nil, r.Config))
}

// Sweep runs one replenishment and garbage-collection pass. Its native work
// is bounded by SweepDeadline.
func (r *SpareWorkerReplenisher) Sweep(ctx context.Context) error {
	d := New(r.Client, r.Control, r.Config)
	// Shared worker lifetime bounds come first and need only Kubernetes.
	boundsErr := r.enforceSharedBounds(ctx, New(r.Client, nil, r.Config))
	deadline := r.SweepDeadline
	if deadline <= 0 {
		deadline = defaultSweepDeadline
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	profiles := &profile.SubstrateWorkspaceProfileList{}
	if err := r.List(ctx, profiles); err != nil {
		return errors.Join(boundsErr, err)
	}
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion(workerPoolAPIVersion())
	pools.SetKind("WorkerPoolList")
	if err := r.List(ctx, pools, client.HasLabels{spareProfileLabel}); err != nil {
		return err
	}
	byProfile := map[string][]*unstructured.Unstructured{}
	for i := range pools.Items {
		pool := &pools.Items[i]
		byProfile[pool.GetLabels()[spareProfileLabel]] = append(byProfile[pool.GetLabels()[spareProfileLabel]], pool)
	}
	workers := &spareWorkerCache{d: d, byNamespace: map[string]map[string][]*pb.Worker{}}
	var errs []error
	for i := range profiles.Items {
		item := &profiles.Items[i]
		want := r.wanted(ctx, item)
		errs = append(errs, r.replenish(ctx, d, workers, item, want, byProfile[string(item.UID)]))
		delete(byProfile, string(item.UID))
	}
	// Spares of a deleted profile.
	for uid, spares := range byProfile {
		for _, pool := range spares {
			if unclaimedSpare(pool, uid) {
				errs = append(errs, deleteIdlePool(ctx, d, workers, pool))
			}
		}
	}
	errs = append(errs, boundsErr, r.sweepShared(ctx, d, profiles.Items, workers))
	r.mu.Lock()
	due := time.Since(r.lastOrphan) >= spareOrphanPeriod
	if due {
		r.lastOrphan = time.Now()
	}
	r.mu.Unlock()
	if due {
		errs = append(errs, collectOrphanClaims(ctx, d, workers))
	}
	return errors.Join(errs...)
}

// wanted is the profile's valid annotation count; an invalid value is logged
// once per value and treated as 0.
func (r *SpareWorkerReplenisher) wanted(ctx context.Context, item *profile.SubstrateWorkspaceProfile) int {
	want, err := spareWorkerCount(item.Annotations)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.invalid == nil {
		r.invalid = map[types.UID]string{}
	}
	if err != nil {
		if raw := item.Annotations[spareWorkersAnnotation]; r.invalid[item.UID] != raw {
			r.invalid[item.UID] = raw
			ctrl.LoggerFrom(ctx).Error(err, "Substrate spare workers disabled for profile with an invalid annotation", "profileNamespace", item.Namespace, "profileName", item.Name)
		}
		return 0
	}
	delete(r.invalid, item.UID)
	if item.DeletionTimestamp != nil || item.Validate() != nil {
		return 0
	}
	if item.Spec.Placement != nil && item.Spec.Placement.SharedWorkers != nil {
		// Free slots in shared pools are this profile's warm capacity.
		return 0
	}
	return want
}

func (r *SpareWorkerReplenisher) replenish(ctx context.Context, d *Lifecycle, workers *spareWorkerCache, item *profile.SubstrateWorkspaceProfile, want int, spares []*unstructured.Unstructured) error {
	var shape *spareShape
	if want > 0 {
		var err error
		if shape, err = d.profileSpareShape(ctx, item); err != nil {
			// The current shape is unknown, for example while the operator
			// pool's SandboxConfig resolves: create nothing and keep the
			// existing spares, which allocations verify themselves.
			ctrl.LoggerFrom(ctx).Info("Substrate spare workers paused for profile", "profileNamespace", item.Namespace, "profileName", item.Name, "reason", err.Error())
			return nil
		}
	}
	var current []*unstructured.Unstructured
	var errs []error
	for _, pool := range spares {
		if !unclaimedSpare(pool, string(item.UID)) {
			continue
		}
		fresh := shape != nil && pool.GetNamespace() == shape.source.GetNamespace() && pool.GetAnnotations()[spareSourceAnnotation] == string(shape.source.GetUID()) && pool.GetAnnotations()[spareShapeAnnotation] == shape.digest
		if fresh {
			current = append(current, pool)
			continue
		}
		errs = append(errs, deleteIdlePool(ctx, d, workers, pool))
	}
	slices.SortFunc(current, func(a, b *unstructured.Unstructured) int { return strings.Compare(a.GetName(), b.GetName()) })
	for len(current) > want {
		errs = append(errs, deleteIdlePool(ctx, d, workers, current[len(current)-1]))
		current = current[:len(current)-1]
	}
	created := 0
	for range want - len(current) {
		if err := d.createSpare(ctx, item, shape); err != nil {
			errs = append(errs, err)
			break
		}
		created++
	}
	if created > 0 {
		ctrl.LoggerFrom(ctx).Info("Substrate spare workers created", "profileNamespace", item.Namespace, "profileName", item.Name, "created", created, "wanted", want)
	}
	return errors.Join(errs...)
}

// profileSpareShape derives the private pool an allocation of this profile
// would create, exactly as allocation does: the operator pool selected by the
// infrastructure template, and an Actor sized by the template's limits, since
// Orka core sends native requests without container resources.
func (d *Lifecycle) profileSpareShape(ctx context.Context, item *profile.SubstrateWorkspaceProfile) (*spareShape, error) {
	ref, err := item.Spec.TemplateRef.Resolve(item.Namespace)
	if err != nil {
		return nil, err
	}
	base, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: ref.Namespace, Name: ref.Name}})
	if err != nil {
		return nil, err
	}
	source, err := d.operatorPool(ctx, base)
	if err != nil {
		return nil, err
	}
	if err := checkSourceSelector(base.GetWorkerSelector().GetMatchLabels(), source); err != nil {
		return nil, err
	}
	resources, err := compileResources(base.GetResources(), corev1.ResourceRequirements{})
	if err != nil {
		return nil, err
	}
	shape := &spareShape{source: source, cpu: resources.GetLimits().GetCpuMillicores(), memory: resources.GetLimits().GetMemoryBytes()}
	spec, err := buildPrivatePoolSpec(source, shape.cpu, shape.memory, spareBirth(spareNamePrefix+"shape"))
	if err != nil {
		return nil, err
	}
	if shape.digest, err = runtimePoolRestoreDigest(spec, false); err != nil {
		return nil, err
	}
	return shape, nil
}

func (d *Lifecycle) createSpare(ctx context.Context, item *profile.SubstrateWorkspaceProfile, shape *spareShape) error {
	name, err := randomName(spareNamePrefix)
	if err != nil {
		return err
	}
	spec, err := buildPrivatePoolSpec(shape.source, shape.cpu, shape.memory, spareBirth(name))
	if err != nil {
		return err
	}
	var object map[string]any
	if err := json.Unmarshal(spec, &object); err != nil {
		return err
	}
	pool := poolObject()
	pool.SetNamespace(shape.source.GetNamespace())
	pool.SetName(name)
	pool.SetLabels(spareLabels(string(item.UID), name))
	pool.SetAnnotations(map[string]string{spareSourceAnnotation: string(shape.source.GetUID()), spareShapeAnnotation: shape.digest, spareProfileAnnotation: item.Namespace + "/" + item.Name})
	pool.Object["spec"] = object
	return d.client.Create(ctx, pool)
}

// spareWorkerCache lists native Workers at most once per namespace per sweep.
type spareWorkerCache struct {
	d           *Lifecycle
	byNamespace map[string]map[string][]*pb.Worker
}

func (c *spareWorkerCache) pool(ctx context.Context, pool *unstructured.Unstructured) ([]*pb.Worker, error) {
	workers, found := c.byNamespace[pool.GetNamespace()]
	if !found {
		var err error
		if workers, err = c.d.namespaceWorkers(ctx, pool.GetNamespace()); err != nil {
			return nil, err
		}
		c.byNamespace[pool.GetNamespace()] = workers
	}
	return workers[pool.GetName()], nil
}

// deleteIdlePool deletes a pool only after proving no native Actor is
// assigned to any of its workers. The UID and resourceVersion preconditions
// make a concurrent claim win: a claimed spare is never deleted here.
func deleteIdlePool(ctx context.Context, d *Lifecycle, workers *spareWorkerCache, pool *unstructured.Unstructured) error {
	if pool.GetDeletionTimestamp() != nil {
		return nil
	}
	poolWorkers, err := workers.pool(ctx, pool)
	if err != nil {
		return err
	}
	for _, worker := range poolWorkers {
		hosts, err := d.workerHostsActor(ctx, worker.GetMetadata().GetName())
		if err != nil {
			return err
		}
		if hosts {
			return fmt.Errorf("an unclaimed or orphaned private WorkerPool hosts a native Actor; it is left in place")
		}
	}
	uid, version := pool.GetUID(), pool.GetResourceVersion()
	err = d.client.Delete(ctx, pool, client.Preconditions{UID: &uid, ResourceVersion: &version}, client.PropagationPolicy(metav1.DeletePropagationForeground))
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func (d *Lifecycle) workerHostsActor(ctx context.Context, worker string) (bool, error) {
	page := ""
	seen := map[string]bool{}
	for {
		assignments, err := d.control.ListWorkerActorAssignments(ctx, &pb.ListWorkerActorAssignmentsRequest{Worker: &pb.ObjectRef{Name: worker}, Options: &pb.ListOptions{PageSize: 1000, PageToken: page}})
		if err != nil {
			return false, err
		}
		if len(assignments.GetActorAssignments()) != 0 {
			return true, nil
		}
		page = assignments.GetNextPageToken()
		if page == "" {
			return false, nil
		}
		if seen[page] || len(seen) >= 100 {
			return false, fmt.Errorf("native assignment pagination repeated a token or exceeded its bound")
		}
		seen[page] = true
	}
}

// collectOrphanClaims deletes claimed pools whose journal positively disowns
// them: the exact journal's record for the claim's sequence does not hold the
// claim, or released it. A missing or replaced journal is not proof; such a
// pool stays for its anchor ownerReference to collect.
func collectOrphanClaims(ctx context.Context, d *Lifecycle, workers *spareWorkerCache) error {
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion(workerPoolAPIVersion())
	pools.SetKind("WorkerPoolList")
	if err := d.client.List(ctx, pools, client.HasLabels{ownershipLabel, workerAllocationLabel}); err != nil {
		return err
	}
	var errs []error
	for i := range pools.Items {
		pool := &pools.Items[i]
		marker := pool.GetAnnotations()[spareClaimAnnotation]
		if marker == "" || pool.GetDeletionTimestamp() != nil {
			continue
		}
		disowned, err := d.claimDisowned(ctx, pool, marker)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if disowned {
			ctrl.LoggerFrom(ctx).Info("Substrate orphaned spare claim collected", "workspaceNamespace", strings.SplitN(marker, "/", 2)[0])
			errs = append(errs, deleteIdlePool(ctx, d, workers, pool))
		}
	}
	return errors.Join(errs...)
}

func (d *Lifecycle) claimDisowned(ctx context.Context, pool *unstructured.Unstructured, marker string) (bool, error) {
	parts := strings.Split(marker, "/")
	if len(parts) != 4 {
		return false, nil
	}
	sequence, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || sequence < 1 {
		return false, nil
	}
	journal := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: parts[0], Name: parts[1]}, journal); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if string(journal.UID) != parts[2] {
		return false, nil
	}
	record, ok := decodeClaimRecord(journal)
	if !ok || record.Request.Sequence < sequence {
		return false, nil
	}
	if record.Request.Sequence > sequence {
		archived := &corev1.ConfigMap{}
		if err := d.client.Get(ctx, types.NamespacedName{Namespace: parts[0], Name: historyName(parts[1], sequence)}, archived); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if record, ok = decodeClaimRecord(archived); !ok || record.Request.Sequence != sequence {
			return false, nil
		}
	}
	spare := record.SpareWorker
	held := spare != nil && !spare.Released && spare.Name == pool.GetName() && spare.UID == string(pool.GetUID()) && record.Placement.Namespace == pool.GetNamespace()
	return !held, nil
}

// decodeClaimRecord reads only what claim ownership needs; it never
// validates or adopts the journal.
func decodeClaimRecord(cm *corev1.ConfigMap) (*journalRecord, bool) {
	raw := cm.Data[journalDataKey]
	if len(raw) == 0 || len(raw) > MaxJournalBytes {
		return nil, false
	}
	record := &journalRecord{}
	if err := json.Unmarshal([]byte(raw), record); err != nil || record.Version != journalVersion {
		return nil, false
	}
	return record, true
}
