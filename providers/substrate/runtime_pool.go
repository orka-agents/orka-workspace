package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"strings"

	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func poolObject() *unstructured.Unstructured {
	pool := &unstructured.Unstructured{}
	pool.SetAPIVersion(workerPoolAPIVersion())
	pool.SetKind("WorkerPool")
	return pool
}

// The source pool is operator infrastructure. Each request gets a separate
// single-replica gVisor pool whose workers are born with unique labels and
// sized so exactly one template-sized Actor fits. v1 workers report a large
// Actor count, so capacity, not count, is what keeps the worker private. With
// spares > 0 the request first selects an existing spare built by the same
// copy logic (spare_workers.go); the claim itself happens later, after the
// journal records it.
//
// A profile that opts in to shared workers (ADR 0008) instead binds the record
// to a shared pool of its scope; spares do not apply to it.
func (d *Lifecycle) prepareRuntimePool(ctx context.Context, record *journalRecord, spares int, profileUID types.UID, shared *profile.SharedWorkersPolicy) error {
	source := poolObject()
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.Placement.Name}, source); err != nil {
		return err
	}
	if source.GetUID() != record.Placement.UID || source.GetDeletionTimestamp() != nil {
		return sdk.ErrStaleIdentity
	}
	record.SourceSelector = maps.Clone(record.TemplateSpec.GetWorkerSelector().GetMatchLabels())
	if err := checkSourceSelector(record.SourceSelector, source); err != nil {
		return err
	}
	cpu, memory := expectedWorkerCapacity(record)
	spec, err := buildPrivatePoolSpec(source, cpu, memory, workerLabels(record))
	if err != nil {
		return err
	}
	record.RuntimePoolSpec = spec
	if err := bindPrivatePool(record, record.Actor.Name+"-workers", source.GetLabels()); err != nil {
		return err
	}
	if shared != nil {
		return d.prepareSharedPlacement(record, source, shared, profileUID)
	}
	if spares > 0 {
		return d.selectSpare(ctx, record, profileUID)
	}
	return nil
}

// checkSourceSelector refuses an operator selector or pool that already uses
// a reserved confinement label.
func checkSourceSelector(selector map[string]string, source *unstructured.Unstructured) error {
	for _, key := range []string{workerAllocationLabel, workerInstanceLabel, spareProfileLabel, sharedPoolLabel} {
		if _, found := selector[key]; found {
			return fmt.Errorf("operator native selector uses a reserved confinement label")
		}
		if _, found := source.GetLabels()[key]; found {
			return fmt.Errorf("operator native pool uses a reserved confinement label")
		}
	}
	return nil
}

// buildPrivatePoolSpec copies the operator pool's worker image, sandbox class with
// its pinned SandboxConfig, and scheduling into a single-replica pool sized
// for exactly one (cpu, memory) Actor, whose workers carry birth labels.
// Per-instance pools and spares are both built here, so a spare differs from
// the pool an allocation would create only in its birth label values.
func buildPrivatePoolSpec(source *unstructured.Unstructured, cpu, memory int64, birth map[string]string) (json.RawMessage, error) {
	spec, found, err := unstructured.NestedMap(source.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("operator WorkerPool has no usable immutable specification")
	}
	if image, _, _ := unstructured.NestedString(spec, "workerImage"); image == "" {
		return nil, fmt.Errorf("operator WorkerPool has no worker image")
	}
	spec["replicas"] = int64(1)
	if _, legacy := spec["sandboxClass"]; legacy {
		return nil, fmt.Errorf("operator WorkerPool uses the removed v1alpha1 sandboxClass field")
	}
	classes, found, err := unstructured.NestedSlice(spec, "sandboxClasses")
	if err != nil {
		return nil, fmt.Errorf("operator WorkerPool sandbox classes are invalid")
	}
	if !found || len(classes) == 0 {
		classes = []any{map[string]any{"name": gvisorSandboxClass}}
	}
	if len(classes) != 1 {
		return nil, fmt.Errorf("operator WorkerPool must use exactly one sandbox class, gVisor")
	}
	class, ok := classes[0].(map[string]any)
	if !ok || class["name"] != gvisorSandboxClass {
		return nil, fmt.Errorf("operator WorkerPool must use gVisor")
	}
	for key := range class {
		if key != "name" && key != "configRef" {
			return nil, fmt.Errorf("operator WorkerPool sandbox class has an unsupported field")
		}
	}
	configRef, err := pinnedSandboxConfig(source, class)
	if err != nil {
		return nil, err
	}
	classes = []any{map[string]any{"name": gvisorSandboxClass, "configRef": map[string]any{"name": configRef}}}
	spec["sandboxClasses"] = classes
	template, _, err := unstructured.NestedMap(spec, "template")
	if err != nil {
		return nil, err
	}
	if template == nil {
		template = map[string]any{}
	}
	if _, set := template["resources"]; set {
		return nil, fmt.Errorf("operator WorkerPool template resources are provider-owned; the private worker is sized for exactly one Actor")
	}
	// The worker reports its Pod limits, less the gVisor reservation, as
	// capacity. Pin both so the template-sized Actor exactly fills it.
	if cpu <= 0 || memory <= 0 {
		return nil, fmt.Errorf("native template has no Actor size for its private worker")
	}
	quantities := map[string]any{
		"cpu":    resource.NewMilliQuantity(cpu, resource.DecimalSI).String(),
		"memory": resource.NewQuantity(memory+gvisorReservedMemoryBytes, resource.BinarySI).String(),
	}
	template["resources"] = map[string]any{"limits": quantities, "requests": maps.Clone(quantities)}
	podLabels, _, err := unstructured.NestedStringMap(template, "labels")
	if err != nil {
		return nil, err
	}
	if podLabels == nil {
		podLabels = map[string]string{}
	}
	for key, value := range birth {
		if foreign := podLabels[key]; foreign != "" && foreign != value {
			return nil, fmt.Errorf("operator worker template uses a reserved confinement label")
		}
		podLabels[key] = value
	}
	if err := unstructured.SetNestedStringMap(template, podLabels, "labels"); err != nil {
		return nil, err
	}
	spec["template"] = template
	return json.Marshal(spec)
}

// bindPrivatePool names the record's private pool, its exact metadata labels
// (the operator pool's own, ownership, and worker birth labels), and the
// derived template's worker selector, from record.RuntimePoolSpec.
func bindPrivatePool(record *journalRecord, name string, operatorLabels map[string]string) error {
	record.RuntimePool = nativeReference{Name: name, Digest: digest(record.RuntimePoolSpec)}
	record.RuntimePoolLabels = maps.Clone(operatorLabels)
	if record.RuntimePoolLabels == nil {
		record.RuntimePoolLabels = map[string]string{}
	}
	maps.Copy(record.RuntimePoolLabels, labels(record))
	maps.Copy(record.RuntimePoolLabels, workerLabels(record))
	selector := maps.Clone(record.SourceSelector)
	maps.Copy(selector, workerLabels(record))
	record.TemplateSpec.WorkerSelector = &pb.Selector{MatchLabels: selector}
	var err error
	record.Template.Digest, err = templateDigest(record.TemplateSpec)
	return err
}

// pinnedSandboxConfig names the SandboxConfig the operator pool actually runs.
// An omitted configRef follows the class default, which an upgrade may move,
// so the private pool pins the operator pool's resolved config by name. A
// config that is switching or unresolved fails closed: while the class's
// Resolved condition is not True for the current generation, Agent Substrate
// keeps the last resolved configRef, which may be a removed default or a
// deleting config.
func pinnedSandboxConfig(source *unstructured.Unstructured, class map[string]any) (string, error) {
	declared := ""
	if raw, set := class["configRef"]; set {
		ref, ok := raw.(map[string]any)
		name, _ := ref["name"].(string)
		if !ok || len(ref) != 1 || strings.TrimSpace(name) == "" || name != strings.TrimSpace(name) {
			return "", fmt.Errorf("operator WorkerPool sandbox configRef must name exactly one SandboxConfig")
		}
		declared = name
	}
	statuses, _, err := unstructured.NestedSlice(source.Object, "status", "sandboxClasses")
	if err != nil {
		return "", fmt.Errorf("operator WorkerPool sandbox class status is invalid")
	}
	resolved := ""
	for _, raw := range statuses {
		status, ok := raw.(map[string]any)
		if !ok || status["name"] != gvisorSandboxClass {
			continue
		}
		if pending, set := status["pendingConfigRef"]; set && pending != nil {
			return "", fmt.Errorf("operator WorkerPool sandbox config is switching; retry after it resolves")
		}
		if !sandboxClassResolved(status, source.GetGeneration()) {
			return "", fmt.Errorf("operator WorkerPool has not resolved a SandboxConfig for gVisor")
		}
		resolved, _, _ = unstructured.NestedString(status, "configRef", "name")
	}
	switch {
	case resolved == "":
		return "", fmt.Errorf("operator WorkerPool has not resolved a SandboxConfig for gVisor")
	case declared != "" && declared != resolved:
		return "", fmt.Errorf("operator WorkerPool sandbox config is switching; retry after it resolves")
	default:
		return resolved, nil
	}
}

// sandboxClassResolved reports a True Resolved condition observed at the
// operator pool's current generation.
func sandboxClassResolved(status map[string]any, generation int64) bool {
	conditions, _, err := unstructured.NestedSlice(status, "conditions")
	if err != nil {
		return false
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok || condition["type"] != "Resolved" {
			continue
		}
		observed, _, err := unstructured.NestedInt64(condition, "observedGeneration")
		return err == nil && condition["status"] == string(metav1.ConditionTrue) && observed == generation
	}
	return false
}

func runtimePoolMatches(pool *unstructured.Unstructured, record *journalRecord) bool {
	spec, found, err := unstructured.NestedMap(pool.Object, "spec")
	if err != nil || !found {
		return false
	}
	data, err := json.Marshal(spec)
	return err == nil && digest(data) == record.RuntimePool.Digest &&
		reflect.DeepEqual(pool.GetLabels(), record.RuntimePoolLabels) &&
		reflect.DeepEqual(pool.GetOwnerReferences(), []metav1.OwnerReference{anchorOwner(record)})
}

// Instance labels rotate on cold boot. Every inherited worker setting remains
// frozen, including image, native isolation, scheduling, resources and labels
// supplied by the operator. Missing provenance fails closed.
func compatibleRuntimePool(previous, next json.RawMessage) bool {
	previousDigest, previousErr := runtimePoolRestoreDigest(previous, false)
	nextDigest, nextErr := runtimePoolRestoreDigest(next, false)
	return previousErr == nil && nextErr == nil && previousDigest == nextDigest
}

// workerImageTransition is journal provenance for a data-only resume or
// restore that followed the operator pool to a new worker image.
type workerImageTransition struct {
	From string `json:"from"`
	To   string `json:"to"`
	// PreviousRuntimePoolDigest identifies the exact private pool
	// specification the retained data was written under.
	PreviousRuntimePoolDigest string      `json:"previousRuntimePoolDigest"`
	ObservedAt                metav1.Time `json:"observedAt"`
}

// maxWorkerImageTransitions bounds inherited provenance on a long lineage.
const maxWorkerImageTransitions = 16

var digestPinnedImage = regexp.MustCompile(`^[^@\s]+@sha256:[0-9a-f]{64}$`)

// dataResumeRuntimePool decides whether retained VOLUMES data may cold boot on
// the operator pool's current specification. Agent Substrate upgrades roll
// WorkerPools to a new worker image, and an API server must never be newer
// than its workers, so a data-only resume follows the operator pool's
// current image. Nothing else may change: replicas, sandbox class and pinned
// SandboxConfig, scheduling, resources, and operator labels stay exactly
// equal, and the new image must be pinned by digest. A memory restore is never
// admitted on this backend, so no process state crosses the image change. The
// returned transition is nil when the image is unchanged.
func dataResumeRuntimePool(previous, next json.RawMessage, now metav1.Time) (*workerImageTransition, error) {
	if compatibleRuntimePool(previous, next) {
		return nil, nil
	}
	previousDigest, previousErr := runtimePoolRestoreDigest(previous, true)
	nextDigest, nextErr := runtimePoolRestoreDigest(next, true)
	if previousErr != nil || nextErr != nil || previousDigest != nextDigest {
		return nil, fmt.Errorf("native resume changed immutable worker infrastructure")
	}
	var before, after map[string]any
	if json.Unmarshal(previous, &before) != nil || json.Unmarshal(next, &after) != nil {
		return nil, fmt.Errorf("private worker infrastructure provenance is invalid")
	}
	from, _, _ := unstructured.NestedString(before, "workerImage")
	to, _, _ := unstructured.NestedString(after, "workerImage")
	if !digestPinnedImage.MatchString(to) {
		return nil, fmt.Errorf("native resume onto a new worker image requires the operator pool's current image pinned by digest")
	}
	return &workerImageTransition{From: from, To: to, PreviousRuntimePoolDigest: digest(previous), ObservedAt: now}, nil
}

// inheritWorkerImageProvenance carries a lineage's image transitions forward
// and appends a new one, bounded.
func inheritWorkerImageProvenance(previous []workerImageTransition, transition *workerImageTransition) ([]workerImageTransition, error) {
	out := append([]workerImageTransition(nil), previous...)
	if transition != nil {
		out = append(out, *transition)
	}
	if len(out) > maxWorkerImageTransitions {
		return nil, fmt.Errorf("native worker image provenance exceeded its bound")
	}
	return out, nil
}

// operatorPoolLabels returns the operator's own WorkerPool labels, without
// the provider's per-allocation ownership and confinement labels.
//
// A shared pool carries only provider-owned labels, so a shared record keeps
// the operator pool's labels as provenance for resume and checkpoints.
func operatorPoolLabels(record *journalRecord) map[string]string {
	if record.SharedWorker != nil {
		return maps.Clone(record.SharedWorker.OperatorLabels)
	}
	out := maps.Clone(record.RuntimePoolLabels)
	for key := range labels(record) {
		delete(out, key)
	}
	for key := range workerLabels(record) {
		delete(out, key)
	}
	return out
}

func runtimePoolRestoreDigest(data json.RawMessage, ignoreImage bool) (string, error) {
	var spec map[string]any
	if len(data) == 0 || json.Unmarshal(data, &spec) != nil {
		return "", fmt.Errorf("private worker infrastructure provenance is missing")
	}
	image, _, err := unstructured.NestedString(spec, "workerImage")
	classes, _, classErr := unstructured.NestedSlice(spec, "sandboxClasses")
	if err != nil || classErr != nil || image == "" || spec["replicas"] != float64(1) || len(classes) != 1 {
		return "", fmt.Errorf("private worker infrastructure provenance is invalid")
	}
	if class, ok := classes[0].(map[string]any); !ok || class["name"] != gvisorSandboxClass {
		return "", fmt.Errorf("private worker infrastructure must use gVisor")
	}
	if ignoreImage {
		delete(spec, "workerImage")
	}
	if limits, _, err := unstructured.NestedStringMap(spec, "template", "resources", "limits"); err != nil || limits["cpu"] == "" || limits["memory"] == "" {
		return "", fmt.Errorf("private worker infrastructure has no single-Actor size")
	}
	labels, found, err := unstructured.NestedStringMap(spec, "template", "labels")
	if err != nil || !found || labels[workerAllocationLabel] == "" || labels[workerInstanceLabel] == "" {
		return "", fmt.Errorf("private worker infrastructure has no birth labels")
	}
	delete(labels, workerAllocationLabel)
	delete(labels, workerInstanceLabel)
	if err := unstructured.SetNestedStringMap(spec, labels, "template", "labels"); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	return digest(canonical), nil
}

func (d *Lifecycle) ensureRuntimePool(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if record.RuntimePool.Name == "" || len(record.RuntimePoolSpec) == 0 || len(record.SourceSelector) == 0 || record.RuntimePoolDeleting {
		return sdk.ErrStaleIdentity
	}
	if record.SharedWorker != nil {
		return d.ensureSharedRuntimePool(ctx, cm, record)
	}
	if record.SpareWorker != nil {
		if record.SpareWorker.Released {
			return sdk.ErrStaleIdentity
		}
		return d.ensureSpareRuntimePool(ctx, cm, record)
	}
	pool := poolObject()
	key := client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}
	err := d.client.Get(ctx, key, pool)
	if apierrors.IsNotFound(err) {
		if record.RuntimePool.UID != "" {
			return sdk.ErrStaleIdentity
		}
		if !record.RuntimePool.CreateIssued {
			record.RuntimePool.CreateIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		pool.SetNamespace(key.Namespace)
		pool.SetName(key.Name)
		pool.SetLabels(record.RuntimePoolLabels)
		pool.SetOwnerReferences([]metav1.OwnerReference{anchorOwner(record)})
		var spec map[string]any
		if err := json.Unmarshal(record.RuntimePoolSpec, &spec); err != nil {
			return err
		}
		pool.Object["spec"] = spec
		if err := d.client.Create(ctx, pool); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if pool.GetUID() == "" || pool.GetDeletionTimestamp() != nil || record.RuntimePool.UID != "" && string(pool.GetUID()) != record.RuntimePool.UID || !runtimePoolMatches(pool, record) {
		return sdk.ErrStaleIdentity
	}
	if record.RuntimePool.UID == "" {
		record.RuntimePool.UID = string(pool.GetUID())
		stamp(&record.Timing.WorkerPoolCreatedAt)
		return d.save(ctx, cm, record)
	}
	return nil
}

func (d *Lifecycle) verifyRuntimePool(ctx context.Context, record *journalRecord) error {
	if record.SharedWorker != nil {
		return d.verifySharedRuntimePoolReadOnly(ctx, record)
	}
	return d.verifyPrivateRuntimePool(ctx, record)
}

// verifyRuntimePoolWith is verifyRuntimePool against the journal this pass
// already read.
func (d *Lifecycle) verifyRuntimePoolWith(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if record.SharedWorker != nil {
		_, err := d.verifySharedRuntimePool(ctx, cm, record, true)
		return err
	}
	return d.verifyPrivateRuntimePool(ctx, record)
}

func (d *Lifecycle) verifyPrivateRuntimePool(ctx context.Context, record *journalRecord) error {
	pool := poolObject()
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool); err != nil {
		return err
	}
	if record.RuntimePool.UID == "" || string(pool.GetUID()) != record.RuntimePool.UID || pool.GetDeletionTimestamp() != nil || record.RuntimePoolDeleting || !runtimePoolMatches(pool, record) {
		return sdk.ErrStaleIdentity
	}
	return nil
}

// Labels and size exist before this worker is eligible to execute. Pin the
// exact ready, empty, single-Actor worker and its process epoch before issuing
// the one-time native Resume.
// privatePoolWorker returns the private pool's only Worker, nil when it has
// none, and an error when it has more than one.
func (d *Lifecycle) privatePoolWorker(ctx context.Context, record *journalRecord) (*pb.Worker, error) {
	workers, err := d.namespaceWorkers(ctx, record.Placement.Namespace)
	if err != nil {
		return nil, err
	}
	switch selected := workers[record.RuntimePool.Name]; len(selected) {
	case 0:
		return nil, nil
	case 1:
		return selected[0], nil
	default:
		return nil, fmt.Errorf("private native pool has multiple workers")
	}
}

func (d *Lifecycle) prepareWorker(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (bool, error) {
	// Registered before any read below, so an event on this pool while they
	// run wakes the workspace again.
	d.awaitWorker(record)
	if record.SharedWorker != nil {
		return d.prepareSharedWorker(ctx, cm, record)
	}
	if err := d.verifyInfrastructureReadOnly(ctx, record); err != nil {
		return false, err
	}
	selected, err := d.privatePoolWorker(ctx, record)
	if err != nil {
		return false, err
	}
	if selected == nil {
		return false, nil
	}
	allocated := selected.GetStatus().GetAllocated()
	if selected.GetStatus().GetState() == pb.WorkerState_WORKER_STATE_UNAVAILABLE || len(selected.GetStatus().GetSandboxClasses()) == 0 || selected.GetStatus().GetRegisteredEpoch() <= 0 {
		// Registration and readiness are still in progress.
		d.noteNativeLag(ctx, selected)
		return false, nil
	}
	if selected.GetMetadata().GetUid() == "" || selected.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_ACTIVE || allocated.GetActors() != 0 || allocated.GetCpuMillicores() != 0 || allocated.GetMemoryBytes() != 0 {
		return false, fmt.Errorf("private native worker must be active and empty")
	}
	if err := verifySingleActorWorker(record, selected); err != nil {
		return false, err
	}
	for key, value := range record.TemplateSpec.GetWorkerSelector().GetMatchLabels() {
		if selected.GetLabels()[key] != value {
			if spareWorkerLabelsPending(record, selected) {
				// The claimed WorkerPool already carries the labels; only
				// their native copy onto the Worker is outstanding.
				d.noteNativeLag(ctx, nil)
				return false, nil
			}
			return false, sdk.ErrStaleIdentity
		}
	}
	if err := d.verifyWorkerExclusive(ctx, record, selected.GetMetadata().GetName(), false); err != nil {
		return false, err
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: selected.WorkerNamespace, Name: selected.WorkerPod}, pod); err != nil {
		return false, err
	}
	fence := &workerFence{Name: selected.Metadata.Name, UID: selected.Metadata.Uid, Namespace: selected.WorkerNamespace, Pool: selected.WorkerPool, Pod: selected.WorkerPod, PodUID: selected.WorkerPodUid, AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.InstanceID, Epoch: selected.GetStatus().GetRegisteredEpoch()}
	if string(pod.UID) != fence.PodUID || pod.DeletionTimestamp != nil || pod.Labels[workerPoolLabel] != fence.Pool {
		return false, sdk.ErrStaleIdentity
	}
	for key, value := range workerLabels(record) {
		if pod.Labels[key] != value {
			return false, sdk.ErrStaleIdentity
		}
	}
	if record.Worker != nil && *record.Worker != *fence {
		return false, sdk.ErrStaleIdentity
	}
	if record.Worker == nil {
		record.Worker = fence
		stamp(&record.Timing.WorkerActiveAt)
		if err := d.save(ctx, cm, record); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (d *Lifecycle) deleteRuntimePool(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (bool, error) {
	switch spare := record.SpareWorker; {
	case spare == nil:
		if !record.RuntimePool.CreateIssued {
			return true, nil
		}
	case record.RuntimePool.UID == "":
		// An unbound claim: only this record's own marker makes the spare
		// its pool. Otherwise nothing of this record's exists to delete.
		if spare.Released || !spare.ClaimIssued {
			return true, nil
		}
		bound, err := d.bindRetiringSpare(ctx, cm, record)
		if err != nil || !bound {
			return err == nil, err
		}
	}
	pool := poolObject()
	err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool)
	if apierrors.IsNotFound(err) {
		if record.RuntimePool.UID == "" {
			return false, fmt.Errorf("private WorkerPool creation outcome is unresolved")
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if pool.GetUID() == "" || record.RuntimePool.UID != "" && string(pool.GetUID()) != record.RuntimePool.UID || !runtimePoolMatches(pool, record) {
		return false, sdk.ErrStaleIdentity
	}
	// A create response or UID save can be lost before cancellation switches
	// the journal out of ensure. Recover only the exact frozen pool, without
	// issuing another create, and persist its lifetime before deletion.
	if record.RuntimePool.UID == "" {
		record.RuntimePool.UID = string(pool.GetUID())
		if err := d.save(ctx, cm, record); err != nil {
			return false, err
		}
	}
	if !record.RuntimePoolDeleting {
		record.RuntimePoolDeleting = true
		if err := d.save(ctx, cm, record); err != nil {
			return false, err
		}
	}
	if pool.GetDeletionTimestamp() == nil {
		uid := pool.GetUID()
		if err := d.client.Delete(ctx, pool, client.Preconditions{UID: &uid}, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return false, nil
}
