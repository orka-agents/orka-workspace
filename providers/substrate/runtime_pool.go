package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func poolObject() *unstructured.Unstructured {
	pool := &unstructured.Unstructured{}
	pool.SetAPIVersion("ate.dev/v1alpha1")
	pool.SetKind("WorkerPool")
	return pool
}

// The source pool is operator infrastructure. Each request gets a separate
// single-replica pool whose workers are born with their confinement labels.
func (d *Lifecycle) prepareRuntimePool(ctx context.Context, record *journalRecord) error {
	source := poolObject()
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.Placement.Name}, source); err != nil {
		return err
	}
	if source.GetUID() != record.Placement.UID || source.GetDeletionTimestamp() != nil {
		return sdk.ErrStaleIdentity
	}
	record.SourceSelector = maps.Clone(record.TemplateSpec.GetWorkerSelector().GetMatchLabels())
	for _, key := range []string{workerAllocationLabel, workerInstanceLabel} {
		if _, found := record.SourceSelector[key]; found {
			return fmt.Errorf("operator native selector uses a reserved confinement label")
		}
		if _, found := source.GetLabels()[key]; found {
			return fmt.Errorf("operator native pool uses a reserved confinement label")
		}
	}
	spec, found, err := unstructured.NestedMap(source.Object, "spec")
	if err != nil || !found {
		return fmt.Errorf("operator WorkerPool has no usable immutable specification")
	}
	if image, _, _ := unstructured.NestedString(spec, "workerImage"); image == "" {
		return fmt.Errorf("operator WorkerPool has no worker image")
	}
	spec["replicas"] = int64(1)
	if spec["sandboxClass"] == nil {
		spec["sandboxClass"] = "gvisor"
	}
	if spec["sandboxClass"] != "gvisor" {
		return fmt.Errorf("operator WorkerPool must use gVisor")
	}
	template, _, err := unstructured.NestedMap(spec, "template")
	if err != nil {
		return err
	}
	if template == nil {
		template = map[string]any{}
	}
	podLabels, _, err := unstructured.NestedStringMap(template, "labels")
	if err != nil {
		return err
	}
	if podLabels == nil {
		podLabels = map[string]string{}
	}
	for key, value := range workerLabels(record) {
		if foreign := podLabels[key]; foreign != "" && foreign != value {
			return fmt.Errorf("operator worker template uses a reserved confinement label")
		}
		podLabels[key] = value
	}
	if err := unstructured.SetNestedStringMap(template, podLabels, "labels"); err != nil {
		return err
	}
	spec["template"] = template
	record.RuntimePoolSpec, err = json.Marshal(spec)
	if err != nil {
		return err
	}
	record.RuntimePool = nativeReference{Name: record.Actor.Name + "-workers", Digest: digest(record.RuntimePoolSpec)}
	record.RuntimePoolLabels = maps.Clone(source.GetLabels())
	if record.RuntimePoolLabels == nil {
		record.RuntimePoolLabels = map[string]string{}
	}
	maps.Copy(record.RuntimePoolLabels, labels(record))
	maps.Copy(record.RuntimePoolLabels, workerLabels(record))
	selector := maps.Clone(record.SourceSelector)
	maps.Copy(selector, workerLabels(record))
	record.TemplateSpec.WorkerSelector = &pb.Selector{MatchLabels: selector}
	record.Template.Digest, err = templateDigest(record.TemplateSpec)
	return err
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
	previousDigest, previousErr := runtimePoolRestoreDigest(previous)
	nextDigest, nextErr := runtimePoolRestoreDigest(next)
	return previousErr == nil && nextErr == nil && previousDigest == nextDigest
}

func runtimePoolRestoreDigest(data json.RawMessage) (string, error) {
	var spec map[string]any
	if len(data) == 0 || json.Unmarshal(data, &spec) != nil {
		return "", fmt.Errorf("private worker infrastructure provenance is missing")
	}
	image, _, err := unstructured.NestedString(spec, "workerImage")
	if err != nil || image == "" || spec["sandboxClass"] != "gvisor" || spec["replicas"] != float64(1) {
		return "", fmt.Errorf("private worker infrastructure provenance is invalid")
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
		return d.save(ctx, cm, record)
	}
	return nil
}

func (d *Lifecycle) verifyRuntimePool(ctx context.Context, record *journalRecord) error {
	pool := poolObject()
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool); err != nil {
		return err
	}
	if record.RuntimePool.UID == "" || string(pool.GetUID()) != record.RuntimePool.UID || pool.GetDeletionTimestamp() != nil || record.RuntimePoolDeleting || !runtimePoolMatches(pool, record) {
		return sdk.ErrStaleIdentity
	}
	return nil
}

// NetworkPolicy and labels exist before this worker is eligible to execute.
// Pin the exact ready, empty worker before issuing the one-time native Resume.
func (d *Lifecycle) prepareWorker(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (bool, error) {
	if err := d.verifyInfrastructureReadOnly(ctx, record); err != nil {
		return false, err
	}
	var selected *pb.Worker
	token := ""
	for pages := 0; pages < 100; pages++ {
		workers, err := d.control.ListWorkers(ctx, &pb.ListWorkersRequest{PageSize: 1000, PageToken: token})
		if err != nil {
			return false, err
		}
		for _, worker := range workers.GetWorkers() {
			if worker.GetWorkerNamespace() != record.Placement.Namespace || worker.GetWorkerPool() != record.RuntimePool.Name {
				continue
			}
			if selected != nil {
				return false, fmt.Errorf("private native pool has multiple workers")
			}
			selected = worker
		}
		token = workers.GetNextPageToken()
		if token == "" {
			break
		}
		if pages == 99 {
			return false, fmt.Errorf("native worker listing exceeded bound")
		}
	}
	if selected == nil {
		return false, nil
	}
	if selected.GetMetadata().GetUid() == "" || selected.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_ACTIVE || selected.GetStatus().GetCapacity().GetActors() != 1 || selected.GetStatus().GetAllocated().GetActors() != 0 {
		return false, fmt.Errorf("private native worker must be active, empty and single-capacity")
	}
	for key, value := range record.TemplateSpec.GetWorkerSelector().GetMatchLabels() {
		if selected.GetLabels()[key] != value {
			return false, sdk.ErrStaleIdentity
		}
	}
	assignments, err := d.control.ListWorkerActorAssignments(ctx, &pb.ListWorkerActorAssignmentsRequest{Worker: &pb.ObjectRef{Name: selected.GetMetadata().GetName()}, PageSize: 1000})
	if err != nil {
		return false, err
	}
	if len(assignments.GetActorAssignments()) != 0 || assignments.GetNextPageToken() != "" {
		return false, fmt.Errorf("private worker has another Actor assignment")
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: selected.WorkerNamespace, Name: selected.WorkerPod}, pod); err != nil {
		return false, err
	}
	fence := &workerFence{Name: selected.Metadata.Name, UID: selected.Metadata.Uid, Namespace: selected.WorkerNamespace, Pool: selected.WorkerPool, Pod: selected.WorkerPod, PodUID: selected.WorkerPodUid, AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.InstanceID}
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
	if err := d.verifyWorkerNetworkPolicies(ctx, record, pod); err != nil {
		return false, err
	}
	if record.Worker == nil {
		record.Worker = fence
		if err := d.save(ctx, cm, record); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (d *Lifecycle) deleteRuntimePool(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (bool, error) {
	if !record.RuntimePool.CreateIssued {
		return true, nil
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
	if string(pool.GetUID()) != record.RuntimePool.UID || !runtimePoolMatches(pool, record) {
		return false, sdk.ErrStaleIdentity
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
