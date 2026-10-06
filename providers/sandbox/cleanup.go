package sandbox

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (d *Lifecycle) StopInstance(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	return d.retire(ctx, key, identity, false)
}

func (d *Lifecycle) SuspendInstance(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	return d.retire(ctx, key, identity, true)
}

func (d *Lifecycle) retire(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity, suspend bool) (workspaceprovider.AllocationObservation, error) {
	var observed workspaceprovider.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, key)
		if err != nil {
			return err
		}
		if record.Observation.Identity != identity {
			return workspaceprovider.ErrStaleIdentity
		}
		if err := d.requireJournal(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		if observed.State == workspaceprovider.AllocationDeleted {
			return nil
		}
		if suspend {
			if err := d.resolveRetention(ctx, record); err != nil {
				return err
			}
			if record.Operation == "suspend" {
				if err := d.verifySuspendedReservation(ctx, cm, record); err != nil {
					return err
				}
			}
		}
		if observed.State == workspaceprovider.AllocationStopped {
			if suspend && observed.RetainedData == nil {
				return workspaceprovider.ErrRequestConflict
			}
			return nil
		}
		if record.Operation == "delete" {
			return workspaceprovider.ErrRequestConflict
		}
		if suspend && record.Volume == nil {
			return fmt.Errorf("Sandbox profile does not permit data-only suspension")
		}
		operation := "stop"
		if suspend {
			operation = "suspend"
		}
		if record.Operation != "ensure" && record.Operation != operation && !(record.Operation == "suspend" && !suspend) {
			return workspaceprovider.ErrRequestConflict
		}
		if err := d.recoverIssued(ctx, cm, record); err != nil {
			return err
		}
		sb, err := d.readSandbox(ctx, record, suspend)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if suspend && apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot suspend a missing Sandbox")
		}
		if err == nil {
			if record.Sandbox.UID == "" {
				record.Sandbox = objectReference{Name: sb.Name, UID: sb.UID}
			}
			pod, podErr := d.pod(ctx, record, sb)
			if podErr != nil && !apierrors.IsNotFound(podErr) {
				if suspend || record.Pod == nil || !errors.Is(podErr, workspaceprovider.ErrStaleIdentity) {
					return podErr
				}
				// Metadata loss permits cleanup only when the separately journaled
				// Pod still has its exact UID, before the upstream producer changes.
				exact, err := d.journaledPod(ctx, record)
				if err != nil {
					return err
				}
				var pods corev1.PodList
				if err := d.client.List(ctx, &pods, client.InNamespace(record.Namespace)); err != nil {
					return err
				}
				for _, candidate := range pods.Items {
					if candidate.UID != exact.UID && metav1.IsControlledBy(&candidate, sb) {
						return workspaceprovider.ErrStaleIdentity
					}
				}
			}
			if podErr == nil && record.Pod == nil {
				record.Pod = &workspaceprovider.PodReference{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}
			}
			if suspend {
				if err := attestBlueprint(record, sb); err != nil {
					return err
				}
				if record.Pod == nil {
					return fmt.Errorf("data suspension requires an observed exact runtime Pod")
				}
				record.Storage, err = d.verifyStorage(ctx, record, sb)
				if err != nil {
					return err
				}
			}
		}
		if suspend {
			if err := d.reserveSuspended(ctx, cm, record); err != nil {
				return err
			}
		}
		record.Operation = operation
		record.Observation = pendingObservation(record)
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		if sb != nil && sb.UID != "" {
			marker := strconv.FormatInt(record.Request.Sequence, 10) + ":" + operation
			if sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended || sb.Annotations[nativeOperationAnnotation] != marker {
				sb.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
				if sb.Annotations == nil {
					sb.Annotations = map[string]string{}
				}
				sb.Annotations[nativeOperationAnnotation] = marker
				if err := d.client.Update(ctx, sb); err != nil {
					return err
				}
				observed = record.Observation
				return nil
			}
			if !suspend {
				gone, err := d.stopJournaledPod(ctx, record)
				if err != nil {
					return err
				}
				if !gone {
					observed = record.Observation
					return nil
				}
			}
			if err := d.confirmSuspended(ctx, record, sb); err != nil {
				if err == workspaceprovider.ErrInstanceRunning {
					observed = record.Observation
					return nil
				}
				return err
			}
		} else {
			// A claim without a Sandbox can still have a pending upstream create.
			// Foreground claim deletion closes that producer before absence counts.
			gone, err := d.deleteObject(ctx, record.Namespace, record.Claim, &extv1beta1.SandboxClaim{})
			if err != nil {
				return err
			}
			if !gone {
				observed = record.Observation
				return nil
			}
			gone, err = d.stopJournaledPod(ctx, record)
			if err != nil {
				return err
			}
			if !gone {
				observed = record.Observation
				return nil
			}
			for _, object := range []client.Object{&sandboxv1beta1.Sandbox{}, &corev1.Pod{}} {
				err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: record.Claim.Name}, object)
				if err == nil {
					return fmt.Errorf("native materialization appeared during claim retirement")
				}
				if !apierrors.IsNotFound(err) {
					return err
				}
			}
		}
		record.Observation.State = workspaceprovider.AllocationStopped
		if suspend {
			record.Observation.RetainedData = retainedData(record)
		}
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		return nil
	})
	return observed, err
}

// Stop uses the committed Pod UID even if ownership metadata has drifted. The
// upstream producer must already be suspended or deleted before this call.
// Suspension keeps its stricter ownership and storage checks.
func (d *Lifecycle) stopJournaledPod(ctx context.Context, record *journalRecord) (bool, error) {
	if record.Pod == nil {
		return true, nil
	}
	pod, err := d.journaledPod(ctx, record)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if pod.DeletionTimestamp == nil {
		version := pod.ResourceVersion
		if err := d.client.Delete(ctx, pod, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &record.Pod.UID, ResourceVersion: &version}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return false, nil
}

func (d *Lifecycle) journaledPod(ctx context.Context, record *journalRecord) (*corev1.Pod, error) {
	ref := record.Pod
	if ref == nil || ref.Namespace != record.Namespace || ref.Name == "" || ref.UID == "" {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, pod); err != nil {
		return nil, err
	}
	if pod.UID != ref.UID {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	return pod, nil
}

func (d *Lifecycle) recoverIssued(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if record.Anchor.CreateIssued && record.Anchor.UID == "" {
		if err := d.ensureAnchor(ctx, cm, record); err != nil {
			return err
		}
	}
	for _, entry := range []struct {
		ref    *objectReference
		object client.Object
	}{
		{&record.Template, &extv1beta1.SandboxTemplate{}}, {&record.WarmPool, &extv1beta1.SandboxWarmPool{}}, {&record.Claim, &extv1beta1.SandboxClaim{}},
	} {
		if !entry.ref.CreateIssued || entry.ref.UID != "" {
			continue
		}
		if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: entry.ref.Name}, entry.object); err != nil {
			return fmt.Errorf("native creation outcome remains unresolved: %w", err)
		}
		if !nativeOwned(entry.object, record, *entry.ref) {
			return workspaceprovider.ErrStaleIdentity
		}
		entry.ref.UID = entry.object.GetUID()
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	return nil
}

func deletedDisposition(hasVolume bool) *workspacev1alpha1.ExecutionWorkspaceDisposition {
	persistent := workspacev1alpha1.DispositionNotApplicable
	if hasVolume {
		persistent = workspacev1alpha1.DispositionDeleted
	}
	return &workspacev1alpha1.ExecutionWorkspaceDisposition{Compute: workspacev1alpha1.DispositionDeleted, AccessCredentials: workspacev1alpha1.DispositionNotApplicable, EphemeralSecrets: workspacev1alpha1.DispositionNotApplicable, WorkspaceData: persistent, PersistentVolumes: persistent, Checkpoints: workspacev1alpha1.DispositionNotApplicable, ProviderResources: workspacev1alpha1.DispositionDeleted}
}

func (d *Lifecycle) DeleteAllocation(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity, policy workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (workspaceprovider.AllocationObservation, error) {
	if policy.PersistentVolumes != workspacev1alpha1.WorkspaceDeletionActionDelete || policy.Checkpoints != workspacev1alpha1.WorkspaceDeletionActionDelete || policy.ProviderResources != workspacev1alpha1.WorkspaceDeletionActionDelete {
		return workspaceprovider.AllocationObservation{}, fmt.Errorf("Sandbox supports only all-Delete dispositions")
	}
	var observed workspaceprovider.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, key)
		if err != nil {
			return err
		}
		if record.Observation.Identity != identity {
			return workspaceprovider.ErrStaleIdentity
		}
		if err := d.requireJournal(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		if observed.State == workspaceprovider.AllocationDeleted {
			if record.DeletionPolicy == nil || *record.DeletionPolicy != policy {
				return workspaceprovider.ErrRequestConflict
			}
			return d.releaseSuspended(ctx, cm, record)
		}
		if observed.State != workspaceprovider.AllocationStopped {
			return workspaceprovider.ErrInstanceRunning
		}
		if record.DeletionPolicy != nil && *record.DeletionPolicy != policy {
			return workspaceprovider.ErrRequestConflict
		}
		if err := d.verifyCleanupDescendants(ctx, record); err != nil {
			return err
		}
		record.Operation = "delete"
		record.DeletionPolicy = &policy
		if err := d.captureCleanupStorage(ctx, record); err != nil {
			return err
		}
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		// Close the producer without letting a parent cascade delete a newly
		// replaced descendant. The journaled Sandbox is deleted by its own UID.
		gone, err := d.deleteObject(ctx, record.Namespace, record.Claim, &extv1beta1.SandboxClaim{}, metav1.DeletePropagationOrphan)
		if err != nil || !gone {
			return err
		}
		sandboxRef := record.Sandbox
		if sandboxRef.Name == "" {
			sandboxRef.Name = record.Claim.Name
		}
		gone, err = d.deleteObject(ctx, record.Namespace, sandboxRef, &sandboxv1beta1.Sandbox{})
		if err != nil || !gone {
			return err
		}
		if record.Pod != nil {
			pod := &corev1.Pod{}
			err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Pod.Namespace, Name: record.Pod.Name}, pod)
			if err == nil {
				return workspaceprovider.ErrInstanceRunning
			}
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
		if record.Storage != nil {
			gone, err = d.deleteObject(ctx, record.Namespace, objectReference{Name: record.Storage.ClaimName, UID: record.Storage.ClaimUID}, &corev1.PersistentVolumeClaim{})
			if err != nil || !gone {
				return err
			}
			// Dynamic provisioner deletion owns the backing volume. Do not bypass
			// its finalizers or report data deletion while the exact PV remains.
			if record.Storage.VolumeName != "" {
				pv := &corev1.PersistentVolume{}
				err := d.client.Get(ctx, types.NamespacedName{Name: record.Storage.VolumeName}, pv)
				if err == nil {
					if pv.UID != record.Storage.VolumeUID {
						return workspaceprovider.ErrStaleIdentity
					}
					return nil
				}
				if !apierrors.IsNotFound(err) {
					return err
				}
			}
		}
		if record.Volume != nil {
			// Provisioning may race cancellation before the first Pod is ready.
			// A PV created after the last PVC read still prevents data closeout.
			var volumes corev1.PersistentVolumeList
			if err := d.client.List(ctx, &volumes); err != nil {
				return err
			}
			for _, volume := range volumes.Items {
				ref := volume.Spec.ClaimRef
				if ref != nil && ref.Namespace == record.Namespace && ref.Name == durableVolumeName+"-"+record.Claim.Name {
					return nil
				}
			}
		}
		for _, entry := range []struct {
			ref    objectReference
			object client.Object
		}{{record.WarmPool, &extv1beta1.SandboxWarmPool{}}, {record.Template, &extv1beta1.SandboxTemplate{}}, {record.Anchor, &corev1.ConfigMap{}}} {
			gone, err = d.deleteObject(ctx, record.Namespace, entry.ref, entry.object)
			if err != nil || !gone {
				return err
			}
		}
		record.Observation.State = workspaceprovider.AllocationDeleted
		record.Observation.RetainedData = nil
		record.Observation.Disposition = deletedDisposition(record.Volume != nil)
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		return d.releaseSuspended(ctx, cm, record)
	})
	return observed, err
}

// A stopped journal is insufficient authority to cascade-delete live native
// objects. Recheck every recorded identity and the exact producer's stop proof
// before the first cleanup mutation; subsequent retries allow prior deletion.
func (d *Lifecycle) verifyCleanupDescendants(ctx context.Context, record *journalRecord) error {
	sandboxRef := record.Sandbox
	if sandboxRef.Name == "" {
		sandboxRef.Name = record.Claim.Name
	}
	sb := &sandboxv1beta1.Sandbox{}
	for _, entry := range []struct {
		ref    objectReference
		object client.Object
	}{{record.Anchor, &corev1.ConfigMap{}}, {record.Template, &extv1beta1.SandboxTemplate{}}, {record.WarmPool, &extv1beta1.SandboxWarmPool{}}, {record.Claim, &extv1beta1.SandboxClaim{}}, {sandboxRef, sb}} {
		if entry.ref.Name == "" {
			continue
		}
		err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: entry.ref.Name}, entry.object)
		if apierrors.IsNotFound(err) {
			if entry.ref.UID == "" && entry.ref.CreateIssued {
				return fmt.Errorf("native creation outcome is unresolved")
			}
			continue
		}
		if err != nil {
			return err
		}
		if entry.ref.UID == "" || entry.object.GetUID() != entry.ref.UID {
			return workspaceprovider.ErrStaleIdentity
		}
	}
	if sb.UID != "" {
		return d.confirmSuspended(ctx, record, sb)
	}
	// A missing Sandbox cannot hide a process whose owner metadata disappeared.
	if record.Pod != nil {
		pod, err := d.journaledPod(ctx, record)
		if err == nil && pod != nil {
			return workspaceprovider.ErrInstanceRunning
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
	}
	// The upstream producer's deterministic name also fences materialization
	// that appeared before its Sandbox or Pod UID was ever committed.
	_, err := d.pod(ctx, record, &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: sandboxRef.Name, UID: sandboxRef.UID}})
	if err == nil {
		return workspaceprovider.ErrInstanceRunning
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (d *Lifecycle) captureCleanupStorage(ctx context.Context, record *journalRecord) error {
	if record.Volume == nil {
		return nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: durableVolumeName + "-" + record.Claim.Name}, pvc)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if record.Storage == nil {
		owner := metav1.GetControllerOf(pvc)
		if record.Sandbox.UID == "" || owner == nil || owner.UID != record.Sandbox.UID || owner.Kind != "Sandbox" || owner.Name != record.Sandbox.Name {
			return fmt.Errorf("cleanup PVC does not belong to the exact Sandbox")
		}
		record.Storage = &storageIdentity{ClaimName: pvc.Name, ClaimUID: pvc.UID}
	} else if record.Storage.ClaimUID != pvc.UID {
		return workspaceprovider.ErrStaleIdentity
	}
	if pvc.Spec.VolumeName != "" {
		pv := &corev1.PersistentVolume{}
		if err := d.client.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return err
		}
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID {
			return workspaceprovider.ErrStaleIdentity
		}
		if record.Storage.VolumeUID != "" && (record.Storage.VolumeUID != pv.UID || record.Storage.VolumeName != pv.Name) {
			return workspaceprovider.ErrStaleIdentity
		}
		record.Storage.VolumeName = pv.Name
		record.Storage.VolumeUID = pv.UID
	}
	return nil
}

// A request retired before native materialization has no Sandbox to refresh.
// Remove its remaining producers before assigning new native names to its
// successor; the old stopped tombstone still retains every attempted identity.
func (d *Lifecycle) removeUnmaterializedAllocation(ctx context.Context, record *journalRecord) (bool, error) {
	for _, object := range []client.Object{&sandboxv1beta1.Sandbox{}, &corev1.Pod{}} {
		err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: record.Claim.Name}, object)
		if err == nil {
			return false, workspaceprovider.ErrInstanceRunning
		}
		if !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	for _, entry := range []struct {
		ref    objectReference
		object client.Object
	}{{record.Claim, &extv1beta1.SandboxClaim{}}, {record.WarmPool, &extv1beta1.SandboxWarmPool{}}, {record.Template, &extv1beta1.SandboxTemplate{}}, {record.Anchor, &corev1.ConfigMap{}}} {
		gone, err := d.deleteObject(ctx, record.Namespace, entry.ref, entry.object)
		if err != nil || !gone {
			return false, err
		}
	}
	return true, nil
}

func (d *Lifecycle) deleteObject(ctx context.Context, namespace string, ref objectReference, object client.Object, propagation ...metav1.DeletionPropagation) (bool, error) {
	if ref.Name == "" {
		return true, nil
	}
	err := d.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, object)
	if apierrors.IsNotFound(err) {
		if ref.UID == "" && ref.CreateIssued {
			return false, fmt.Errorf("native creation outcome is unresolved")
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if ref.UID == "" || object.GetUID() != ref.UID {
		return false, workspaceprovider.ErrStaleIdentity
	}
	if object.GetDeletionTimestamp() == nil {
		policy := metav1.DeletePropagationForeground
		if len(propagation) != 0 {
			policy = propagation[0]
		}
		version := object.GetResourceVersion()
		if err := d.client.Delete(ctx, object, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &ref.UID, ResourceVersion: &version}, PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return false, nil
}
