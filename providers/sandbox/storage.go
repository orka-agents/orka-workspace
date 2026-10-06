package sandbox

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/sandbox/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const durableVolumeName = "orka-workspace"
const durableMountPath = "/durable/orka-workspace"

func validateDeletionPolicy(policy workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) error {
	if policy.PersistentVolumes != workspacev1alpha1.WorkspaceDeletionActionDelete || policy.Checkpoints != workspacev1alpha1.WorkspaceDeletionActionDelete || policy.ProviderResources != workspacev1alpha1.WorkspaceDeletionActionDelete {
		return fmt.Errorf("Sandbox supports only all-Delete dispositions")
	}
	return nil
}

func (d *Lifecycle) validateWorkspaceLifecycle(ctx context.Context, record *journalRecord, suspend bool) error {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	key := record.Request.Key
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != key.ProviderUID || workspace.Spec.ClassBinding != record.Request.Runtime.ClassBinding {
		return workspaceprovider.ErrStaleIdentity
	}
	lifecycle := workspace.Spec.Lifecycle
	if err := validateDeletionPolicy(lifecycle.DeletionPolicy); err != nil {
		return err
	}
	if !suspend {
		return nil
	}
	if workspace.Spec.Mode != workspacev1alpha1.ExecutionWorkspaceModeInteractive || workspace.Spec.SessionRef == nil || workspace.Spec.SessionRef.Name == "" || workspace.Spec.SessionRef.UID == "" || !slices.Contains(lifecycle.AllowedOnDetach, workspacev1alpha1.WorkspaceOnDetachSuspend) {
		return fmt.Errorf("Sandbox suspension requires interactive session reuse and allowed Suspend")
	}
	if lifecycle.IdleTimeout != nil && lifecycle.IdleTimeout.Duration <= 0 || lifecycle.MaxLifetime != nil && lifecycle.MaxLifetime.Duration <= 0 {
		return fmt.Errorf("Sandbox suspension expiry must be positive")
	}
	if lifecycle.IdleTimeout == nil && lifecycle.MaxLifetime == nil {
		return fmt.Errorf("Sandbox suspension requires a positive retention expiry")
	}
	if lifecycle.IdleTimeout != nil && lifecycle.MaxLifetime != nil && lifecycle.MaxLifetime.Duration < lifecycle.IdleTimeout.Duration {
		return fmt.Errorf("Sandbox maxLifetime must be at least idleTimeout")
	}
	if record.MaxSuspended != nil && lifecycle.MaxLifetime == nil {
		return fmt.Errorf("suspended count cap requires a positive maxLifetime")
	}
	return nil
}

func (d *Lifecycle) profile(ctx context.Context, record *journalRecord) (*profilev1alpha1.SandboxWorkspaceProfile, error) {
	ref := record.Request.ParametersRef
	if ref == nil {
		return nil, nil
	}
	if ref.Group != profilev1alpha1.GroupVersion.Group || ref.Kind != "SandboxWorkspaceProfile" {
		return nil, fmt.Errorf("Sandbox requires SandboxWorkspaceProfile parameters")
	}
	raw := &unstructured.Unstructured{}
	raw.SetGroupVersionKind(profilev1alpha1.GroupVersion.WithKind("SandboxWorkspaceProfile"))
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Request.Key.Namespace, Name: ref.Name}, raw); err != nil {
		return nil, err
	}
	profile := &profilev1alpha1.SandboxWorkspaceProfile{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, profile); err != nil {
		return nil, err
	}
	binding := record.Request.ParametersBinding
	if profile.UID != binding.UID || profile.Generation != binding.Generation || profile.DeletionTimestamp != nil {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	hash, err := workspaceprovider.ParametersProfileHash(raw)
	if err != nil {
		return nil, err
	}
	if hash != binding.ProfileHash {
		return nil, fmt.Errorf("Sandbox profile hash changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	return profile, nil
}

func freezeRetention(record *journalRecord, profile *profilev1alpha1.SandboxWorkspaceProfile) {
	record.RetentionResolved = true
	record.MaxSuspended = nil
	if profile != nil && profile.Spec.Retention != nil && profile.Spec.Retention.MaxSuspendedWorkspaces != nil {
		limit := *profile.Spec.Retention.MaxSuspendedWorkspaces
		record.MaxSuspended = &limit
	}
}

// Older journals omitted retention. Re-resolve only their pinned immutable
// profile before the first suspension; already reserved journals remain frozen.
func (d *Lifecycle) resolveRetention(ctx context.Context, record *journalRecord) error {
	if record.RetentionResolved {
		return nil
	}
	profile, err := d.profile(ctx, record)
	if err != nil {
		return err
	}
	freezeRetention(record, profile)
	return nil
}

func (d *Lifecycle) resolveProfile(ctx context.Context, record *journalRecord) error {
	profile, err := d.profile(ctx, record)
	if err != nil {
		return err
	}
	freezeRetention(record, profile)
	if profile == nil {
		return nil
	}
	if profile.Spec.Suspend == nil {
		return nil
	}
	if err := d.validateWorkspaceLifecycle(ctx, record, true); err != nil {
		return err
	}
	volume, err := profile.Spec.Suspend.ResolveVolume()
	if err != nil {
		return err
	}
	if err := validateDurableRuntime(record); err != nil {
		return err
	}
	class := &storagev1.StorageClass{}
	if volume.StorageClassName == "" {
		var classes storagev1.StorageClassList
		if err := d.client.List(ctx, &classes); err != nil {
			return err
		}
		for i := range classes.Items {
			candidate := &classes.Items[i]
			if candidate.Annotations["storageclass.kubernetes.io/is-default-class"] != "true" && candidate.Annotations["storageclass.beta.kubernetes.io/is-default-class"] != "true" {
				continue
			}
			if class.Name != "" {
				return fmt.Errorf("multiple default StorageClasses are ambiguous")
			}
			class = candidate
		}
		if class.Name == "" {
			return fmt.Errorf("no default StorageClass is available")
		}
		volume.StorageClassName = class.Name
	} else if err := d.client.Get(ctx, types.NamespacedName{Name: volume.StorageClassName}, class); err != nil {
		return err
	}
	if class.UID == "" || class.DeletionTimestamp != nil || class.Provisioner == "" || class.Provisioner == "kubernetes.io/no-provisioner" || (class.ReclaimPolicy != nil && *class.ReclaimPolicy != corev1.PersistentVolumeReclaimDelete) {
		return fmt.Errorf("durable StorageClass must dynamically provision with Delete reclaim policy")
	}
	record.Volume = &volume
	record.StorageClassUID = class.UID
	return nil
}

func (d *Lifecycle) verifyStorageClass(ctx context.Context, record *journalRecord) error {
	class := &storagev1.StorageClass{}
	if err := d.client.Get(ctx, types.NamespacedName{Name: record.Volume.StorageClassName}, class); err != nil {
		return err
	}
	if class.UID != record.StorageClassUID || class.DeletionTimestamp != nil || (class.ReclaimPolicy != nil && *class.ReclaimPolicy != corev1.PersistentVolumeReclaimDelete) {
		return fmt.Errorf("pinned StorageClass changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	return nil
}

func volumeTemplate(record *journalRecord) (sandboxv1beta1.PersistentVolumeClaimTemplate, error) {
	volume := record.Volume
	capacity, err := resource.ParseQuantity(volume.Capacity)
	if err != nil || capacity.Sign() <= 0 {
		return sandboxv1beta1.PersistentVolumeClaimTemplate{}, fmt.Errorf("durable workspace capacity %q must be a positive storage quantity", volume.Capacity)
	}
	modes := make([]corev1.PersistentVolumeAccessMode, len(volume.AccessModes))
	for i, mode := range volume.AccessModes {
		modes[i] = corev1.PersistentVolumeAccessMode(mode)
	}
	result := sandboxv1beta1.PersistentVolumeClaimTemplate{Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &volume.StorageClassName, AccessModes: modes, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: capacity}}}}
	result.Name = durableVolumeName
	return result, nil
}

func (d *Lifecycle) verifyStorage(ctx context.Context, record *journalRecord, sb *sandboxv1beta1.Sandbox) (*storageIdentity, error) {
	if record.Volume == nil {
		return nil, nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: durableVolumeName + "-" + sb.Name}, pvc); err != nil {
		return nil, err
	}
	if pvc.UID == "" || !metav1.IsControlledBy(pvc, sb) || pvc.DeletionTimestamp != nil {
		return nil, fmt.Errorf("durable PVC ownership or lifetime changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	template, err := volumeTemplate(record)
	if err != nil {
		return nil, err
	}
	expected := template.Spec
	actual := *pvc.Spec.DeepCopy()
	actual.VolumeName = ""
	if actual.VolumeMode != nil && *actual.VolumeMode == corev1.PersistentVolumeFilesystem {
		actual.VolumeMode = nil
	}
	slices.Sort(actual.AccessModes)
	slices.Sort(expected.AccessModes)
	if !reflect.DeepEqual(actual, expected) {
		return nil, fmt.Errorf("durable PVC spec differs from its frozen volume template")
	}
	if pvc.Spec.VolumeName == "" {
		return nil, fmt.Errorf("durable PVC is not bound")
	}
	pv := &corev1.PersistentVolume{}
	if err := d.client.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
		return nil, err
	}
	if pv.UID == "" || pv.DeletionTimestamp != nil || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete || pv.Annotations["pv.kubernetes.io/provisioned-by"] == "" || pv.Spec.StorageClassName != record.Volume.StorageClassName {
		return nil, fmt.Errorf("durable PV does not prove dynamically provisioned all-Delete ownership")
	}
	identity := &storageIdentity{ClaimName: pvc.Name, ClaimUID: pvc.UID, VolumeName: pv.Name, VolumeUID: pv.UID}
	if record.Storage != nil && *record.Storage != *identity {
		return nil, fmt.Errorf("retained PVC or PV identity changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	if record.Storage == nil {
		if err := d.verifyStorageClass(ctx, record); err != nil {
			return nil, err
		}
	}
	return identity, nil
}

func validateDurableRuntime(record *journalRecord) error {
	container := record.Request.Runtime.Template.Spec.Containers[0]
	foundMount, foundEnv := false, false
	for _, mount := range container.VolumeMounts {
		if mount.Name == durableVolumeName {
			if mount.MountPath != durableMountPath || mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" {
				return fmt.Errorf("durable workspace mount differs from the supported layout")
			}
			foundMount = true
		}
	}
	for _, env := range container.Env {
		if env.Name == "ORKA_ACP_DURABLE_WORKSPACE_DIR" {
			if env.Value != durableMountPath || env.ValueFrom != nil {
				return fmt.Errorf("durable workspace environment differs from the supported layout")
			}
			foundEnv = true
		}
	}
	for _, v := range record.Request.Runtime.Template.Spec.Volumes {
		if v.Name == durableVolumeName {
			return fmt.Errorf("durable workspace PVC is injected by the SandboxClaim")
		}
	}
	if !foundMount || !foundEnv {
		return fmt.Errorf("suspend-capable runtime is missing the admitted durable mount or environment")
	}
	return nil
}
