package sandbox

import (
	"context"
	"fmt"
	"reflect"
	"slices"

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

func (d *Lifecycle) resolveProfile(ctx context.Context, record *journalRecord) error {
	ref := record.Request.ParametersRef
	if ref == nil {
		return nil
	}
	if ref.Group != profilev1alpha1.GroupVersion.Group || ref.Kind != "SandboxWorkspaceProfile" {
		return fmt.Errorf("Sandbox requires SandboxWorkspaceProfile parameters")
	}
	raw := &unstructured.Unstructured{}
	raw.SetGroupVersionKind(profilev1alpha1.GroupVersion.WithKind("SandboxWorkspaceProfile"))
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Request.Key.Namespace, Name: ref.Name}, raw); err != nil {
		return err
	}
	profile := &profilev1alpha1.SandboxWorkspaceProfile{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, profile); err != nil {
		return err
	}
	binding := record.Request.ParametersBinding
	if profile.UID != binding.UID || profile.Generation != binding.Generation || profile.DeletionTimestamp != nil {
		return workspaceprovider.ErrStaleIdentity
	}
	hash, err := workspaceprovider.ParametersProfileHash(raw)
	if err != nil {
		return err
	}
	if hash != binding.ProfileHash {
		return fmt.Errorf("Sandbox profile hash changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	if profile.Spec.Suspend == nil {
		return nil
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

func volumeTemplate(record *journalRecord) sandboxv1beta1.PersistentVolumeClaimTemplate {
	volume := record.Volume
	modes := make([]corev1.PersistentVolumeAccessMode, len(volume.AccessModes))
	for i, mode := range volume.AccessModes {
		modes[i] = corev1.PersistentVolumeAccessMode(mode)
	}
	result := sandboxv1beta1.PersistentVolumeClaimTemplate{Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &volume.StorageClassName, AccessModes: modes, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(volume.Capacity)}}}}
	result.Name = durableVolumeName
	return result
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
	expected := volumeTemplate(record).Spec
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
