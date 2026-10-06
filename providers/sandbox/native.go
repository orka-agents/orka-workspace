package sandbox

import (
	"context"
	"fmt"
	"maps"
	"reflect"

	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func validateRequest(request workspaceprovider.WorkloadRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Runtime == nil || request.Runtime.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return fmt.Errorf("Sandbox requires an admitted runtime with restartPolicy Never")
	}
	if len(request.Runtime.Template.Spec.Containers) != 1 || len(request.Runtime.Template.Spec.InitContainers) != 0 {
		return fmt.Errorf("Sandbox requires one supervisor container and no init containers")
	}
	for key := range request.Runtime.Template.Labels {
		if key == extv1beta1.SandboxIDLabel || key == sandboxv1beta1.SandboxTemplateRefHashLabel || key == sandboxcontrollers.SandboxNameHashLabel {
			return fmt.Errorf("runtime template carries provider-owned label %q", key)
		}
	}
	return nil
}

func anchorOwner(record *journalRecord) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: record.Anchor.Name, UID: record.Anchor.UID, Controller: &controller}
}

func nativeMetadata(record *journalRecord, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: record.Namespace, Name: name, Labels: labels(record), OwnerReferences: []metav1.OwnerReference{anchorOwner(record)}}
}

func desiredTemplate(record *journalRecord) *extv1beta1.SandboxTemplate {
	template := record.Request.Runtime.Template
	policy := extv1beta1.VolumeClaimTemplatesPolicyDisallowed
	if record.Volume != nil {
		policy = extv1beta1.VolumeClaimTemplatesPolicyAllowed
	}
	return &extv1beta1.SandboxTemplate{ObjectMeta: nativeMetadata(record, record.Template.Name), Spec: extv1beta1.SandboxTemplateSpec{
		SandboxBlueprint:        sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{ObjectMeta: sandboxv1beta1.PodMetadata{Labels: maps.Clone(template.Labels), Annotations: maps.Clone(template.Annotations)}, Spec: *template.Spec.DeepCopy()}},
		NetworkPolicyManagement: extv1beta1.NetworkPolicyManagementUnmanaged, EnvVarsInjectionPolicy: extv1beta1.EnvVarsInjectionPolicyDisallowed, VolumeClaimTemplatesPolicy: policy}}
}

func desiredWarmPool(record *journalRecord) *extv1beta1.SandboxWarmPool {
	return &extv1beta1.SandboxWarmPool{ObjectMeta: nativeMetadata(record, record.WarmPool.Name), Spec: extv1beta1.SandboxWarmPoolSpec{Replicas: new(int32), TemplateRef: extv1beta1.SandboxTemplateRef{Name: record.Template.Name}}}
}

func desiredClaim(record *journalRecord) (*extv1beta1.SandboxClaim, error) {
	claim := &extv1beta1.SandboxClaim{ObjectMeta: nativeMetadata(record, record.Claim.Name), Spec: extv1beta1.SandboxClaimSpec{WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: record.WarmPool.Name}}}
	if record.Volume != nil {
		volume, err := volumeTemplate(record)
		if err != nil {
			return nil, err
		}
		claim.Spec.VolumeClaimTemplates = []sandboxv1beta1.PersistentVolumeClaimTemplate{volume}
	}
	return claim, nil
}

func nativeOwned(object client.Object, record *journalRecord, ref objectReference) bool {
	expectedLabels := labels(record)
	if _, ok := object.(*extv1beta1.SandboxTemplate); ok {
		// Upstream v1.0.3 publishes this deterministic reference hash on its
		// own template. It adds no ownership authority and may be absent
		// before the upstream controller's first reconciliation.
		if value, exists := object.GetLabels()[sandboxv1beta1.SandboxTemplateRefHashLabel]; exists {
			if value != sandboxcontrollers.NameHash(record.Template.Name) {
				return false
			}
			expectedLabels[sandboxv1beta1.SandboxTemplateRefHashLabel] = value
		}
	}
	return object.GetNamespace() == record.Namespace && object.GetName() == ref.Name && object.GetUID() != "" && (ref.UID == "" || object.GetUID() == ref.UID) &&
		reflect.DeepEqual(object.GetLabels(), expectedLabels) && reflect.DeepEqual(object.GetOwnerReferences(), []metav1.OwnerReference{anchorOwner(record)})
}

// ensureObject persists the issued create before sending it. An unresolved
// create followed by absence is ambiguous and cannot authorize recreation.
func (d *Lifecycle) ensureObject(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, ref *objectReference, object client.Object, desired client.Object) error {
	err := d.client.Get(ctx, client.ObjectKeyFromObject(desired), object)
	if apierrors.IsNotFound(err) {
		if ref.UID != "" || ref.CreateIssued {
			return fmt.Errorf("native %T creation or deletion is unresolved: %w", object, workspaceprovider.ErrStaleIdentity)
		}
		ref.CreateIssued = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		if err := d.client.Create(ctx, desired); err != nil {
			if definitiveCreateRejection(err) {
				ref.CreateIssued = false
				if saveErr := d.save(ctx, cm, record); saveErr != nil {
					return saveErr
				}
			}
			return err
		}
		object = desired
	} else if err != nil {
		return err
	}
	if !nativeOwned(object, record, *ref) || object.GetDeletionTimestamp() != nil {
		return workspaceprovider.ErrStaleIdentity
	}
	if ref.UID == "" {
		ref.UID = object.GetUID()
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	return nil
}

func (d *Lifecycle) ensureAnchor(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	anchor := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: record.Namespace, Name: record.Anchor.Name}
	err := d.client.Get(ctx, key, anchor)
	if apierrors.IsNotFound(err) {
		if record.Anchor.UID != "" || record.Anchor.CreateIssued {
			return fmt.Errorf("native ownership anchor is missing after creation intent: %w", workspaceprovider.ErrStaleIdentity)
		}
		record.Anchor.CreateIssued = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		immutable := true
		anchor = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: labels(record)}, Immutable: &immutable, Data: map[string]string{"journalNamespace": cm.Namespace, "journalName": cm.Name, "journalUID": string(cm.UID)}}
		if err := d.client.Create(ctx, anchor); err != nil {
			if definitiveCreateRejection(err) {
				record.Anchor.CreateIssued = false
				if saveErr := d.save(ctx, cm, record); saveErr != nil {
					return saveErr
				}
			}
			return err
		}
	} else if err != nil {
		return err
	}
	if anchor.UID == "" || (record.Anchor.UID != "" && anchor.UID != record.Anchor.UID) || !reflect.DeepEqual(anchor.Labels, labels(record)) || len(anchor.OwnerReferences) != 0 || anchor.Immutable == nil || !*anchor.Immutable || anchor.Data["journalUID"] != string(cm.UID) || anchor.Data["journalName"] != cm.Name || anchor.Data["journalNamespace"] != cm.Namespace || anchor.DeletionTimestamp != nil {
		return workspaceprovider.ErrStaleIdentity
	}
	if record.Anchor.UID == "" {
		record.Anchor.UID = anchor.UID
		return d.save(ctx, cm, record)
	}
	return nil
}

// A rejected API request cannot have created an object. A timeout or transport
// failure still leaves its durable creation intent unresolved.
func definitiveCreateRejection(err error) bool {
	return apierrors.IsInvalid(err) || apierrors.IsForbidden(err) || apierrors.IsBadRequest(err) ||
		apierrors.IsUnauthorized(err) || apierrors.IsNotFound(err) || apierrors.IsTooManyRequests(err)
}

func (d *Lifecycle) ensureNative(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if err := d.ensureAnchor(ctx, cm, record); err != nil {
		return err
	}
	template := &extv1beta1.SandboxTemplate{}
	expectedTemplate := desiredTemplate(record)
	if err := d.ensureObject(ctx, cm, record, &record.Template, template, expectedTemplate); err != nil {
		return err
	}
	if err := d.client.Get(ctx, client.ObjectKeyFromObject(expectedTemplate), template); err != nil {
		return err
	}
	if !apiequality.Semantic.DeepEqual(template.Spec, expectedTemplate.Spec) {
		return fmt.Errorf("SandboxTemplate drifted from the journal")
	}
	warm := &extv1beta1.SandboxWarmPool{}
	expectedWarm := desiredWarmPool(record)
	if err := d.ensureObject(ctx, cm, record, &record.WarmPool, warm, expectedWarm); err != nil {
		return err
	}
	if err := d.client.Get(ctx, client.ObjectKeyFromObject(expectedWarm), warm); err != nil {
		return err
	}
	if !apiequality.Semantic.DeepEqual(warm.Spec, expectedWarm.Spec) {
		return fmt.Errorf("SandboxWarmPool drifted from zero warm capacity")
	}
	if record.Volume != nil && record.Storage == nil {
		if err := d.verifyStorageClass(ctx, record); err != nil {
			return err
		}
	}
	claim := &extv1beta1.SandboxClaim{}
	expectedClaim, err := desiredClaim(record)
	if err != nil {
		return err
	}
	if err := d.ensureObject(ctx, cm, record, &record.Claim, claim, expectedClaim); err != nil {
		return err
	}
	if err := d.client.Get(ctx, client.ObjectKeyFromObject(expectedClaim), claim); err != nil {
		return err
	}
	if !apiequality.Semantic.DeepEqual(claim.Spec, expectedClaim.Spec) {
		return fmt.Errorf("SandboxClaim drifted from the journal")
	}
	return nil
}

func (d *Lifecycle) sandbox(ctx context.Context, record *journalRecord) (*sandboxv1beta1.Sandbox, error) {
	return d.readSandbox(ctx, record, true)
}

func (d *Lifecycle) readSandbox(ctx context.Context, record *journalRecord, validateSpec bool) (*sandboxv1beta1.Sandbox, error) {
	claim := &extv1beta1.SandboxClaim{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: record.Claim.Name}, claim); err != nil {
		return nil, err
	}
	if !nativeOwned(claim, record, record.Claim) {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	if validateSpec {
		expected, err := desiredClaim(record)
		if err != nil {
			return nil, err
		}
		if !apiequality.Semantic.DeepEqual(claim.Spec, expected.Spec) {
			return nil, workspaceprovider.ErrStaleIdentity
		}
	}
	if claim.Status.SandboxStatus.Name != "" && claim.Status.SandboxStatus.Name != record.Claim.Name {
		return nil, fmt.Errorf("claim selected an unexpected Sandbox")
	}
	sb := &sandboxv1beta1.Sandbox{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: record.Claim.Name}, sb); err != nil {
		return nil, err
	}
	if sb.UID == "" || (record.Sandbox.UID != "" && sb.UID != record.Sandbox.UID) || !metav1.IsControlledBy(sb, claim) || sb.Labels[extv1beta1.SandboxIDLabel] != string(claim.UID) || sb.Annotations[sandboxv1beta1.SandboxTemplateRefAnnotation] != record.Template.Name {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	if validateSpec && !apiequality.Semantic.DeepEqual(sb.Spec.VolumeClaimTemplates, claim.Spec.VolumeClaimTemplates) {
		return nil, fmt.Errorf("Sandbox durable volume template differs from claim")
	}
	return sb, nil
}

func attestBlueprint(record *journalRecord, sb *sandboxv1beta1.Sandbox) error {
	template := desiredTemplate(record)
	if !apiequality.Semantic.DeepEqual(template.Spec.PodTemplate.Spec, sb.Spec.PodTemplate.Spec) {
		return fmt.Errorf("Sandbox PodSpec differs from the admitted template")
	}
	actual := maps.Clone(sb.Spec.PodTemplate.ObjectMeta.Labels)
	if actual[extv1beta1.SandboxIDLabel] != string(record.Claim.UID) || actual[sandboxv1beta1.SandboxTemplateRefHashLabel] != sandboxcontrollers.NameHash(record.Template.Name) {
		return workspaceprovider.ErrStaleIdentity
	}
	delete(actual, extv1beta1.SandboxIDLabel)
	delete(actual, sandboxv1beta1.SandboxTemplateRefHashLabel)
	if len(actual) == 0 {
		actual = nil
	}
	if !reflect.DeepEqual(template.Spec.PodTemplate.ObjectMeta.Labels, actual) || !reflect.DeepEqual(template.Spec.PodTemplate.ObjectMeta.Annotations, sb.Spec.PodTemplate.ObjectMeta.Annotations) {
		return fmt.Errorf("Sandbox Pod metadata differs from the admitted template")
	}
	return nil
}

func (d *Lifecycle) pod(ctx context.Context, record *journalRecord, sb *sandboxv1beta1.Sandbox) (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := d.client.List(ctx, &pods, client.InNamespace(record.Namespace)); err != nil {
		return nil, err
	}
	var found *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, sb) {
			if pod.Name == sb.Name {
				return nil, workspaceprovider.ErrStaleIdentity
			}
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("Sandbox has multiple Pods")
		}
		found = pod
	}
	if found == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("pods"), sb.Name)
	}
	if record.Pod != nil && (found.Name != record.Pod.Name || found.UID != record.Pod.UID) {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	if record.PreviousPod != nil && found.UID == record.PreviousPod.UID {
		return nil, fmt.Errorf("cold resume reused the retired Pod")
	}
	return found, nil
}
