package sandbox

import (
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"maps"
	"reflect"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
)

func podLabelsMatch(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) bool {
	if sandbox == nil || pod == nil {
		return false
	}
	expected := maps.Clone(sandbox.Spec.PodTemplate.ObjectMeta.Labels)
	// agent-sandbox v1.0 propagates the claim UID from the Sandbox PodTemplate
	// onto the Pod. Keep it in the exact label comparison so the realized Pod
	// remains bound to the claim -> Sandbox -> Pod identity attested above.
	expected[sandboxcontrollers.SandboxNameHashLabel] = sandboxcontrollers.NameHash(sandbox.Name)
	return reflect.DeepEqual(expected, pod.Labels)
}

func podAnnotationsMatch(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) bool {
	if sandbox == nil || pod == nil {
		return false
	}
	actual := maps.Clone(pod.Annotations)
	// agent-sandbox adds only these bookkeeping annotations while propagating
	// the attested PodTemplate metadata. Admission-added annotations are not
	// allowlisted: annotations can select network and runtime integrations, so
	// any other realized mutation must recycle the workspace before bootstrap.
	delete(actual, sandboxv1beta1.SandboxPropagatedLabelsAnnotation)
	delete(actual, sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation)
	expected := sandbox.Spec.PodTemplate.ObjectMeta.Annotations
	if len(expected) == 0 {
		expected = nil
	}
	if len(actual) == 0 {
		actual = nil
	}
	return reflect.DeepEqual(expected, actual)
}

// podSpecsMatch compares the realized Pod against the
// attested Sandbox template before any credentials cross the network. The
// provider may adopt an existing Pod without reconciling its spec, so owner
// references and identity labels are not sufficient proof. Normalize only
// fields populated by the core API server, admission, or scheduler; every
// container, volume, security, network, and runtime field remains fail-closed.
func podSpecsMatch(expected, actual corev1.PodSpec, injectedDurableClaimName string) bool {
	expectedSpec := normalizePodSpec(expected)
	actualSpec := normalizePodSpec(actual)

	// The provider injects the durable workspace PVC volume from the claim's
	// volumeClaimTemplates; its per-sandbox claim name cannot be rendered into
	// the template, so the reserved-name PVC volume is compared by presence of
	// its mount rather than by claim identity. Any other unexpected volume
	// still fails the match.
	actualSpec.Volumes = stripInjectedDurableWorkspaceVolume(expectedSpec.Volumes, actualSpec.Volumes, injectedDurableClaimName)

	// These fields are derived from cluster scheduling/admission state rather
	// than from the provider-visible Sandbox template.
	expectedSpec.NodeName, actualSpec.NodeName = "", ""
	expectedSpec.Priority, actualSpec.Priority = nil, nil
	expectedSpec.PreemptionPolicy, actualSpec.PreemptionPolicy = nil, nil
	expectedSpec.Overhead, actualSpec.Overhead = nil, nil
	if len(expectedSpec.ImagePullSecrets) == 0 {
		actualSpec.ImagePullSecrets = nil
	}
	if expectedSpec.PriorityClassName == "" {
		actualSpec.PriorityClassName = ""
	}

	return apiequality.Semantic.DeepEqual(expectedSpec, actualSpec)
}

func normalizePodSpec(spec corev1.PodSpec) corev1.PodSpec {
	result := *spec.DeepCopy()
	if result.DNSPolicy == "" {
		result.DNSPolicy = corev1.DNSClusterFirst
	}
	if result.RestartPolicy == "" {
		result.RestartPolicy = corev1.RestartPolicyAlways
	}
	if result.SecurityContext == nil {
		result.SecurityContext = &corev1.PodSecurityContext{}
	}
	if result.TerminationGracePeriodSeconds == nil {
		result.TerminationGracePeriodSeconds = new(int64)
		*result.TerminationGracePeriodSeconds = corev1.DefaultTerminationGracePeriodSeconds
	}
	if result.SchedulerName == "" {
		result.SchedulerName = corev1.DefaultSchedulerName
	}
	if result.EnableServiceLinks == nil {
		result.EnableServiceLinks = new(bool)
		*result.EnableServiceLinks = corev1.DefaultEnableServiceLinks
	}
	if result.ServiceAccountName == "" {
		result.ServiceAccountName = "default"
	}
	// The core API's internal-to-v1 conversion mirrors the effective service
	// account into this deprecated alias on Pods. Embedded PodSpecs in CRDs do
	// not receive that conversion, so compare the canonical field once here.
	result.DeprecatedServiceAccount = result.ServiceAccountName
	for i := range result.InitContainers {
		normalizeContainer(&result.InitContainers[i])
	}
	for i := range result.Containers {
		normalizeContainer(&result.Containers[i])
	}
	result.Tolerations = explicitTolerations(result.Tolerations)
	return result
}

func normalizeContainer(container *corev1.Container) {
	if container == nil {
		return
	}
	if container.ImagePullPolicy == "" {
		container.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if container.TerminationMessagePath == "" {
		container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if container.TerminationMessagePolicy == "" {
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	for i := range container.Ports {
		if container.Ports[i].Protocol == "" {
			container.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
	for i := range container.Env {
		fieldRef := container.Env[i].ValueFrom
		if fieldRef != nil && fieldRef.FieldRef != nil && fieldRef.FieldRef.APIVersion == "" {
			fieldRef.FieldRef.APIVersion = "v1"
		}
	}
}

func explicitTolerations(tolerations []corev1.Toleration) []corev1.Toleration {
	result := make([]corev1.Toleration, 0, len(tolerations))
	for i := range tolerations {
		toleration := tolerations[i]
		if toleration.Operator == corev1.TolerationOpExists && toleration.Effect == corev1.TaintEffectNoExecute &&
			(toleration.Key == corev1.TaintNodeNotReady || toleration.Key == corev1.TaintNodeUnreachable) {
			continue
		}
		result = append(result, toleration)
	}
	return result
}

func stripInjectedDurableWorkspaceVolume(expected, actual []corev1.Volume, injectedClaimName string) []corev1.Volume {
	for _, volume := range expected {
		if volume.Name == durableVolumeName {
			return actual
		}
	}
	result := make([]corev1.Volume, 0, len(actual))
	for _, volume := range actual {
		// A read-only PVC source would mount the active repository workspace
		// read-only despite the writable mount the template declares; it is
		// retained so the spec comparison fails instead of Serving a
		// workspace whose clone/edit/commit operations cannot work.
		if volume.Name == durableVolumeName && volume.PersistentVolumeClaim != nil &&
			!volume.PersistentVolumeClaim.ReadOnly &&
			injectedClaimName != "" && volume.PersistentVolumeClaim.ClaimName == injectedClaimName {
			continue
		}
		result = append(result, volume)
	}
	return result
}
