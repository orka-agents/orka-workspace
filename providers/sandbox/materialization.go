package sandbox

import (
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"maps"
	"reflect"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
	"strings"
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
	actualSpec.Tolerations = stripDefaultTolerationInjection(expectedSpec.Tolerations, actualSpec.Tolerations)

	// These fields are derived from cluster scheduling/admission state rather
	// than from the provider-visible Sandbox template.
	expectedSpec.NodeName, actualSpec.NodeName = "", ""
	expectedSpec.Overhead, actualSpec.Overhead = nil, nil
	// Priority admission fills omitted values from the requested or global
	// default class. Explicit values remain part of the frozen template.
	if expectedSpec.Priority == nil {
		actualSpec.Priority = nil
	}
	if expectedSpec.PreemptionPolicy == nil {
		actualSpec.PreemptionPolicy = nil
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
		result.ServiceAccountName = result.DeprecatedServiceAccount
		if result.ServiceAccountName == "" {
			result.ServiceAccountName = "default"
		}
	}
	// Pod defaulting gives the canonical field precedence over the deprecated
	// alias, then ServiceAccount admission defaults an omitted account. Embedded
	// PodSpecs in CRDs do not receive these passes.
	result.DeprecatedServiceAccount = result.ServiceAccountName
	for i := range result.InitContainers {
		normalizeContainer(&result.InitContainers[i])
	}
	for i := range result.Containers {
		normalizeContainer(&result.Containers[i])
	}
	for i := range result.Volumes {
		volume := &result.Volumes[i]
		if volume.DownwardAPI != nil {
			if volume.DownwardAPI.DefaultMode == nil {
				mode := corev1.DownwardAPIVolumeSourceDefaultMode
				volume.DownwardAPI.DefaultMode = &mode
			}
			normalizeDownwardAPIItems(volume.DownwardAPI.Items)
		}
		if volume.Projected != nil {
			if volume.Projected.DefaultMode == nil {
				mode := corev1.ProjectedVolumeSourceDefaultMode
				volume.Projected.DefaultMode = &mode
			}
			for j := range volume.Projected.Sources {
				if downwardAPI := volume.Projected.Sources[j].DownwardAPI; downwardAPI != nil {
					normalizeDownwardAPIItems(downwardAPI.Items)
				}
			}
		}
	}
	if len(result.Tolerations) == 0 {
		result.Tolerations = nil
	}
	return result
}

func normalizeDownwardAPIItems(items []corev1.DownwardAPIVolumeFile) {
	for i := range items {
		if fieldRef := items[i].FieldRef; fieldRef != nil && fieldRef.APIVersion == "" {
			fieldRef.APIVersion = "v1"
		}
	}
}

func normalizeContainer(container *corev1.Container) {
	if container == nil {
		return
	}
	if container.ImagePullPolicy == "" {
		container.ImagePullPolicy = corev1.PullIfNotPresent
		// Kubernetes retains an explicit latest tag even beside a digest. All
		// admitted images are digest-pinned, so an omitted tag stays IfNotPresent.
		imageName, _, _ := strings.Cut(container.Image, "@")
		if strings.HasSuffix(imageName, ":latest") {
			container.ImagePullPolicy = corev1.PullAlways
		}
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
	normalizeProbe(container.LivenessProbe)
	normalizeProbe(container.ReadinessProbe)
	normalizeProbe(container.StartupProbe)
	if container.Lifecycle != nil {
		if container.Lifecycle.PostStart != nil {
			normalizeHTTPGetAction(container.Lifecycle.PostStart.HTTPGet)
		}
		if container.Lifecycle.PreStop != nil {
			normalizeHTTPGetAction(container.Lifecycle.PreStop.HTTPGet)
		}
	}
}

// Match Kubernetes v1.37 core/v1 probe and action defaults on both copies;
// explicit thresholds and handlers remain part of the admitted template.
func normalizeProbe(probe *corev1.Probe) {
	if probe == nil {
		return
	}
	if probe.TimeoutSeconds == 0 {
		probe.TimeoutSeconds = 1
	}
	if probe.PeriodSeconds == 0 {
		probe.PeriodSeconds = 10
	}
	if probe.SuccessThreshold == 0 {
		probe.SuccessThreshold = 1
	}
	if probe.FailureThreshold == 0 {
		probe.FailureThreshold = 3
	}
	normalizeHTTPGetAction(probe.HTTPGet)
	if probe.GRPC != nil && probe.GRPC.Service == nil {
		probe.GRPC.Service = new(string)
	}
}

func normalizeHTTPGetAction(action *corev1.HTTPGetAction) {
	if action == nil {
		return
	}
	if action.Path == "" {
		action.Path = "/"
	}
	if action.Scheme == "" {
		action.Scheme = corev1.URISchemeHTTP
	}
	// Nil means HTTP/1.1 even when H2CContainerProbe is disabled. When the
	// gate is enabled, Pod defaulting also writes this value explicitly.
	if action.Protocol == nil {
		action.Protocol = new(corev1.HTTPProtocolHTTP1)
	}
}

func stripDefaultTolerationInjection(expected, actual []corev1.Toleration) []corev1.Toleration {
	var result []corev1.Toleration
	for _, toleration := range actual {
		if toleration.Operator == corev1.TolerationOpExists && toleration.Effect == corev1.TaintEffectNoExecute &&
			toleration.Value == "" && toleration.TolerationSeconds != nil && *toleration.TolerationSeconds == 300 &&
			(toleration.Key == corev1.TaintNodeNotReady || toleration.Key == corev1.TaintNodeUnreachable) {
			// DefaultTolerationSeconds only injects a taint's default when no
			// declared toleration has that key (or all keys) and NoExecute (or
			// all effects). A frozen toleration must remain compared exactly.
			declared := false
			for _, frozen := range expected {
				if (frozen.Key == toleration.Key || frozen.Key == "") && (frozen.Effect == corev1.TaintEffectNoExecute || frozen.Effect == "") {
					declared = true
					break
				}
			}
			if !declared {
				continue
			}
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
