package fake

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strconv"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const podInstanceLabel = "fake.workspace.orka.ai/instance"
const podRequestAnnotation = "fake.workspace.orka.ai/request-revision"

func validatePodRequest(request workspaceprovider.WorkloadRequest) error {
	if request.Runtime == nil {
		return nil
	}
	runtime := request.Runtime
	if runtime.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return fmt.Errorf("fake runtime requires restartPolicy Never; another process needs a new workload sequence")
	}
	if runtime.BootstrapPort < 1 || runtime.BootstrapPort > 65535 {
		return fmt.Errorf("runtime bootstrap port is invalid")
	}
	for _, container := range runtime.Template.Spec.Containers {
		if container.Name != runtime.ContainerName || len(container.Ports) == 0 {
			continue
		}
		found := false
		for _, port := range container.Ports {
			if port.ContainerPort == runtime.BootstrapPort && (port.Protocol == "" || port.Protocol == corev1.ProtocolTCP) {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("runtime bootstrap port is not declared by the supervisor container")
		}
	}
	return nil
}
func desiredPod(record *journalRecord) *corev1.Pod {
	template := record.Request.Runtime.Template
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: record.Pod.Namespace, Name: record.Pod.Name, Labels: map[string]string{}, Annotations: map[string]string{}}, Spec: *template.Spec.DeepCopy()}
	for k, v := range template.Labels {
		pod.Labels[k] = v
	}
	for k, v := range template.Annotations {
		pod.Annotations[k] = v
	}
	pod.Labels[podInstanceLabel] = record.Observation.Identity.InstanceID
	pod.Annotations[podRequestAnnotation] = record.Request.Revision
	if pod.Namespace == record.Request.Key.Namespace {
		controller := true
		pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: workspacev1alpha1.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: record.Request.Key.Name, UID: record.Request.Key.WorkspaceUID, Controller: &controller}}
	}
	return pod
}
func (d *Lifecycle) pod(ctx context.Context, record *journalRecord) (*corev1.Pod, error) {
	if record.Pod == nil {
		return nil, fmt.Errorf("runtime journal has no Pod intent")
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Pod.Namespace, Name: record.Pod.Name}, pod); err != nil {
		return nil, err
	}
	if record.Pod.UID != "" && pod.UID != record.Pod.UID {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	expected := desiredPod(record)
	if !reflect.DeepEqual(pod.Labels, expected.Labels) || !reflect.DeepEqual(pod.Annotations, expected.Annotations) || !apiequality.Semantic.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) {
		return nil, fmt.Errorf("runtime Pod ownership or request metadata changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	actualSpec := *pod.Spec.DeepCopy()
	expectedSpec := *expected.Spec.DeepCopy()
	if expectedSpec.NodeName == "" {
		actualSpec.NodeName = ""
	}
	// Priority admission derives these values from a requested or global
	// default class. Explicit template fields remain part of the frozen intent.
	if expectedSpec.Priority == nil {
		actualSpec.Priority = nil
	}
	if expectedSpec.PreemptionPolicy == nil {
		actualSpec.PreemptionPolicy = nil
	}
	if expectedSpec.PriorityClassName == "" {
		actualSpec.PriorityClassName = ""
	}
	normalizePodSpec(&actualSpec)
	normalizePodSpec(&expectedSpec)
	if !apiequality.Semantic.DeepEqual(actualSpec, expectedSpec) {
		return nil, fmt.Errorf("runtime Pod differs from its admitted template")
	}
	for _, status := range append(append([]corev1.ContainerStatus(nil), pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
		if status.RestartCount > 0 {
			return nil, fmt.Errorf("runtime Pod restarted a process: %w", workspaceprovider.ErrStaleIdentity)
		}
	}
	return pod, nil
}
func (d *Lifecycle) ensurePod(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (workspaceprovider.AllocationObservation, error) {
	pod, err := d.pod(ctx, record)
	if apierrors.IsNotFound(err) {
		if record.Pod.UID != "" {
			return pendingObservation(record), nil
		}
		if record.CreateIssued {
			return pendingObservation(record), fmt.Errorf("runtime Pod creation is unresolved; absence cannot authorize replay: %w", workspaceprovider.ErrStaleIdentity)
		}
		record.CreateIssued = true
		if err := d.save(ctx, cm, record); err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
		pod = desiredPod(record)
		if err := d.client.Create(ctx, pod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return pendingObservation(record), nil
			}
			if apierrors.IsInvalid(err) || apierrors.IsForbidden(err) || apierrors.IsBadRequest(err) || apierrors.IsUnauthorized(err) || apierrors.IsNotFound(err) || apierrors.IsTooManyRequests(err) {
				record.CreateIssued = false
				if saveErr := d.save(ctx, cm, record); saveErr != nil {
					return workspaceprovider.AllocationObservation{}, saveErr
				}
			}
			return workspaceprovider.AllocationObservation{}, err
		}
	} else if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if pod.UID == "" {
		return pendingObservation(record), fmt.Errorf("API server has not assigned runtime Pod UID")
	}
	if record.Pod.UID == "" {
		record.Pod.UID = pod.UID
		if err := d.save(ctx, cm, record); err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
	}
	observed, err := d.observePod(ctx, record)
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	record.Observation = observed
	if err := d.save(ctx, cm, record); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	return observed, nil
}
func pendingObservation(record *journalRecord) workspaceprovider.AllocationObservation {
	observed := record.Observation
	observed.State = workspaceprovider.AllocationPending
	observed.Startup = nil
	return observed
}
func (d *Lifecycle) observePod(ctx context.Context, record *journalRecord) (workspaceprovider.AllocationObservation, error) {
	pod, err := d.pod(ctx, record)
	if apierrors.IsNotFound(err) {
		return pendingObservation(record), nil
	}
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if record.Pod.UID == "" || pod.DeletionTimestamp != nil || net.ParseIP(pod.Status.PodIP) == nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return pendingObservation(record), nil
	}
	observed := record.Observation
	observed.State = workspaceprovider.AllocationReady
	observed.Startup = &workspaceprovider.StartupEvidence{ContractVersion: workspaceprovider.LifecycleContractV1, Identity: observed.Identity, Endpoint: "http://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(record.Request.Runtime.BootstrapPort))), Pod: &workspaceprovider.PodReference{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}}
	return observed, nil
}
func (d *Lifecycle) stopPod(ctx context.Context, record *journalRecord) (bool, error) {
	if record.Pod == nil {
		return false, fmt.Errorf("runtime journal has no Pod intent")
	}
	pod := &corev1.Pod{}
	err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Pod.Namespace, Name: record.Pod.Name}, pod)
	if apierrors.IsNotFound(err) {
		if record.CreateIssued && record.Pod.UID == "" {
			return false, fmt.Errorf("runtime Pod creation is unresolved; exact termination cannot be proved")
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if record.Pod.UID == "" {
		return false, fmt.Errorf("runtime Pod UID was not committed before retirement; recover the creation outcome first")
	}
	if pod.UID != record.Pod.UID {
		return false, workspaceprovider.ErrStaleIdentity
	}
	// Cleanup uses the durable UID even if admission or Pod contents changed.
	if pod.DeletionTimestamp == nil {
		uid := record.Pod.UID
		if err := d.client.Delete(ctx, pod, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return false, nil
}

// Normalize only documented API defaults and scheduler-assigned fields. Extra
// containers, mounts, privileges and mutated request fields still fail equality.
func normalizePodSpec(spec *corev1.PodSpec) {
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSClusterFirst
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = corev1.DefaultSchedulerName
	}
	if spec.ServiceAccountName == "" {
		spec.ServiceAccountName = "default"
	}
	if spec.DeprecatedServiceAccount == "" {
		spec.DeprecatedServiceAccount = spec.ServiceAccountName
	}
	if spec.TerminationGracePeriodSeconds == nil {
		value := int64(30)
		spec.TerminationGracePeriodSeconds = &value
	}
	if spec.EnableServiceLinks == nil {
		value := true
		spec.EnableServiceLinks = &value
	}
	if spec.PreemptionPolicy == nil {
		value := corev1.PreemptLowerPriority
		spec.PreemptionPolicy = &value
	}
	if spec.Priority == nil {
		value := int32(0)
		spec.Priority = &value
	}
	if spec.SecurityContext != nil && reflect.DeepEqual(*spec.SecurityContext, corev1.PodSecurityContext{}) {
		spec.SecurityContext = nil
	}
	tolerations := make([]corev1.Toleration, 0, len(spec.Tolerations))
	for _, value := range spec.Tolerations {
		if value.Operator == corev1.TolerationOpExists && value.Effect == corev1.TaintEffectNoExecute && value.TolerationSeconds != nil && *value.TolerationSeconds == 300 && (value.Key == corev1.TaintNodeNotReady || value.Key == corev1.TaintNodeUnreachable) {
			continue
		}
		tolerations = append(tolerations, value)
	}
	if len(tolerations) == 0 {
		tolerations = nil
	}
	spec.Tolerations = tolerations
	for i := range spec.Containers {
		normalizeContainer(&spec.Containers[i])
	}
	for i := range spec.InitContainers {
		normalizeContainer(&spec.InitContainers[i])
	}
	for i := range spec.Volumes {
		volume := &spec.Volumes[i]
		mode := int32(0644)
		if volume.ConfigMap != nil && volume.ConfigMap.DefaultMode == nil {
			volume.ConfigMap.DefaultMode = &mode
		}
		if volume.Projected != nil && volume.Projected.DefaultMode == nil {
			volume.Projected.DefaultMode = &mode
		}
		if volume.DownwardAPI != nil && volume.DownwardAPI.DefaultMode == nil {
			volume.DownwardAPI.DefaultMode = &mode
		}
	}
}
func normalizeContainer(container *corev1.Container) {
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
		source := container.Env[i].ValueFrom
		if source != nil && source.FieldRef != nil && source.FieldRef.APIVersion == "" {
			source.FieldRef.APIVersion = "v1"
		}
	}
}
