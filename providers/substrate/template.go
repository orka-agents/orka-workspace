package substrate

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"slices"
	"strings"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const durableVolumeName = "orka-workspace"
const durableMountPath = "/durable/orka-workspace"
const identityVolumeName = "orka-substrate-identity"
const identityMountPath = "/run/orka-substrate-identity"

func validateRequest(request sdk.WorkloadRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Runtime == nil || len(request.Runtime.Template.Spec.Containers) != 1 || len(request.Runtime.Template.Spec.InitContainers) != 0 || request.Runtime.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return fmt.Errorf("native runtime requires one supervisor and restartPolicy Never")
	}
	if request.Runtime.NetworkPolicy == nil {
		return fmt.Errorf("native runtime requires admitted network rules")
	}
	if err := validateNativeNetworkPolicy(request.Runtime); err != nil {
		return err
	}
	for _, feature := range request.Runtime.RequiredFeatures {
		if feature != api.WorkspaceFeatureACPRuntime && feature != api.WorkspaceFeatureNativeProcess && feature != api.WorkspaceFeatureSuspend && feature != api.WorkspaceFeatureCheckpoint && feature != api.WorkspaceFeatureRestore {
			return fmt.Errorf("native runtime feature %s is unsupported", feature)
		}
	}
	return nil
}

func templateDigest(template *pb.ActorTemplate) (string, error) {
	copy := proto.Clone(template).(*pb.ActorTemplate)
	copy.Metadata = &pb.ResourceMetadata{Atespace: template.GetMetadata().GetAtespace(), Name: template.GetMetadata().GetName()}
	copy.Status = nil
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(copy)
	return digest(data), err
}

func (d *Lifecycle) compileTemplate(ctx context.Context, record *journalRecord, name string) error {
	base, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: record.Atespace, Name: name}})
	if err != nil {
		return err
	}
	if base.GetMetadata().GetUid() == "" || base.GetSandboxConfig().GetSandboxClass() != pb.SandboxClass_SANDBOX_CLASS_GVISOR || base.GetSandboxConfig().GetConfigName() == "" || base.GetSnapshotsConfig().GetStorageLocation() == "" {
		return fmt.Errorf("native infrastructure requires exact template identity, explicit gVisor, and snapshot storage")
	}
	match := base.GetWorkerSelector().GetMatchLabels()
	if len(match) == 0 {
		return fmt.Errorf("native template must select exactly one WorkerPool")
	}
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1alpha1")
	pools.SetKind("WorkerPoolList")
	if err := d.client.List(ctx, pools, client.MatchingLabels(match)); err != nil {
		return err
	}
	pools.Items = slices.DeleteFunc(pools.Items, func(pool unstructured.Unstructured) bool {
		_, allocation := pool.GetLabels()[workerAllocationLabel]
		_, instance := pool.GetLabels()[workerInstanceLabel]
		return allocation || instance
	})
	if len(pools.Items) != 1 || pools.Items[0].GetUID() == "" {
		return fmt.Errorf("native selector must identify one exact WorkerPool")
	}
	record.Placement = api.PodReference{Namespace: pools.Items[0].GetNamespace(), Name: pools.Items[0].GetName(), UID: pools.Items[0].GetUID()}
	compiled := proto.Clone(base).(*pb.ActorTemplate)
	compiled.Metadata = &pb.ResourceMetadata{Atespace: record.Atespace, Name: record.Template.Name}
	compiled.Status = nil
	compiled.SnapshotsConfig.OnPause = pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	compiled.SnapshotsConfig.OnCommit = pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	compiled.SnapshotsConfig.OnResume = &pb.OnResumeConfig{FromData: pb.ResumeSource_RESUME_SOURCE_COLD_BOOT}
	for _, volume := range compiled.Volumes {
		if volume.GetName() == durableVolumeName || volume.GetName() == identityVolumeName {
			return fmt.Errorf("native infrastructure uses a reserved runtime volume")
		}
	}
	container, err := compileContainer(record)
	if err != nil {
		return err
	}
	resources, err := compileResources(record.Request.Runtime.Template.Spec.Containers[0].Resources)
	if err != nil {
		return err
	}
	// gVisor applies limits to the entire Actor, not its individual containers.
	if resources != nil {
		compiled.Resources = resources
	}
	if record.SuspendEnabled {
		compiled.Volumes = append(compiled.Volumes, &pb.Volume{Name: durableVolumeName, DurableDir: &pb.DurableDirVolumeSource{}})
	}
	compiled.Volumes = append(compiled.Volumes, &pb.Volume{Name: identityVolumeName, SystemInfo: &pb.SystemInfoVolumeSource{DataSources: []*pb.SystemInfoDataSource{{ActorMetadata: &pb.ActorMetadataDataSource{Items: []*pb.ActorMetadataItem{
		{Field: pb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"}, {Field: pb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "name"}, {Field: pb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "uid"},
	}}}}}})
	compiled.Containers = []*pb.Container{container}
	record.TemplateSpec = compiled
	record.Template.Digest, err = templateDigest(compiled)
	return err
}

func compileContainer(record *journalRecord) (*pb.Container, error) {
	if err := validateNativeScheduling(record.Request.Runtime.Template.Spec); err != nil {
		return nil, err
	}
	for _, volume := range record.Request.Runtime.Template.Spec.Volumes {
		if volume.EmptyDir != nil {
			if volume.EmptyDir.SizeLimit != nil && !volume.EmptyDir.SizeLimit.IsZero() {
				return nil, fmt.Errorf("native emptyDir quota for %s is unsupported", volume.Name)
			}
			return nil, fmt.Errorf("native emptyDir volume %s is unsupported", volume.Name)
		}
	}
	container := record.Request.Runtime.Template.Spec.Containers[0]
	securityContext, err := compileSecurityContext(record.Request.Runtime.Template.Spec.SecurityContext, container.SecurityContext)
	if err != nil {
		return nil, err
	}
	if len(container.Command) == 0 {
		return nil, fmt.Errorf("native supervisor requires an explicit command")
	}
	for _, values := range [][]string{container.Command, container.Args} {
		for _, value := range values {
			if hasKubernetesExpansion(value) {
				return nil, fmt.Errorf("native command/argument expansion is unsupported")
			}
		}
	}
	if container.WorkingDir != "" && (!path.IsAbs(container.WorkingDir) || strings.ContainsRune(container.WorkingDir, 0)) {
		return nil, fmt.Errorf("native working directory must be an absolute path")
	}
	args := []string{`chmod 0755 /; exec "$@"`, "orka-substrate-init"}
	args = append(args, container.Command...)
	args = append(args, container.Args...)
	compiled := &pb.Container{Name: container.Name, Image: container.Image, Command: []string{"/bin/sh", "-ec"}, Args: args, Readyz: &pb.ContainerReadyz{HttpGet: &pb.HTTPGetAction{Path: "/v2/health", Port: 80}, TimeoutSeconds: 120}, SecurityContext: securityContext}
	seen := map[string]bool{}
	for _, env := range container.Env {
		// The native DTO treats values literally. Kubernetes expands $(NAME)
		// using earlier variables and reduces $$ escapes, so these expressions
		// must be resolved before admission to this backend.
		if hasKubernetesExpansion(env.Value) {
			return nil, fmt.Errorf("native environment expansion for %s is unsupported", env.Name)
		}
		// Preserve the pinned native compiler's fixed ephemeral session/broker
		// defaults. Actor identity uses SystemInfo, not a Kubernetes namespace.
		if env.Name == "ORKA_ACP_SESSION_BASE_DIR" || env.Name == "ORKA_ACP_MCP_BROKER_URL" || env.Name == "ORKA_ACP_POD_NAMESPACE" {
			continue
		}
		if env.Name == "" || seen[env.Name] || len(env.Name) > 256 || len(env.Value) > 32768 {
			return nil, fmt.Errorf("native environment is invalid")
		}
		seen[env.Name] = true
		value := env.Value
		if env.ValueFrom != nil {
			if env.ValueFrom.FieldRef == nil {
				return nil, fmt.Errorf("native environment must be literal or an admitted identity field")
			}
			switch env.ValueFrom.FieldRef.FieldPath {
			case "metadata.uid":
				value = record.Observation.Identity.InstanceID
			case "metadata.name":
				value = record.Actor.Name
			case "metadata.namespace":
				value = record.Request.Runtime.Template.Namespace
			default:
				return nil, fmt.Errorf("native downward field is unsupported")
			}
		}
		if env.Name == "ORKA_ACP_LISTEN_ADDRESS" {
			value = ":80"
		}
		compiled.Env = append(compiled.Env, &pb.EnvVar{Name: env.Name, Value: value})
	}
	if len(compiled.Env) > 32 {
		return nil, fmt.Errorf("native environment exceeds upstream limit")
	}
	if record.SuspendEnabled {
		if !slices.ContainsFunc(compiled.Env, func(e *pb.EnvVar) bool {
			return e.Name == "ORKA_ACP_DURABLE_WORKSPACE_DIR" && e.Value == durableMountPath
		}) || !slices.ContainsFunc(compiled.Env, func(e *pb.EnvVar) bool {
			return e.Name == "ORKA_ACP_DURABLE_WORKSPACE_KEY" && e.Value == "shared"
		}) || !slices.ContainsFunc(container.VolumeMounts, func(m corev1.VolumeMount) bool {
			return m.Name == durableVolumeName && m.MountPath == durableMountPath && !m.ReadOnly && m.SubPath == "" && m.SubPathExpr == ""
		}) {
			return nil, fmt.Errorf("data-only native profile requires admitted durable workspace mount and environment")
		}
		compiled.Args[0] = `chmod 0755 /; chmod 0711 /durable /durable/orka-workspace; exec "$@"`
		compiled.VolumeMounts = append(compiled.VolumeMounts, &pb.VolumeMount{Name: durableVolumeName, MountPath: durableMountPath})
	}
	for _, mount := range container.VolumeMounts {
		if mount.Name == durableVolumeName && record.SuspendEnabled {
			continue
		}
		return nil, fmt.Errorf("native mount %s is not supported", mount.Name)
	}
	if container.WorkingDir != "" {
		// The native DTO fixes cwd to /. Pass the admitted directory as data,
		// then change cwd before exec without interpreting shell metacharacters.
		compiled.Args[0] = strings.TrimSuffix(compiled.Args[0], `exec "$@"`) + `cd "$1" || exit "$?"; shift; exec "$@"`
		compiled.Args = slices.Insert(compiled.Args, 2, container.WorkingDir)
	}
	compiled.VolumeMounts = append(compiled.VolumeMounts, &pb.VolumeMount{Name: identityVolumeName, MountPath: identityMountPath})
	return compiled, nil
}

func hasKubernetesExpansion(value string) bool {
	return strings.Contains(value, "$(") || strings.Contains(value, "$$")
}

// Runtime Pod placement cannot be translated to the native Actor scheduler.
// Its exact operator WorkerPool has a separately pinned scheduling contract.
func validateNativeScheduling(pod corev1.PodSpec) error {
	if len(pod.NodeSelector) != 0 || pod.NodeName != "" || pod.Affinity != nil || len(pod.Tolerations) != 0 || pod.SchedulerName != "" ||
		pod.PriorityClassName != "" || pod.Priority != nil || pod.PreemptionPolicy != nil || pod.RuntimeClassName != nil ||
		len(pod.Overhead) != 0 || len(pod.TopologySpreadConstraints) != 0 || len(pod.SchedulingGates) != 0 || pod.SchedulingGroup != nil ||
		len(pod.ResourceClaims) != 0 || pod.Resources != nil || pod.OS != nil || len(pod.EvictionResponders) != 0 {
		return fmt.Errorf("native runtime Pod scheduling constraints are unsupported")
	}
	for _, container := range pod.Containers {
		if len(container.Resources.Claims) != 0 {
			return fmt.Errorf("native runtime resource claims are unsupported")
		}
		for _, port := range container.Ports {
			if port.HostPort != 0 || port.HostIP != "" {
				return fmt.Errorf("native runtime host port placement is unsupported")
			}
		}
	}
	return nil
}

// The pinned OCI builder always starts as UID/GID 0 and exposes only capability
// changes. Accept explicit constraints matching those defaults, and reject any
// other supplied setting instead of silently discarding it.
func compileSecurityContext(podContext *corev1.PodSecurityContext, containerContext *corev1.SecurityContext) (*pb.SecurityContext, error) {
	validateIdentity := func(user, group *int64, nonRoot *bool) error {
		if user != nil && *user != 0 || group != nil && *group != 0 || nonRoot != nil && *nonRoot {
			return fmt.Errorf("native process requires UID/GID 0 and cannot require non-root")
		}
		return nil
	}
	if podContext != nil {
		if err := validateIdentity(podContext.RunAsUser, podContext.RunAsGroup, podContext.RunAsNonRoot); err != nil {
			return nil, err
		}
		remaining := podContext.DeepCopy()
		remaining.RunAsUser, remaining.RunAsGroup, remaining.RunAsNonRoot = nil, nil, nil
		if !reflect.DeepEqual(remaining, &corev1.PodSecurityContext{}) {
			return nil, fmt.Errorf("native pod security context contains unsupported settings")
		}
	}
	if containerContext == nil {
		return nil, nil
	}
	if err := validateIdentity(containerContext.RunAsUser, containerContext.RunAsGroup, containerContext.RunAsNonRoot); err != nil {
		return nil, err
	}
	if containerContext.Privileged != nil && *containerContext.Privileged {
		return nil, fmt.Errorf("native privileged process is unsupported")
	}
	if containerContext.ReadOnlyRootFilesystem != nil && *containerContext.ReadOnlyRootFilesystem {
		return nil, fmt.Errorf("native read-only root filesystem is unsupported")
	}
	remaining := containerContext.DeepCopy()
	remaining.RunAsUser, remaining.RunAsGroup, remaining.RunAsNonRoot = nil, nil, nil
	remaining.Privileged, remaining.ReadOnlyRootFilesystem, remaining.Capabilities = nil, nil, nil
	if !reflect.DeepEqual(remaining, &corev1.SecurityContext{}) {
		return nil, fmt.Errorf("native container security context contains unsupported settings")
	}
	if containerContext.Capabilities == nil {
		return nil, nil
	}
	compiled := &pb.SecurityContext{Capabilities: &pb.Capabilities{}}
	for _, list := range []struct {
		values []corev1.Capability
		target *[]string
		drop   bool
	}{
		{values: containerContext.Capabilities.Add, target: &compiled.Capabilities.Add},
		{values: containerContext.Capabilities.Drop, target: &compiled.Capabilities.Drop, drop: true},
	} {
		if len(list.values) > 64 {
			return nil, fmt.Errorf("native capabilities exceed the upstream limit")
		}
		seen := map[string]bool{}
		for _, capability := range list.values {
			name := string(capability)
			if name == "ALL" && !list.drop || len(name) > 63 || strings.HasPrefix(name, "CAP_") || !nativeCapabilityName.MatchString(name) || seen[name] {
				return nil, fmt.Errorf("native capability %s is unsupported", name)
			}
			seen[name] = true
			*list.target = append(*list.target, name)
		}
	}
	return compiled, nil
}

var nativeCapabilityName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func compileResources(requirements corev1.ResourceRequirements) (*pb.Resources, error) {
	for name, quantity := range requirements.Requests {
		if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
			return nil, fmt.Errorf("native resource request %s is unsupported", name)
		}
		if quantity.Sign() < 0 {
			return nil, fmt.Errorf("native resource request must not be negative")
		}
		if quantity.IsZero() {
			continue
		}
		limit, bounded := requirements.Limits[name]
		// The native scheduler reserves the full Actor limit. This covers a
		// smaller request without inventing an unadmitted maximum for request-
		// only resources, which the pinned backend cannot represent.
		if !bounded || limit.Cmp(quantity) < 0 {
			return nil, fmt.Errorf("native resource request %s requires an admitted limit at least as large", name)
		}
	}
	var resources *pb.Resources
	for name, quantity := range requirements.Limits {
		if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
			return nil, fmt.Errorf("native resource %s is unsupported", name)
		}
		if quantity.Sign() <= 0 {
			return nil, fmt.Errorf("native resource must be positive")
		}
		if resources == nil {
			resources = &pb.Resources{}
		}
		resources.Limits = append(resources.Limits, &pb.Limits{Name: string(name), Quantity: quantity.String()})
	}
	slices.SortFunc(resources.GetLimits(), func(a, b *pb.Limits) int { return strings.Compare(a.Name, b.Name) })
	return resources, nil
}

func (d *Lifecycle) verifyPlacement(ctx context.Context, record *journalRecord) error {
	pool := &unstructured.Unstructured{}
	pool.SetAPIVersion("ate.dev/v1alpha1")
	pool.SetKind("WorkerPool")
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.Placement.Name}, pool); err != nil {
		return err
	}
	if pool.GetUID() != record.Placement.UID || pool.GetDeletionTimestamp() != nil {
		return sdk.ErrStaleIdentity
	}
	if len(record.SourceSelector) == 0 {
		return sdk.ErrStaleIdentity
	}
	for key, value := range record.SourceSelector {
		if key == workerAllocationLabel || key == workerInstanceLabel {
			return sdk.ErrStaleIdentity
		}
		if pool.GetLabels()[key] != value {
			return sdk.ErrStaleIdentity
		}
	}
	return nil
}

func compatibleRestore(previous, next *pb.ActorTemplate) bool {
	normalize := func(input *pb.ActorTemplate) *pb.ActorTemplate {
		out := proto.Clone(input).(*pb.ActorTemplate)
		out.Metadata = nil
		out.Status = nil
		if out.WorkerSelector != nil {
			delete(out.WorkerSelector.MatchLabels, workerAllocationLabel)
			delete(out.WorkerSelector.MatchLabels, workerInstanceLabel)
		}
		for _, container := range out.Containers {
			for _, env := range container.Env {
				switch env.Name {
				case "ORKA_ACP_POD_UID", "ORKA_ACP_POD_NAME", "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE", "ORKA_ACP_CREDENTIAL_BOOTSTRAP_PUBLIC_KEY", "ORKA_ACP_RUNTIME_POOL_UID", "ORKA_ACP_RUNTIME_POOL_GENERATION", "ORKA_ACP_CONTROLLER_EPOCH", "ORKA_ACP_PROVIDER_TOKEN_GENERATION":
					env.Value = ""
				}
			}
		}
		return out
	}
	return proto.Equal(normalize(previous), normalize(next))
}

func anchorOwner(record *journalRecord) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: record.Anchor.Name, UID: types.UID(record.Anchor.UID)}
}
