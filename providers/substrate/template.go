package substrate

import (
	"context"
	"fmt"
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
	for _, feature := range request.Runtime.RequiredFeatures {
		if feature != api.WorkspaceFeatureACPRuntime && feature != api.WorkspaceFeatureSuspend && feature != api.WorkspaceFeatureCheckpoint && feature != api.WorkspaceFeatureRestore {
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
	container := record.Request.Runtime.Template.Spec.Containers[0]
	if len(container.Command) == 0 {
		return nil, fmt.Errorf("native supervisor requires an explicit command")
	}
	args := []string{`chmod 0755 /; exec "$@"`, "orka-substrate-init"}
	args = append(args, container.Command...)
	args = append(args, container.Args...)
	compiled := &pb.Container{Name: container.Name, Image: container.Image, Command: []string{"/bin/sh", "-ec"}, Args: args, Readyz: &pb.ContainerReadyz{HttpGet: &pb.HTTPGetAction{Path: "/v2/health", Port: 80}, TimeoutSeconds: 120}, SecurityContext: &pb.SecurityContext{Capabilities: &pb.Capabilities{Drop: []string{"ALL"}, Add: []string{"CHOWN", "KILL", "SETGID", "SETUID"}}}}
	seen := map[string]bool{}
	for _, env := range container.Env {
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
		if mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" || !slices.ContainsFunc(record.Request.Runtime.Template.Spec.Volumes, func(v corev1.Volume) bool { return v.Name == mount.Name && v.EmptyDir != nil }) {
			return nil, fmt.Errorf("native mount %s is not supported", mount.Name)
		}
	}
	compiled.VolumeMounts = append(compiled.VolumeMounts, &pb.VolumeMount{Name: identityVolumeName, MountPath: identityMountPath})
	return compiled, nil
}

func compileResources(requirements corev1.ResourceRequirements) (*pb.Resources, error) {
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
