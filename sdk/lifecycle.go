package workspaceprovider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
)

// LifecycleContractV1 is separate from the stored workspace schema version.
// Phase 1 defines this Go contract; publishing it in workspace status requires
// the Phase 2 schema and secure-startup integration.
const LifecycleContractV1 = "orka.workspace.lifecycle.v1"

var (
	ErrNotFound             = errors.New("allocation not found")
	ErrStaleIdentity        = errors.New("allocation or instance identity is stale")
	ErrRequestConflict      = errors.New("immutable workload request changed")
	ErrInstanceRunning      = errors.New("instance has not stopped")
	ErrWorkspaceNotAdmitted = errors.New("workspace is not currently admitted by Orka core")
)

// AllocationKey binds operations to one workspace and installed provider. Names
// alone never authorize operations on a replacement object.
type AllocationKey struct {
	Namespace    string    `json:"namespace"`
	Name         string    `json:"name"`
	WorkspaceUID types.UID `json:"workspaceUID"`
	ProviderUID  types.UID `json:"providerUID"`
}

func (k AllocationKey) Validate() error {
	if k.Namespace == "" || k.Name == "" || k.WorkspaceUID == "" || k.ProviderUID == "" {
		return fmt.Errorf("namespace, name, workspace UID and provider UID are required")
	}
	return nil
}

// WorkloadRequest is immutable allocation intent. It contains public startup
// configuration only. Orka delivers credentials after verifying the exact
// reported instance, using the separate one-time bootstrap protocol.
type WorkloadRequest struct {
	Key               AllocationKey                             `json:"key"`
	Revision          string                                    `json:"revision"`
	Image             string                                    `json:"image"`
	Command           []string                                  `json:"command,omitempty"`
	Args              []string                                  `json:"args,omitempty"`
	Resources         corev1.ResourceRequirements               `json:"resources,omitempty"`
	ParametersRef     *workspacev1alpha1.TypedObjectReference   `json:"parametersRef,omitempty"`
	ParametersBinding *workspacev1alpha1.ImmutableObjectBinding `json:"parametersBinding,omitempty"`
	Runtime           *RuntimeWorkload                          `json:"runtime,omitempty"`
}

// RuntimeWorkload freezes the public supervisor definition used by RuntimePool
// materialization. Providers translate this definition to their backend; Orka
// compares the realized Pod when accessible. The template includes admitted
// storage, isolation, networking and public bootstrap configuration. It must
// never refer to Orka's private bootstrap or runtime credential Secrets.
type RuntimeWorkload struct {
	PoolBinding      workspacev1alpha1.ImmutableObjectBinding      `json:"poolBinding"`
	ClassBinding     workspacev1alpha1.ImmutableObjectBinding      `json:"classBinding"`
	Protocol         string                                        `json:"protocol"`
	ContainerName    string                                        `json:"containerName"`
	Template         corev1.PodTemplateSpec                        `json:"template"`
	RequiredFeatures []workspacev1alpha1.ExecutionWorkspaceFeature `json:"requiredFeatures,omitempty"`
}

// WorkloadRevision hashes all immutable request fields except Revision itself.
func WorkloadRevision(request WorkloadRequest) (string, error) {
	request.Revision = ""
	data, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (r WorkloadRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	parts := strings.Split(r.Image, "@sha256:")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
		return fmt.Errorf("workload image must be pinned by SHA-256 digest")
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return fmt.Errorf("invalid image digest: %w", err)
	}
	if r.ParametersRef != nil && (r.ParametersRef.Group == "" || r.ParametersRef.Kind == "" || r.ParametersRef.Name == "") {
		return fmt.Errorf("parameter reference requires group, kind and name")
	}
	if (r.ParametersRef == nil) != (r.ParametersBinding == nil) {
		return fmt.Errorf("parameter reference and immutable binding must be supplied together")
	}
	if r.ParametersBinding != nil && (r.ParametersBinding.Name != r.ParametersRef.Name || !validBinding(*r.ParametersBinding)) {
		return fmt.Errorf("parameter binding must pin the referenced name, UID, generation and profile hash")
	}
	if r.Runtime != nil {
		if err := r.validateRuntime(); err != nil {
			return err
		}
	}
	revision, err := WorkloadRevision(r)
	if err != nil {
		return err
	}
	if r.Revision != revision {
		return ErrRequestConflict
	}
	return nil
}

func validBinding(binding workspacev1alpha1.ImmutableObjectBinding) bool {
	return binding.Name != "" && binding.UID != "" && binding.Generation > 0 && validDigest(binding.ProfileHash)
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != 71 {
		return false
	}
	_, err := hex.DecodeString(value[7:])
	return err == nil
}

func (r WorkloadRequest) validateRuntime() error {
	runtime := r.Runtime
	if !validBinding(runtime.PoolBinding) || !validBinding(runtime.ClassBinding) || runtime.Protocol != "orka.harness.v2" {
		return fmt.Errorf("runtime requires immutable pool/class bindings and orka.harness.v2")
	}
	pod := runtime.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		return fmt.Errorf("runtime must explicitly disable ServiceAccount token mounting")
	}
	found := false
	for _, container := range append(append([]corev1.Container(nil), pod.InitContainers...), pod.Containers...) {
		if len(container.EnvFrom) != 0 {
			return fmt.Errorf("runtime environment sources must be resolved before allocation")
		}
		for _, env := range container.Env {
			if env.ValueFrom != nil && (env.ValueFrom.SecretKeyRef != nil || env.ValueFrom.ConfigMapKeyRef != nil) {
				return fmt.Errorf("runtime environment cannot resolve mutable configuration or credentials")
			}
		}
	}
	for _, container := range pod.Containers {
		if container.Name != runtime.ContainerName {
			continue
		}
		found = true
		if container.Image != r.Image || !apiequality.Semantic.DeepEqual(container.Command, r.Command) ||
			!apiequality.Semantic.DeepEqual(container.Args, r.Args) || !apiequality.Semantic.DeepEqual(container.Resources, r.Resources) {
			return fmt.Errorf("runtime container must match the frozen image, command, arguments and resources")
		}
	}
	if !found {
		return fmt.Errorf("runtime container is missing from the template")
	}
	for _, volume := range pod.Volumes {
		if volume.Secret != nil {
			return fmt.Errorf("runtime cannot mount credential Secrets before bootstrap")
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil || source.ServiceAccountToken != nil {
					return fmt.Errorf("runtime cannot project credentials before bootstrap")
				}
			}
		}
	}
	return nil
}

// InstanceIdentity is the full fence for one physical incarnation. AllocationID
// may survive a restart; InstanceID must change for every replacement process.
type InstanceIdentity struct {
	AllocationID    string `json:"allocationID"`
	InstanceID      string `json:"instanceID"`
	RequestRevision string `json:"requestRevision"`
}

// PodReference allows Orka to retain read-only UID and materialization checks.
// Providers without Kubernetes Pods leave it nil and supply their own evidence.
type PodReference struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

// StartupEvidence is an infrastructure claim, never runtime admission. Orka
// must still bind bootstrap credentials and probe the authenticated exact fence.
type StartupEvidence struct {
	ContractVersion string           `json:"contractVersion"`
	Identity        InstanceIdentity `json:"identity"`
	Endpoint        string           `json:"endpoint"`
	Pod             *PodReference    `json:"pod,omitempty"`
}

type AllocationState string

const (
	AllocationPending AllocationState = "Pending"
	AllocationReady   AllocationState = "Ready"
	AllocationStopped AllocationState = "Stopped"
	AllocationDeleted AllocationState = "Deleted"
)

type AllocationObservation struct {
	Key         AllocationKey                                    `json:"key"`
	Identity    InstanceIdentity                                 `json:"identity"`
	State       AllocationState                                  `json:"state"`
	Startup     *StartupEvidence                                 `json:"startup,omitempty"`
	Disposition *workspacev1alpha1.ExecutionWorkspaceDisposition `json:"disposition,omitempty"`
}

// ValidateStartup rejects incomplete or mismatched provider claims. It does not
// replace the consumer's Pod, one-time listener, or authenticated fence probes.
func ValidateStartup(request WorkloadRequest, observed AllocationObservation) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if observed.Key != request.Key || observed.Identity.RequestRevision != request.Revision ||
		observed.Identity.AllocationID == "" || observed.Identity.InstanceID == "" {
		return ErrStaleIdentity
	}
	if observed.State != AllocationReady || observed.Startup == nil {
		return fmt.Errorf("startup evidence is not ready")
	}
	evidence := observed.Startup
	if evidence.ContractVersion != LifecycleContractV1 || evidence.Identity != observed.Identity {
		return ErrStaleIdentity
	}
	u, err := url.Parse(evidence.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("startup endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	if evidence.Pod != nil && (evidence.Pod.Namespace == "" || evidence.Pod.Name == "" || evidence.Pod.UID == "") {
		return fmt.Errorf("startup Pod reference requires namespace, name and UID")
	}
	return nil
}

// Lifecycle is derived from RuntimePool workload materialization and cleanup.
// Every method is retry-safe after a lost response. EnsureAllocation persists
// intent before backend effects and never recreates a stopped/deleted request.
// Observe is read-only. StopInstance must reject any mismatched fence. Delete
// requires observed termination and records data disposition before completion.
// Providers keep a deletion tombstone until the owning workspace is finalized.
type Lifecycle interface {
	EnsureAllocation(context.Context, WorkloadRequest) (AllocationObservation, error)
	Observe(context.Context, AllocationKey) (AllocationObservation, error)
	StopInstance(context.Context, AllocationKey, InstanceIdentity) (AllocationObservation, error)
	DeleteAllocation(context.Context, AllocationKey, InstanceIdentity, workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (AllocationObservation, error)
}

// AttachmentController is optional. Acknowledgement must not wait for runtime
// admission; revocation must fence the requested epoch even during an outage.
type AttachmentController interface {
	ActivateAttachment(context.Context, *workspacev1alpha1.ExecutionWorkspace) error
	RevokeAttachment(context.Context, *workspacev1alpha1.ExecutionWorkspace, int64) error
}
