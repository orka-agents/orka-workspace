// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
)

// LifecycleContractV1 is separate from the stored workspace schema version.
const LifecycleContractV1 = "orka.workspace.lifecycle.v1"

var (
	ErrStaleIdentity   = errors.New("allocation or instance identity is stale")
	ErrRequestConflict = errors.New("immutable workload request changed")
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
// +kubebuilder:validation:XValidation:rule="(self.sequence == 1) == !has(self.previousInstance)",message="only the first sequence omits previousInstance"
// +kubebuilder:validation:XValidation:rule="!has(self.retainedData) || (has(self.previousInstance) && self.retainedData.sourceInstance.allocationID == self.previousInstance.allocationID && self.retainedData.sourceInstance.instanceID == self.previousInstance.instanceID && self.retainedData.sourceInstance.requestRevision == self.previousInstance.requestRevision)",message="retained data must belong to the previous instance"
type WorkloadRequest struct {
	// RestoreFrom pins an independently retained Data artifact. The provider
	// acquires its own durable artifact reference before starting the instance.
	// +optional
	RestoreFrom *WorkloadCheckpointReference `json:"restoreFrom,omitempty"`
	// Sequence starts at one and increases only after the previous instance has stopped.
	// +kubebuilder:validation:Minimum=1
	Sequence int64 `json:"sequence"`
	// PreviousInstance fences the exact retired instance when Sequence increases.
	// +optional
	PreviousInstance *InstanceIdentity `json:"previousInstance,omitempty"`
	// RetainedData pins the verified data lineage used for a cold resume.
	// +optional
	RetainedData *RetainedDataReference `json:"retainedData,omitempty"`
	Key          AllocationKey          `json:"key"`
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	Revision          string                      `json:"revision"`
	Image             string                      `json:"image"`
	Command           []string                    `json:"command,omitempty"`
	Args              []string                    `json:"args,omitempty"`
	Resources         corev1.ResourceRequirements `json:"resources,omitempty"`
	ParametersRef     *TypedObjectReference       `json:"parametersRef,omitempty"`
	ParametersBinding *ImmutableObjectBinding     `json:"parametersBinding,omitempty"`
	Runtime           *RuntimeWorkload            `json:"runtime,omitempty"`
}

// WorkloadCheckpointReference carries public checkpoint identity only. Native
// artifact references stay in the provider's private persisted catalog.
type WorkloadCheckpointReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Type=string
	UID types.UID `json:"uid"`
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	Digest string `json:"digest"`
}

// RuntimeWorkload freezes the public supervisor definition used by RuntimePool
// materialization. Providers translate this definition to their backend; Orka
// compares the realized Pod when accessible. The template includes admitted
// storage, isolation, networking and public bootstrap configuration. It must
// never refer to Orka's private bootstrap or runtime credential Secrets.
type RuntimeWorkload struct {
	// NetworkPolicy carries the admitted network permission envelope. Before
	// reporting startup readiness, the provider ensures required isolation
	// directions are applied and no selecting policy grants permissions outside
	// these rules.
	// +optional
	NetworkPolicy *networkingv1.NetworkPolicySpec `json:"networkPolicy,omitempty"`
	// BootstrapPort is the public one-time listener port on the reported instance.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	BootstrapPort    int32                       `json:"bootstrapPort"`
	PoolBinding      ImmutableObjectBinding      `json:"poolBinding"`
	ClassBinding     ImmutableObjectBinding      `json:"classBinding"`
	Protocol         string                      `json:"protocol"`
	ContainerName    string                      `json:"containerName"`
	Template         corev1.PodTemplateSpec      `json:"template"`
	RequiredFeatures []ExecutionWorkspaceFeature `json:"requiredFeatures,omitempty"`
}

// WorkloadRevision hashes all immutable request fields except Revision itself.
func WorkloadRevision(request WorkloadRequest) (string, error) {
	request.Revision = ""
	if request.Runtime != nil {
		request.Runtime = request.Runtime.DeepCopy()
		defaultEmbeddedWorkloadPod(&request.Runtime.Template.Spec)
	}
	data, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// Embedded Pod fields are defaulted by the workspace CRD before persistence.
// Hash their defaulted form so the same intent keeps its revision after that
// round trip. Schema tests exercise the actual API-server defaulting algorithm.
func defaultEmbeddedWorkloadPod(pod *corev1.PodSpec) {
	defaultEnv := func(env []corev1.EnvVar) {
		for i := range env {
			if ref := env[i].ValueFrom; ref != nil && ref.FileKeyRef != nil && ref.FileKeyRef.Optional == nil {
				ref.FileKeyRef.Optional = new(false)
			}
		}
	}
	defaultPorts := func(ports []corev1.ContainerPort) {
		for i := range ports {
			if ports[i].Protocol == "" {
				ports[i].Protocol = corev1.ProtocolTCP
			}
		}
	}
	defaultProbes := func(probes ...*corev1.Probe) {
		for _, probe := range probes {
			if probe != nil && probe.GRPC != nil && probe.GRPC.Service == nil {
				probe.GRPC.Service = new(string)
			}
		}
	}
	for i := range pod.Containers {
		defaultPorts(pod.Containers[i].Ports)
		defaultEnv(pod.Containers[i].Env)
		defaultProbes(pod.Containers[i].LivenessProbe, pod.Containers[i].ReadinessProbe, pod.Containers[i].StartupProbe)
	}
	for i := range pod.InitContainers {
		defaultPorts(pod.InitContainers[i].Ports)
		defaultEnv(pod.InitContainers[i].Env)
		defaultProbes(pod.InitContainers[i].LivenessProbe, pod.InitContainers[i].ReadinessProbe, pod.InitContainers[i].StartupProbe)
	}
	for i := range pod.EphemeralContainers {
		defaultPorts(pod.EphemeralContainers[i].Ports)
		defaultEnv(pod.EphemeralContainers[i].Env)
		defaultProbes(pod.EphemeralContainers[i].LivenessProbe, pod.EphemeralContainers[i].ReadinessProbe, pod.EphemeralContainers[i].StartupProbe)
	}
	for i := range pod.Volumes {
		volume := &pod.Volumes[i]
		if disk := volume.AzureDisk; disk != nil {
			if disk.FSType == nil {
				disk.FSType = new("ext4")
			}
			if disk.ReadOnly == nil {
				disk.ReadOnly = new(false)
			}
		}
		if disk := volume.ISCSI; disk != nil && disk.ISCSIInterface == "" {
			disk.ISCSIInterface = "default"
		}
		if disk := volume.RBD; disk != nil {
			if disk.Keyring == "" {
				disk.Keyring = "/etc/ceph/keyring"
			}
			if disk.RBDPool == "" {
				disk.RBDPool = "rbd"
			}
			if disk.RadosUser == "" {
				disk.RadosUser = "admin"
			}
		}
		if disk := volume.ScaleIO; disk != nil {
			if disk.FSType == "" {
				disk.FSType = "xfs"
			}
			if disk.StorageMode == "" {
				disk.StorageMode = "ThinProvisioned"
			}
		}
	}
}

func (r WorkloadRequest) Validate() error {
	if r.RestoreFrom != nil && (r.RestoreFrom.Name == "" || r.RestoreFrom.UID == "" || !validDigest(r.RestoreFrom.Digest)) {
		return fmt.Errorf("restore requires exact checkpoint name, UID and digest")
	}
	if r.Sequence < 1 || (r.Sequence == 1) != (r.PreviousInstance == nil) {
		return fmt.Errorf("first workload sequence must be one; replacements require the previous instance")
	}
	if r.PreviousInstance != nil && !r.PreviousInstance.Valid() {
		return ErrStaleIdentity
	}
	if r.RetainedData != nil && (r.PreviousInstance == nil || r.RetainedData.SourceInstance != *r.PreviousInstance || !r.RetainedData.Valid()) {
		return fmt.Errorf("cold resume requires retained data from the exact previous instance")
	}
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if err := validatePinnedImage(r.Image); err != nil {
		return err
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

func validBinding(binding ImmutableObjectBinding) bool {
	return binding.Name != "" && binding.UID != "" && binding.Generation > 0 && validDigest(binding.ProfileHash)
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != 71 {
		return false
	}
	_, err := hex.DecodeString(value[7:])
	return err == nil
}

func validatePinnedImage(image string) error {
	parts := strings.Split(image, "@sha256:")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
		return fmt.Errorf("workload image must be pinned by SHA-256 digest")
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return fmt.Errorf("invalid image digest: %w", err)
	}
	return nil
}

func (r WorkloadRequest) validateRuntime() error {
	runtime := r.Runtime
	if runtime.BootstrapPort < 1 || runtime.BootstrapPort > 65535 {
		return fmt.Errorf("runtime requires a valid bootstrap port")
	}
	if !validBinding(runtime.PoolBinding) || !validBinding(runtime.ClassBinding) || runtime.Protocol != "orka.harness.v2" {
		return fmt.Errorf("runtime requires immutable pool/class bindings and orka.harness.v2")
	}
	pod := runtime.Template.Spec
	if len(pod.ImagePullSecrets) != 0 {
		return fmt.Errorf("runtime cannot reference image-pull credential Secrets before bootstrap")
	}
	if len(pod.EphemeralContainers) != 0 {
		return fmt.Errorf("runtime templates cannot contain ephemeral containers")
	}
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		return fmt.Errorf("runtime must explicitly disable ServiceAccount token mounting")
	}
	found := false
	for _, container := range append(append([]corev1.Container(nil), pod.InitContainers...), pod.Containers...) {
		if err := validatePinnedImage(container.Image); err != nil {
			return fmt.Errorf("runtime container %q: %w", container.Name, err)
		}
		if len(container.EnvFrom) != 0 {
			return fmt.Errorf("runtime environment sources must be resolved before allocation")
		}
		for _, env := range container.Env {
			if env.ValueFrom != nil && (env.ValueFrom.SecretKeyRef != nil || env.ValueFrom.ConfigMapKeyRef != nil || env.ValueFrom.FileKeyRef != nil) {
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
		if volumeUsesSecret(volume.VolumeSource) || volume.ConfigMap != nil {
			return fmt.Errorf("runtime cannot reference credential Secrets or mutable configuration before bootstrap")
		}
		if volume.Image != nil {
			if err := validatePinnedImage(volume.Image.Reference); err != nil {
				return fmt.Errorf("runtime image volume %q: %w", volume.Name, err)
			}
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				// Only downward metadata belongs in public allocation intent.
				// Reject unknown or mixed sources as well as credential/config
				// projections, including certificate and trust-bundle rotation.
				if source.DownwardAPI == nil || !apiequality.Semantic.DeepEqual(source, corev1.VolumeProjection{DownwardAPI: source.DownwardAPI}) {
					return fmt.Errorf("runtime projected volumes allow only downward API metadata before bootstrap")
				}
			}
		}
	}
	return nil
}

func volumeUsesSecret(volume corev1.VolumeSource) bool {
	return volume.Secret != nil ||
		(volume.ISCSI != nil && volume.ISCSI.SecretRef != nil) ||
		(volume.RBD != nil && volume.RBD.SecretRef != nil) ||
		(volume.FlexVolume != nil && volume.FlexVolume.SecretRef != nil) ||
		(volume.Cinder != nil && volume.Cinder.SecretRef != nil) ||
		(volume.CephFS != nil && volume.CephFS.SecretRef != nil) ||
		(volume.AzureFile != nil && volume.AzureFile.SecretName != "") ||
		(volume.ScaleIO != nil && volume.ScaleIO.SecretRef != nil) ||
		(volume.StorageOS != nil && volume.StorageOS.SecretRef != nil) ||
		(volume.CSI != nil && volume.CSI.NodePublishSecretRef != nil)
}

// InstanceIdentity is the full fence for one physical incarnation. AllocationID
// may survive a restart; InstanceID must change for every replacement process.
type InstanceIdentity struct {
	AllocationID string `json:"allocationID"`
	InstanceID   string `json:"instanceID"`
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
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
	// Process identifies a native runtime process that is not a Kubernetes Pod.
	// Its sealed bootstrap challenge prevents rerouting credentials to a replacement.
	// +optional
	Process *NativeProcessEvidence `json:"process,omitempty"`
	// PersistentVolumes reports the exact durable storage mounted by this instance.
	// Core independently reads accessible claims and volumes before credential delivery.
	// +listType=map
	// +listMapKey=volumeName
	// +optional
	PersistentVolumes []PersistentVolumeEvidence `json:"persistentVolumes,omitempty"`
	ContractVersion   string                     `json:"contractVersion"`
	Identity          InstanceIdentity           `json:"identity"`
	Endpoint          string                     `json:"endpoint"`
	Pod               *PodReference              `json:"pod,omitempty"`
}

// NativeProcessEvidence is public process and placement evidence. The worker
// Pod is infrastructure; its spec is not the admitted runtime container spec.
type NativeProcessEvidence struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
	// +kubebuilder:validation:Minimum=1
	Version int64        `json:"version"`
	Worker  PodReference `json:"worker"`
	// ChallengeSHA256 pins the process's public, one-time encryption challenge.
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	ChallengeSHA256 string `json:"challengeSHA256"`
}

// PersistentVolumeEvidence pins the claim and backing volume used by one Pod mount.
type PersistentVolumeEvidence struct {
	// VolumeName is the corresponding admitted Pod volume name.
	// +kubebuilder:validation:MinLength=1
	VolumeName string                  `json:"volumeName"`
	Claim      PodReference            `json:"claim"`
	Volume     ObjectIdentityReference `json:"volume"`
}

// AllocationState is the observed physical instance state.
// +kubebuilder:validation:Enum=Pending;Ready;Stopped;Deleted
type AllocationState string

const (
	AllocationPending AllocationState = "Pending"
	AllocationReady   AllocationState = "Ready"
	AllocationStopped AllocationState = "Stopped"
	AllocationDeleted AllocationState = "Deleted"
)

// RetainedDataReference identifies provider-verified durable lineage. The provider
// must revalidate its private journal and backend resources before resuming. This
// is public provenance, never a credential or authority to adopt foreign data.
type RetainedDataReference struct {
	// ID is an opaque identifier owned by this workspace and provider installation.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ID             string           `json:"id"`
	SourceInstance InstanceIdentity `json:"sourceInstance"`
	// ProofSHA256 binds the provider's immutable verified lineage record.
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	ProofSHA256 string `json:"proofSHA256"`
}

func (i InstanceIdentity) Valid() bool {
	return i.AllocationID != "" && i.InstanceID != "" && validDigest(i.RequestRevision)
}

func (r RetainedDataReference) Valid() bool {
	return r.ID != "" && r.SourceInstance.Valid() && validDigest(r.ProofSHA256)
}

// AllocationObservation binds provider evidence to a single immutable request.
type AllocationObservation struct {
	// Sequence is the workload sequence actually observed by the provider.
	// +kubebuilder:validation:Minimum=1
	Sequence int64 `json:"sequence"`
	// RetainedData is published only after capture and exact termination are verified.
	// +optional
	RetainedData *RetainedDataReference         `json:"retainedData,omitempty"`
	Key          AllocationKey                  `json:"key"`
	Identity     InstanceIdentity               `json:"identity"`
	State        AllocationState                `json:"state"`
	Startup      *StartupEvidence               `json:"startup,omitempty"`
	Disposition  *ExecutionWorkspaceDisposition `json:"disposition,omitempty"`
}

// ValidateStartup rejects incomplete or mismatched provider claims. It does not
// replace the consumer's Pod, one-time listener, or authenticated fence probes.
func ValidateStartup(request WorkloadRequest, observed AllocationObservation) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if observed.Sequence != request.Sequence || observed.Key != request.Key || observed.Identity.RequestRevision != request.Revision ||
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
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || strings.HasSuffix(u.Host, ":") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("startup endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	port := 80
	if u.Scheme == "https" {
		port = 443
	}
	if explicit := u.Port(); explicit != "" {
		number, err := strconv.Atoi(explicit)
		if err != nil || number < 1 || number > 65535 {
			return fmt.Errorf("startup endpoint port must be between 1 and 65535")
		}
		port = number
	}
	if request.Runtime != nil && int(request.Runtime.BootstrapPort) != port {
		return fmt.Errorf("startup endpoint port differs from the admitted bootstrap port")
	}
	if request.Runtime != nil && evidence.Pod == nil && evidence.Process == nil {
		return fmt.Errorf("runtime startup requires an exact Pod or native process identity")
	}
	if evidence.Pod != nil && (evidence.Pod.Namespace == "" || evidence.Pod.Name == "" || evidence.Pod.UID == "") {
		return fmt.Errorf("startup Pod reference requires namespace, name and UID")
	}
	if evidence.Process != nil {
		p := evidence.Process
		if evidence.Pod != nil || p.Namespace == "" || p.Name == "" || p.UID == "" || p.Version < 1 ||
			p.Worker.Namespace == "" || p.Worker.Name == "" || p.Worker.UID == "" || !validDigest(p.ChallengeSHA256) {
			return fmt.Errorf("native startup requires exact process, worker and sealed challenge identities")
		}
	}
	volumes := map[string]bool{}
	for _, volume := range evidence.PersistentVolumes {
		if volume.VolumeName == "" || volumes[volume.VolumeName] || evidence.Pod == nil ||
			volume.Claim.Namespace != evidence.Pod.Namespace || volume.Claim.Name == "" || volume.Claim.UID == "" ||
			volume.Volume.Name == "" || volume.Volume.UID == "" {
			return fmt.Errorf("persistent volume evidence requires unique mounts and exact claim/volume identities")
		}
		volumes[volume.VolumeName] = true
	}
	return nil
}

// WorkloadRetirementAction selects the physical action authorized after core drain.
// +kubebuilder:validation:Enum=Stop;Suspend;Delete
type WorkloadRetirementAction string

const (
	WorkloadRetirementStop    WorkloadRetirementAction = "Stop"
	WorkloadRetirementSuspend WorkloadRetirementAction = "Suspend"
	WorkloadRetirementDelete  WorkloadRetirementAction = "Delete"
)

// WorkloadRetirement is core's durable authorization to retire one exact instance.
// DesiredState alone does not authorize terminating a workload: core first closes
// runtime admission and completes its drain and Task/RuntimeSession settlement.
type WorkloadRetirement struct {
	// +kubebuilder:validation:Minimum=1
	Sequence int64                    `json:"sequence"`
	Identity InstanceIdentity         `json:"identity"`
	Action   WorkloadRetirementAction `json:"action"`
}
