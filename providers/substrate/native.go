package substrate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// gvisorSandboxClass is the only native isolation this provider admits.
const gvisorSandboxClass = "gvisor"

// routerTargetHeader selects an Actor at the native ingress router.
const routerTargetHeader = "ate-target-actor"

func actorRef(record *journalRecord) *pb.ObjectRef {
	return &pb.ObjectRef{Atespace: record.Atespace, Name: record.Actor.Name}
}
func (d *Lifecycle) actor(ctx context.Context, record *journalRecord) (*pb.Actor, error) {
	actor, err := d.control.GetActor(ctx, &pb.GetActorRequest{Actor: actorRef(record)})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	meta := actor.GetMetadata()
	ref := actor.GetActorTemplate()
	if meta.GetAtespace() != record.Atespace || meta.GetName() != record.Actor.Name || meta.GetUid() == "" || record.Actor.UID != "" && record.Actor.UID != meta.GetUid() || ref.GetAtespace() != record.Atespace || (ref.GetName() != record.Template.Name && ref.GetName() != record.CreateTemplate.Name) || len(actor.GetWorkerSelector().GetMatchLabels()) != 0 {
		return nil, sdk.ErrStaleIdentity
	}
	return actor, nil
}

// verifyTemplate re-reads the immutable template by name and pins its UID,
// digest and the data-only, golden-disabled snapshot invariants.
func (d *Lifecycle) verifyTemplate(ctx context.Context, atespace string, ref nativeReference) error {
	template, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: atespace, Name: ref.Name}})
	if err != nil {
		return err
	}
	hash, err := templateDigest(template)
	if err != nil {
		return err
	}
	if template.GetMetadata().GetUid() != ref.UID || ref.UID == "" || hash != ref.Digest || validateDataOnlyTemplate(template) != nil {
		return fmt.Errorf("native template identity or data-only golden-disabled snapshot policy changed")
	}
	return nil
}

// validateDataOnlyTemplate checks the snapshot invariants that make every
// suspension data-only and every fresh Actor a cold boot.
func validateDataOnlyTemplate(template *pb.ActorTemplate) error {
	config := template.GetSnapshotConfig()
	if config.GetPreferredFidelity() != pb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES || config.GetGoldenSnapshotConfig().GetMode() != pb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED || template.GetGoldenEgressPolicy() != nil {
		return fmt.Errorf("native template must use VOLUMES snapshots with golden snapshots disabled")
	}
	limits := template.GetResources().GetLimits()
	if limits.GetCpuMillicores() <= 0 || limits.GetMemoryBytes() <= 0 {
		return fmt.Errorf("native template must size the Actor with CPU and memory limits")
	}
	return nil
}

func (d *Lifecycle) ensureInfrastructure(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if err := d.verifyPlacement(ctx, record); err != nil {
		return err
	}
	anchor := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: record.Placement.Namespace, Name: record.Anchor.Name}
	err := d.client.Get(ctx, key, anchor)
	if apierrors.IsNotFound(err) {
		if record.Anchor.UID != "" {
			return sdk.ErrStaleIdentity
		}
		if !record.Anchor.CreateIssued {
			record.Anchor.CreateIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		anchor = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: labels(record)}, Data: map[string]string{"journalName": cm.Name, "journalNamespace": cm.Namespace, "journalUID": string(cm.UID)}}
		if err := d.client.Create(ctx, anchor); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if anchor.UID == "" || !reflect.DeepEqual(anchor.Labels, labels(record)) || anchor.Data["journalUID"] != string(cm.UID) || record.Anchor.UID != "" && record.Anchor.UID != string(anchor.UID) {
		return sdk.ErrStaleIdentity
	}
	if record.Anchor.UID == "" {
		record.Anchor.UID = string(anchor.UID)
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	return d.ensureRuntimePool(ctx, cm, record)
}

func (d *Lifecycle) verifyInfrastructureReadOnly(ctx context.Context, record *journalRecord) error {
	anchor := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: record.Placement.Namespace, Name: record.Anchor.Name}
	if err := d.client.Get(ctx, key, anchor); err != nil {
		return err
	}
	journal := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, journalKey(record.Request.Key), journal); err != nil {
		return err
	}
	if record.Anchor.UID == "" || string(anchor.UID) != record.Anchor.UID || anchor.DeletionTimestamp != nil || !reflect.DeepEqual(anchor.Labels, labels(record)) || anchor.Data["journalName"] != journal.Name || anchor.Data["journalNamespace"] != journal.Namespace || anchor.Data["journalUID"] != string(journal.UID) {
		return sdk.ErrStaleIdentity
	}
	return d.verifyRuntimePool(ctx, record)
}

func workerLabels(record *journalRecord) map[string]string {
	return map[string]string{workerAllocationLabel: record.Observation.Identity.AllocationID, workerInstanceLabel: record.Observation.Identity.InstanceID}
}

// expectedWorkerCapacity is the capacity a private worker sized for exactly
// one template-sized Actor reports.
func expectedWorkerCapacity(record *journalRecord) (cpu, memory int64) {
	limits := record.TemplateSpec.GetResources().GetLimits()
	return limits.GetCpuMillicores(), limits.GetMemoryBytes()
}

// verifySingleActorWorker checks the native worker is gVisor and sized so the
// template-sized Actor is the only one that fits.
func verifySingleActorWorker(record *journalRecord, worker *pb.Worker) error {
	return verifySingleActorWorkerShape(record, worker, false)
}

// verifySingleActorWorkerShape checks the gVisor class and single-Actor
// sizing. A worker process that is re-registering reports no sandbox classes
// until it lists its runtimes again; teardown may proceed through that window
// (allowRegistering) because the exact identity, capacity and exclusive
// assignment checks still apply, while startup always requires the class.
func verifySingleActorWorkerShape(record *journalRecord, worker *pb.Worker, allowRegistering bool) error {
	classes := worker.GetStatus().GetSandboxClasses()
	registering := allowRegistering && len(classes) == 0
	if !registering && (len(classes) != 1 || classes[0].GetSandboxClass() != gvisorSandboxClass) {
		return fmt.Errorf("native worker must run only the gVisor sandbox class")
	}
	cpu, memory := expectedWorkerCapacity(record)
	capacity := worker.GetStatus().GetCapacity()
	if cpu <= 0 || memory <= 0 || capacity.GetCpuMillicores() != cpu || capacity.GetMemoryBytes() != memory || capacity.GetActors() < 1 {
		return fmt.Errorf("native worker capacity must fit exactly one template-sized Actor")
	}
	return nil
}

// verifyWorkerExclusive lists the worker's assignments and fails closed when
// any Actor other than this record's exact lifetime is placed there.
func (d *Lifecycle) verifyWorkerExclusive(ctx context.Context, record *journalRecord, workerName string, requireAssigned bool) error {
	ref := &pb.ObjectRef{Name: workerName}
	page := ""
	seen := map[string]bool{}
	assigned := false
	for {
		assignments, err := d.control.ListWorkerActorAssignments(ctx, &pb.ListWorkerActorAssignmentsRequest{Worker: ref, Options: &pb.ListOptions{PageSize: 1000, PageToken: page}})
		if err != nil {
			return err
		}
		for _, assignment := range assignments.GetActorAssignments() {
			if record.Actor.UID == "" || assignment.GetActorUid() != record.Actor.UID || !proto.Equal(assignment.GetActor(), actorRef(record)) {
				return fmt.Errorf("native worker hosts another Actor; shared placement is refused")
			}
			assigned = true
		}
		page = assignments.GetNextPageToken()
		if page == "" {
			break
		}
		if seen[page] || len(seen) >= 100 {
			return fmt.Errorf("native assignment pagination repeated a token or exceeded its bound")
		}
		seen[page] = true
	}
	if requireAssigned && !assigned {
		return fmt.Errorf("native worker does not report the recorded Actor assignment")
	}
	return nil
}

func (d *Lifecycle) bindWorker(ctx context.Context, record *journalRecord) error {
	if record.Worker == nil {
		return fmt.Errorf("native worker identity is missing before binding")
	}
	worker := record.Worker
	pod := &corev1.Pod{}
	key := types.NamespacedName{Namespace: worker.Namespace, Name: worker.Pod}
	if err := d.client.Get(ctx, key, pod); err != nil {
		return err
	}
	if string(pod.UID) != worker.PodUID || pod.DeletionTimestamp != nil || pod.Labels[workerPoolLabel] != worker.Pool {
		return sdk.ErrStaleIdentity
	}
	for key, expected := range workerLabels(record) {
		if value := pod.Labels[key]; value != expected {
			return fmt.Errorf("native worker has a foreign allocation label: %w", sdk.ErrStaleIdentity)
		}
	}
	return nil
}

// Worker identity lookup also supports exact retirement recovery. Running
// Actors must run under the worker process epoch registered for the fence.
func (d *Lifecycle) worker(ctx context.Context, record *journalRecord, actor *pb.Actor) (*workerFence, error) {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() == "" || assignment.GetWorkerPodUid() == "" {
		return nil, fmt.Errorf("native Actor has no exact worker assignment")
	}
	worker, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: assignment.Worker})
	if err != nil {
		return nil, err
	}
	if worker.GetMetadata().GetUid() == "" || worker.GetMetadata().GetName() != assignment.GetWorker().GetName() || worker.GetWorkerNamespace() != record.Placement.Namespace || worker.GetWorkerPool() != record.RuntimePool.Name || worker.GetWorkerPodUid() != assignment.GetWorkerPodUid() || worker.GetWorkerPod() != assignment.GetWorkerPod() || assignment.GetWorkerNamespace() != record.Placement.Namespace || assignment.GetWorkerPool() != record.RuntimePool.Name {
		return nil, fmt.Errorf("native runtime requires a worker in the admitted private WorkerPool")
	}
	if err := verifySingleActorWorker(record, worker); err != nil {
		return nil, err
	}
	epoch := worker.GetStatus().GetRegisteredEpoch()
	if actor.GetStatus().GetState() == pb.ActorState_ACTOR_STATE_RUNNING && (epoch <= 0 || assignment.GetWorkerEpoch() != epoch || assignment.GetSandboxRuntime().GetSandboxClass() != gvisorSandboxClass) {
		return nil, fmt.Errorf("native Actor does not run under the worker's registered gVisor process epoch: %w", sdk.ErrStaleIdentity)
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: worker.GetWorkerNamespace(), Name: worker.GetWorkerPod()}, pod); err != nil {
		return nil, err
	}
	if string(pod.UID) != worker.GetWorkerPodUid() || pod.DeletionTimestamp != nil || pod.Labels[workerPoolLabel] != record.RuntimePool.Name {
		return nil, sdk.ErrStaleIdentity
	}
	for key, expected := range workerLabels(record) {
		if value := pod.Labels[key]; (value != "" && value != expected) || (record.Worker != nil && value != expected) {
			return nil, sdk.ErrStaleIdentity
		}
	}
	return &workerFence{Name: worker.GetMetadata().GetName(), UID: worker.GetMetadata().GetUid(), Namespace: pod.Namespace, Pool: record.RuntimePool.Name, Pod: pod.Name, PodUID: string(pod.UID), AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.InstanceID, Epoch: epoch}, nil
}

// endpoint is the native router origin; the target header selects the Actor.
func (d *Lifecycle) endpoint() string {
	base, err := routerBaseURL(d.config.RouterEndpoint)
	if err != nil {
		return ""
	}
	return base.String()
}

func routerTarget(record *journalRecord) string {
	return record.Atespace + "/" + record.Actor.Name
}

// This read-only DTO mirrors the existing public sealed bootstrap challenge.
// Signing, sealing, credentials, and runtime admission stay in core.
type bootstrapChallenge struct {
	Schema    string `json:"schema"`
	Nonce     string `json:"nonce"`
	BootNonce string `json:"bootNonce"`
	PublicKey string `json:"publicKey"`
	Actor     struct {
		Atespace string `json:"atespace"`
		Name     string `json:"name"`
		UID      string `json:"uid"`
	} `json:"actor"`
}
type challengeGetter interface {
	Get(ctx context.Context, endpoint, target string) (bootstrapChallenge, error)
}

// routerChallengeGetter reads the public challenge through the native router
// over mTLS, presenting the provider's allowlisted client identity.
type routerChallengeGetter struct{ config Config }

var errChallengeClosed = errors.New("native bootstrap listener closed")

func (g routerChallengeGetter) Get(ctx context.Context, endpoint, target string) (bootstrapChallenge, error) {
	tlsConfig, err := routerTLSConfig(g.config)
	if err != nil {
		return bootstrapChallenge{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/v2/credential-bootstrap", nil)
	if err != nil {
		return bootstrapChallenge{}, err
	}
	request.Header.Set(routerTargetHeader, target)
	transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	c := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := c.Do(request)
	if err != nil {
		return bootstrapChallenge{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return bootstrapChallenge{}, errChallengeClosed
	}
	if response.StatusCode != http.StatusOK {
		return bootstrapChallenge{}, fmt.Errorf("native public bootstrap challenge is unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(data) > 65536 {
		return bootstrapChallenge{}, fmt.Errorf("native challenge size is invalid")
	}
	var challenge bootstrapChallenge
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&challenge); err != nil {
		return bootstrapChallenge{}, fmt.Errorf("native challenge is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return bootstrapChallenge{}, fmt.Errorf("native challenge has trailing data")
	}
	return challenge, nil
}
func (d *Lifecycle) challenge(ctx context.Context, record *journalRecord, actor *pb.Actor) (string, error) {
	nonce, err := nativeBootstrapNonce(record.Request.Runtime.Template.Spec.Containers[0].Env)
	if err != nil {
		return "", err
	}
	getter := d.challengeClient
	if getter == nil {
		getter = routerChallengeGetter{config: d.config}
	}
	endpoint := d.endpoint()
	if endpoint == "" {
		return "", fmt.Errorf("native router endpoint is not configured")
	}
	challenge, err := getter.Get(ctx, endpoint, routerTarget(record))
	closed := errors.Is(err, errChallengeClosed) && record.ChallengeSHA256 != ""
	if err != nil && !closed {
		return "", err
	}
	key, keyErr := base64.RawURLEncoding.DecodeString(challenge.PublicKey)
	boot, bootErr := base64.RawURLEncoding.DecodeString(challenge.BootNonce)
	if !closed && (nonce == "" || challenge.Schema != "orka.harness.v2/sealed-bootstrap/v1" || challenge.Nonce != nonce || challenge.Actor.Atespace != record.Atespace || challenge.Actor.Name != record.Actor.Name || challenge.Actor.UID != record.Actor.UID || keyErr != nil || len(key) != 32 || bootErr != nil || len(boot) != 32) {
		return "", sdk.ErrStaleIdentity
	}
	if record.ChallengeSHA256 != "" && record.ChallengeVersion != actor.GetMetadata().GetVersion() {
		return "", sdk.ErrStaleIdentity
	}
	current, err := d.actor(ctx, record)
	if err != nil {
		return "", err
	}
	if current == nil || current.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() || current.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING || !proto.Equal(current.GetStatus().GetWorkerAssignment(), actor.GetStatus().GetWorkerAssignment()) {
		return "", sdk.ErrStaleIdentity
	}
	worker, err := d.worker(ctx, record, current)
	if err != nil {
		return "", err
	}
	if record.Worker == nil || *worker != *record.Worker {
		return "", sdk.ErrStaleIdentity
	}
	if closed {
		return record.ChallengeSHA256, nil
	}
	data, err := json.Marshal(challenge)
	if err != nil {
		return "", err
	}
	hash := digest(data)
	if record.ChallengeSHA256 != "" && record.ChallengeSHA256 != hash {
		return "", fmt.Errorf("native supervisor process changed: %w", sdk.ErrStaleIdentity)
	}
	return hash, nil
}

func pendingObservation(record *journalRecord) sdk.AllocationObservation {
	observed := record.Observation
	observed.State = sdk.AllocationPending
	observed.Startup = nil
	return observed
}
func (d *Lifecycle) observeReady(ctx context.Context, record *journalRecord) (sdk.AllocationObservation, error) {
	if record.Template.UID == "" || record.Worker == nil || record.ChallengeSHA256 == "" {
		return pendingObservation(record), nil
	}
	// Exact loss is checked first: removed infrastructure can be part of the
	// same loss and must not hide it behind a readiness error.
	actor, err := d.actor(ctx, record)
	if err != nil {
		return sdk.AllocationObservation{}, err
	}
	if actor == nil {
		return sdk.AllocationObservation{}, lostInstance(record, nativeActorDisappeared)
	}
	if actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING {
		return sdk.AllocationObservation{}, lostInstance(record, nativeActorStopped)
	}
	if err := d.verifyInheritedCheckpoint(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.verifyPlacement(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.verifyInfrastructureReadOnly(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	// The Actor names its template; the immutable template UID is pinned by
	// re-reading it, since v1 reports no running template UID on the Actor.
	if actor.GetActorTemplate().GetName() != record.Template.Name {
		return sdk.AllocationObservation{}, sdk.ErrStaleIdentity
	}
	if err := d.verifyTemplate(ctx, record.Atespace, record.Template); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.verifyEgressPolicy(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	worker, err := d.worker(ctx, record, actor)
	if err != nil {
		return sdk.AllocationObservation{}, err
	}
	if record.Worker == nil || *worker != *record.Worker {
		return sdk.AllocationObservation{}, sdk.ErrStaleIdentity
	}
	if err := d.bindWorker(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.verifyWorkerExclusive(ctx, record, worker.Name, true); err != nil {
		return sdk.AllocationObservation{}, err
	}
	hash, err := d.challenge(ctx, record, actor)
	if err != nil {
		return sdk.AllocationObservation{}, err
	}
	if record.ChallengeSHA256 == "" || record.ChallengeSHA256 != hash {
		return pendingObservation(record), nil
	}
	observed := record.Observation
	observed.State = sdk.AllocationReady
	observed.Startup = &sdk.StartupEvidence{ContractVersion: sdk.LifecycleContractV1, Identity: observed.Identity, Endpoint: d.endpoint(), Process: &api.NativeProcessEvidence{Namespace: record.Atespace, Name: record.Actor.Name, UID: record.Actor.UID, Version: actor.GetMetadata().GetVersion(), Worker: api.PodReference{Namespace: worker.Namespace, Name: worker.Pod, UID: types.UID(worker.PodUID)}, ChallengeSHA256: hash, RouterTarget: routerTarget(record)}}
	return observed, nil
}
