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
	"slices"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
	if meta.GetAtespace() != record.Atespace || meta.GetName() != record.Actor.Name || meta.GetUid() == "" || record.Actor.UID != "" && record.Actor.UID != meta.GetUid() || ref.GetAtespace() != record.Atespace || (ref.GetName() != record.Template.Name && ref.GetName() != record.CreateTemplate.Name) {
		return nil, sdk.ErrStaleIdentity
	}
	return actor, nil
}
func (d *Lifecycle) verifyTemplate(ctx context.Context, atespace string, ref nativeReference) error {
	template, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: atespace, Name: ref.Name}})
	if err != nil {
		return err
	}
	hash, err := templateDigest(template)
	if err != nil {
		return err
	}
	config := template.GetSnapshotsConfig()
	if template.GetMetadata().GetUid() != ref.UID || ref.UID == "" || hash != ref.Digest || config.GetOnPause() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || config.GetOnCommit() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || config.GetOnResume().GetFromData() != pb.ResumeSource_RESUME_SOURCE_COLD_BOOT {
		return fmt.Errorf("native template identity or Data/Data/ColdBoot policy changed")
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
	policy := &networkingv1.NetworkPolicy{}
	key.Name = record.NetworkPolicy.Name
	err = d.client.Get(ctx, key, policy)
	desired := nativeNetworkPolicy(record)
	if apierrors.IsNotFound(err) {
		if record.NetworkPolicy.UID != "" {
			return sdk.ErrStaleIdentity
		}
		if !record.NetworkPolicy.CreateIssued {
			record.NetworkPolicy.CreateIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		policy = &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: labels(record), OwnerReferences: []metav1.OwnerReference{anchorOwner(record)}}, Spec: desired}
		if err := d.client.Create(ctx, policy); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if policy.UID == "" || record.NetworkPolicy.UID != "" && record.NetworkPolicy.UID != string(policy.UID) || !reflect.DeepEqual(policy.Labels, labels(record)) || !reflect.DeepEqual(policy.OwnerReferences, []metav1.OwnerReference{anchorOwner(record)}) || !apiequality.Semantic.DeepEqual(sdk.NormalizedNetworkPolicySpec(policy.Spec), desired) {
		return fmt.Errorf("native network policy differs from admitted rules")
	}
	if record.NetworkPolicy.UID == "" {
		record.NetworkPolicy.UID = string(policy.UID)
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
	policy := &networkingv1.NetworkPolicy{}
	key.Name = record.NetworkPolicy.Name
	if err := d.client.Get(ctx, key, policy); err != nil {
		return err
	}
	desired := nativeNetworkPolicy(record)
	if record.NetworkPolicy.UID == "" || string(policy.UID) != record.NetworkPolicy.UID || policy.DeletionTimestamp != nil || !reflect.DeepEqual(policy.Labels, labels(record)) || !reflect.DeepEqual(policy.OwnerReferences, []metav1.OwnerReference{anchorOwner(record)}) || !apiequality.Semantic.DeepEqual(sdk.NormalizedNetworkPolicySpec(policy.Spec), desired) {
		return fmt.Errorf("native network confinement identity or admitted rules changed")
	}
	return d.verifyRuntimePool(ctx, record)
}

func validateNativeNetworkPolicy(runtime *sdk.RuntimeWorkload) error {
	policy := runtime.NetworkPolicy
	// Kubernetes defaults omitted policyTypes to Ingress, adding Egress when
	// egress rules are present. Worker management ingress is operator-owned.
	ingress := len(policy.PolicyTypes) == 0 || slices.Contains(policy.PolicyTypes, networkingv1.PolicyTypeIngress)
	egress := slices.Contains(policy.PolicyTypes, networkingv1.PolicyTypeEgress) || len(policy.PolicyTypes) == 0 && len(policy.Egress) != 0
	if !egress && len(policy.Egress) != 0 {
		return fmt.Errorf("native network policy cannot activate inactive egress rules")
	}
	if ingress || len(policy.Ingress) != 0 {
		return fmt.Errorf("native runtime ingress policy is unsupported")
	}
	if !egress || len(policy.PolicyTypes) != 1 {
		return fmt.Errorf("native network policy requires only effective Egress")
	}
	for _, rule := range policy.Egress {
		for _, peer := range rule.To {
			if peer.PodSelector != nil && peer.NamespaceSelector == nil && runtime.Template.Namespace == "" {
				return fmt.Errorf("native namespace-relative egress requires a frozen runtime namespace")
			}
		}
	}
	return nil
}

func nativeNetworkPolicy(record *journalRecord) networkingv1.NetworkPolicySpec {
	desired := *record.Request.Runtime.NetworkPolicy.DeepCopy()
	desired.PodSelector = metav1.LabelSelector{MatchLabels: workerLabels(record)}
	desired.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
	desired.Ingress = nil
	namespace := record.Request.Runtime.Template.Namespace
	if namespace == "" {
		namespace = record.Request.Key.Namespace
	}
	qualifyNativeEgressPeers(&desired, namespace)
	return sdk.NormalizedNetworkPolicySpec(desired)
}

func qualifyNativeEgressPeers(policy *networkingv1.NetworkPolicySpec, namespace string) {
	for i := range policy.Egress {
		for j := range policy.Egress[i].To {
			peer := &policy.Egress[i].To[j]
			if peer.PodSelector != nil && peer.NamespaceSelector == nil {
				// Resolve Pod-only peers in the namespace of the original policy
				// before comparing policies from different namespaces.
				peer.NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: namespace}}
			}
		}
	}
}

// NetworkPolicies add permissions. Inherited operator labels may select more
// policies than the recorded confinement policy, so inspect their complete
// egress union on the exact worker before Resume and every Ready observation.
// Ingress remains operator-owned; this object check does not prove CNI packet
// enforcement or replace the installation's direct-egress acknowledgement.
func (d *Lifecycle) verifyWorkerNetworkPolicies(ctx context.Context, record *journalRecord, pod *corev1.Pod) error {
	policies := &networkingv1.NetworkPolicyList{}
	if err := d.client.List(ctx, policies, client.InNamespace(pod.Namespace)); err != nil {
		return fmt.Errorf("read native worker NetworkPolicies: %w", err)
	}
	admitted := nativeNetworkPolicy(record)
	found, isolated := false, false
	for _, policy := range policies.Items {
		actual := sdk.NormalizedNetworkPolicySpec(policy.Spec)
		if policy.Name == record.NetworkPolicy.Name {
			if record.NetworkPolicy.UID == "" || string(policy.UID) != record.NetworkPolicy.UID || policy.DeletionTimestamp != nil || !reflect.DeepEqual(policy.Labels, labels(record)) || !reflect.DeepEqual(policy.OwnerReferences, []metav1.OwnerReference{anchorOwner(record)}) || !apiequality.Semantic.DeepEqual(actual, admitted) {
				return fmt.Errorf("native network confinement identity or admitted rules changed")
			}
			found = true
		}
		selector, err := metav1.LabelSelectorAsSelector(&actual.PodSelector)
		if err != nil {
			return fmt.Errorf("native NetworkPolicy %q selector is invalid: %w", policy.Name, err)
		}
		if !selector.Matches(klabels.Set(pod.Labels)) {
			continue
		}
		if policy.DeletionTimestamp != nil {
			return fmt.Errorf("native worker NetworkPolicy %q is being deleted", policy.Name)
		}
		for _, direction := range actual.PolicyTypes {
			if direction != networkingv1.PolicyTypeIngress && direction != networkingv1.PolicyTypeEgress {
				return fmt.Errorf("native worker NetworkPolicy %q has unsupported direction %q", policy.Name, direction)
			}
		}
		if !slices.Contains(actual.PolicyTypes, networkingv1.PolicyTypeEgress) {
			continue
		}
		isolated = true
		// Extra Pod-only peers refer to the worker's namespace, whereas the
		// admitted request's peers were translated from the runtime namespace.
		qualifyNativeEgressPeers(&actual, pod.Namespace)
		for _, rule := range actual.Egress {
			if !slices.ContainsFunc(admitted.Egress, func(allowed networkingv1.NetworkPolicyEgressRule) bool {
				return nativePeersWithin(rule.To, allowed.To) && nativePortsWithin(rule.Ports, allowed.Ports)
			}) {
				return fmt.Errorf("native worker labels select unadmitted egress permissions in NetworkPolicy %q", policy.Name)
			}
		}
	}
	if !found || !isolated {
		return fmt.Errorf("native worker is missing its admitted egress isolation")
	}
	return nil
}

// Within a rule, peers and ports form independent OR lists combined with AND.
// Keep both subsets in one admitted rule to avoid cross-grants between rules.
// Selector implication and IP-block exclusions remain conservatively exact.
func nativePeersWithin(actual, allowed []networkingv1.NetworkPolicyPeer) bool {
	if len(allowed) == 0 {
		return true
	}
	if len(actual) == 0 {
		return false
	}
	for _, peer := range actual {
		if !slices.ContainsFunc(allowed, func(candidate networkingv1.NetworkPolicyPeer) bool { return reflect.DeepEqual(peer, candidate) }) {
			return false
		}
	}
	return true
}

func nativePortsWithin(actual, allowed []networkingv1.NetworkPolicyPort) bool {
	if len(allowed) == 0 {
		return true
	}
	if len(actual) == 0 {
		return false
	}
	for _, port := range actual {
		if !slices.ContainsFunc(allowed, func(candidate networkingv1.NetworkPolicyPort) bool { return nativePortWithin(port, candidate) }) {
			return false
		}
	}
	return true
}

func nativePortWithin(actual, allowed networkingv1.NetworkPolicyPort) bool {
	if !reflect.DeepEqual(actual.Protocol, allowed.Protocol) {
		return false
	}
	if allowed.Port == nil {
		return allowed.EndPort == nil
	}
	if actual.Port == nil {
		return false
	}
	if actual.Port.Type != intstr.Int || allowed.Port.Type != intstr.Int {
		return reflect.DeepEqual(actual, allowed)
	}
	start, end := actual.Port.IntVal, actual.Port.IntVal
	if actual.EndPort != nil {
		end = *actual.EndPort
	}
	allowedStart, allowedEnd := allowed.Port.IntVal, allowed.Port.IntVal
	if allowed.EndPort != nil {
		allowedEnd = *allowed.EndPort
	}
	return start > 0 && end >= start && end <= 65535 && allowedStart > 0 && allowedEnd >= allowedStart && allowedEnd <= 65535 && start >= allowedStart && end <= allowedEnd
}

func workerLabels(record *journalRecord) map[string]string {
	return map[string]string{workerAllocationLabel: record.Observation.Identity.AllocationID, workerInstanceLabel: record.Observation.Identity.InstanceID}
}

func (d *Lifecycle) bindWorker(ctx context.Context, record *journalRecord) error {
	if record.Worker == nil {
		return fmt.Errorf("native worker identity is missing before network binding")
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
	labels := workerLabels(record)
	for key, expected := range labels {
		if value := pod.Labels[key]; value != expected {
			return fmt.Errorf("native worker has a foreign allocation label: %w", sdk.ErrStaleIdentity)
		}
	}
	return d.verifyWorkerNetworkPolicies(ctx, record, pod)
}

// Worker identity lookup also supports exact retirement recovery. Confinement
// is checked separately on startup paths so policy drift cannot prevent Stop.
func (d *Lifecycle) worker(ctx context.Context, record *journalRecord, actor *pb.Actor) (*workerFence, error) {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() == "" || assignment.GetWorkerPodUid() == "" {
		return nil, fmt.Errorf("native Actor has no exact worker assignment")
	}
	worker, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: assignment.Worker})
	if err != nil {
		return nil, err
	}
	if worker.GetMetadata().GetUid() == "" || worker.GetMetadata().GetName() != assignment.GetWorker().GetName() || worker.GetWorkerNamespace() != record.Placement.Namespace || worker.GetWorkerPool() != record.RuntimePool.Name || worker.GetWorkerPodUid() != assignment.GetWorkerPodUid() || worker.GetWorkerPod() != assignment.GetWorkerPod() || worker.GetStatus().GetCapacity().GetActors() != 1 || assignment.GetWorkerNamespace() != record.Placement.Namespace || assignment.GetWorkerPool() != record.RuntimePool.Name {
		return nil, fmt.Errorf("native runtime requires a single-Actor worker in the admitted WorkerPool")
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
	return &workerFence{Name: worker.GetMetadata().GetName(), UID: worker.GetMetadata().GetUid(), Namespace: pod.Namespace, Pool: record.RuntimePool.Name, Pod: pod.Name, PodUID: string(pod.UID), AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.InstanceID}, nil
}

func (d *Lifecycle) endpoint(record *journalRecord) string {
	return "http://" + record.Actor.Name + "." + record.Atespace + "." + d.config.ActorDNSSuffix + ":80"
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
	Get(context.Context, string) (bootstrapChallenge, error)
}
type httpChallengeGetter struct{}

var errChallengeClosed = errors.New("native bootstrap listener closed")

func (httpChallengeGetter) Get(ctx context.Context, endpoint string) (bootstrapChallenge, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/v2/credential-bootstrap", nil)
	if err != nil {
		return bootstrapChallenge{}, err
	}
	transport := &http.Transport{Proxy: nil}
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
		getter = httpChallengeGetter{}
	}
	challenge, err := getter.Get(ctx, d.endpoint(record))
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
	if err := d.verifyInheritedCheckpoint(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.verifyPlacement(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.verifyInfrastructureReadOnly(ctx, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.verifyTemplate(ctx, record.Atespace, record.Template); err != nil {
		return sdk.AllocationObservation{}, err
	}
	actor, err := d.actor(ctx, record)
	if err != nil {
		return sdk.AllocationObservation{}, err
	}
	if actor == nil || actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING {
		if record.ChallengeSHA256 != "" {
			return sdk.AllocationObservation{}, fmt.Errorf("recorded native Actor stopped; uncertain work cannot be replayed")
		}
		return pendingObservation(record), nil
	}
	if actor.GetStatus().GetCurrentActorTemplateUid() != record.Template.UID {
		return sdk.AllocationObservation{}, sdk.ErrStaleIdentity
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
	hash, err := d.challenge(ctx, record, actor)
	if err != nil {
		return sdk.AllocationObservation{}, err
	}
	if record.ChallengeSHA256 == "" || record.ChallengeSHA256 != hash {
		return pendingObservation(record), nil
	}
	observed := record.Observation
	observed.State = sdk.AllocationReady
	observed.Startup = &sdk.StartupEvidence{ContractVersion: sdk.LifecycleContractV1, Identity: observed.Identity, Endpoint: d.endpoint(record), Process: &api.NativeProcessEvidence{Namespace: record.Atespace, Name: record.Actor.Name, UID: record.Actor.UID, Version: actor.GetMetadata().GetVersion(), Worker: api.PodReference{Namespace: worker.Namespace, Name: worker.Pod, UID: types.UID(worker.PodUID)}, ChallengeSHA256: hash}}
	return observed, nil
}
