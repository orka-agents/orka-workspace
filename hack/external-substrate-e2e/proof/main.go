// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Runs the standalone provider's public reconciler against installed native
// control and real Kubernetes objects. Core admission is an explicit fixture.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type challenge struct {
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

type proof struct {
	c                                     client.Client
	native                                pb.ControlClient
	config                                provider.Config
	namespace, image, marker, sourceNonce string
	profile                               *profile.SubstrateWorkspaceProfile
	installed                             *api.ExecutionWorkspaceProvider
	class                                 api.ImmutableObjectBinding
	checks                                []string
	basePool                              *unstructured.Unstructured
}

// Check actual Kubernetes confinement immediately before forwarding each real
// native Resume. This observes installed objects independently of the provider
// journal, including policy-before-Pod creation and immutable source placement.
type guardedControl struct {
	pb.ControlClient
	proof *proof
}

func (c *guardedControl) ResumeActor(ctx context.Context, request *pb.ResumeActorRequest, options ...grpc.CallOption) (*pb.ResumeActorResponse, error) {
	if err := c.proof.verifyBeforeResume(ctx, request.Actor); err != nil {
		return nil, err
	}
	return c.ControlClient.ResumeActor(ctx, request, options...)
}

func (p *proof) verifyBeforeResume(ctx context.Context, ref *pb.ObjectRef) error {
	actor, err := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: ref})
	if err != nil {
		return err
	}
	template, err := p.native.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: actor.ActorTemplate})
	if err != nil {
		return err
	}
	selector := map[string]string{}
	for _, key := range []string{"substrate.workspace.orka.ai/allocation-id", "substrate.workspace.orka.ai/instance-id"} {
		selector[key] = template.GetWorkerSelector().GetMatchLabels()[key]
		if selector[key] == "" {
			return fmt.Errorf("pre-Resume Actor template lacks private worker selector")
		}
	}
	pods := &corev1.PodList{}
	if err := p.c.List(ctx, pods, client.InNamespace(p.namespace), client.MatchingLabels(selector)); err != nil {
		return err
	}
	if len(pods.Items) != 1 || pods.Items[0].DeletionTimestamp != nil {
		return fmt.Errorf("pre-Resume requires one real private worker Pod")
	}
	pod := &pods.Items[0]
	pool := &unstructured.Unstructured{}
	pool.SetAPIVersion("ate.dev/v1alpha1")
	pool.SetKind("WorkerPool")
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: pod.Labels["ate.dev/worker-pool"]}, pool); err != nil {
		return err
	}
	replicas, _, err := unstructured.NestedInt64(pool.Object, "spec", "replicas")
	if err != nil || replicas != 1 || pool.GetUID() == p.basePool.GetUID() {
		return fmt.Errorf("pre-Resume worker is not in a private single-replica pool")
	}
	birthLabels, _, err := unstructured.NestedStringMap(pool.Object, "spec", "template", "labels")
	if err != nil {
		return err
	}
	for key, value := range selector {
		if birthLabels[key] != value || pool.GetLabels()[key] != value {
			return fmt.Errorf("pre-Resume private labels absent from worker birth template or pool")
		}
	}
	policies := &networkingv1.NetworkPolicyList{}
	if err := p.c.List(ctx, policies, client.InNamespace(p.namespace)); err != nil {
		return err
	}
	confined := false
	for _, policy := range policies.Items {
		if policy.Spec.PodSelector.MatchLabels["substrate.workspace.orka.ai/allocation-id"] == selector["substrate.workspace.orka.ai/allocation-id"] &&
			policy.Spec.PodSelector.MatchLabels["substrate.workspace.orka.ai/instance-id"] == selector["substrate.workspace.orka.ai/instance-id"] &&
			policy.DeletionTimestamp == nil && !policy.CreationTimestamp.After(pod.CreationTimestamp.Time) &&
			len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == networkingv1.PolicyTypeEgress && len(policy.Spec.Egress) == 1 {
			confined = true
		}
	}
	if !confined {
		return fmt.Errorf("pre-Resume admitted egress policy did not precede worker Pod birth")
	}
	base := &unstructured.Unstructured{}
	base.SetAPIVersion("ate.dev/v1alpha1")
	base.SetKind("WorkerPool")
	if err := p.c.Get(ctx, client.ObjectKeyFromObject(p.basePool), base); err != nil {
		return err
	}
	baseSpec, _ := json.Marshal(base.Object["spec"])
	originalSpec, _ := json.Marshal(p.basePool.Object["spec"])
	if base.GetUID() != p.basePool.GetUID() || string(baseSpec) != string(originalSpec) {
		return fmt.Errorf("operator source WorkerPool identity or spec changed")
	}
	p.passed("real private single-replica worker was born with admitted egress selector; policy existed before Pod and native Resume; source pool unchanged")
	return nil
}

func randomValue() string {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}
func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func (p *proof) passed(message string) {
	p.checks = append(p.checks, message)
	fmt.Println("PASS", message)
}

func poll(ctx context.Context, check func() (bool, error)) error {
	for {
		done, err := check()
		if done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func getJSON(ctx context.Context, url string, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("fixture HTTP status %d", response.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(out)
}

func getJSONWithDNSRetry(ctx context.Context, url string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var lastErr error
	if err := poll(ctx, func() (bool, error) {
		lastErr = getJSON(ctx, url, out)
		var dnsErr *net.DNSError
		if errors.As(lastErr, &dnsErr) {
			return false, nil
		}
		return true, lastErr
	}); err != nil {
		return fmt.Errorf("public read failed: %w (last DNS error %v)", err, lastErr)
	}
	return nil
}

func (p *proof) reconcileWorkspace(ctx context.Context, w *api.ExecutionWorkspace, target api.AllocationState) error {
	r := &provider.ExecutionWorkspaceReconciler{Client: p.c, Control: p.native, Config: p.config}
	var lastErr error
	err := poll(ctx, func() (bool, error) {
		_, lastErr = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
		if target != api.AllocationDeleted && (errors.Is(lastErr, sdk.ErrStaleIdentity) || errors.Is(lastErr, sdk.ErrRequestConflict)) {
			return false, lastErr
		}
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
			return false, err
		}
		return lastErr == nil && w.Status.Allocation != nil && w.Status.Allocation.State == target, nil
	})
	if err != nil {
		return fmt.Errorf("workspace %s target %s: %w, last reconciliation error %v", w.Name, target, err, lastErr)
	}
	return nil
}

func (p *proof) newWorkspace(ctx context.Context, name string, initialize bool, cp *api.ExecutionWorkspaceCheckpoint) (*api.ExecutionWorkspace, error) {
	w := &api.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace, Name: name}, Spec: api.ExecutionWorkspaceSpec{
		Slot:         "default",
		ClassBinding: p.class, ProviderBinding: api.ImmutableObjectBinding{Name: p.installed.Name, UID: p.installed.UID, Generation: p.installed.Generation},
		DesiredState: api.ExecutionWorkspaceDesiredReady, Mode: api.ExecutionWorkspaceModeInteractive,
		SessionRef: &api.ObjectIdentityReference{Name: name + "-session", UID: types.UID(name + "-session-fixture")},
		Lifecycle: api.ExecutionWorkspaceLifecycle{DefaultOnDetach: api.WorkspaceOnDetachSuspend, AllowedOnDetach: []api.WorkspaceOnDetach{api.WorkspaceOnDetachSuspend, api.WorkspaceOnDetachDelete},
			DetachTimeout: metav1.Duration{Duration: time.Minute}, MaxLifetime: &metav1.Duration{Duration: time.Hour},
			DeletionPolicy: api.ExecutionWorkspaceDeletionPolicy{ProviderResources: api.WorkspaceDeletionActionDelete, PersistentVolumes: api.WorkspaceDeletionActionDelete, Checkpoints: api.WorkspaceDeletionActionDelete}}}}
	if err := p.c.Create(ctx, w); err != nil {
		return nil, err
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(p.profile)
	if err != nil {
		return nil, err
	}
	raw["apiVersion"], raw["kind"] = profile.GroupVersion.String(), "SubstrateWorkspaceProfile"
	hash, err := sdk.ParametersProfileHash(&unstructured.Unstructured{Object: raw})
	if err != nil {
		return nil, err
	}
	nonce := randomValue()
	if initialize {
		nonce = p.sourceNonce
	}
	container := corev1.Container{Name: "runtime", Image: p.image, Command: []string{"/usr/local/bin/orka-acp-runtime"},
		Env: []corev1.EnvVar{{Name: "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE", Value: nonce},
			{Name: "ORKA_ACP_CREDENTIAL_BOOTSTRAP_PUBLIC_KEY", Value: randomValue()},
			{Name: "ORKA_ACP_DURABLE_WORKSPACE_DIR", Value: "/durable/orka-workspace"},
			{Name: "ORKA_ACP_DURABLE_WORKSPACE_KEY", Value: "shared"},
			{Name: "SUBSTRATE_E2E_INITIALIZE_NONCE", Value: p.sourceNonce}, {Name: "SUBSTRATE_E2E_MARKER", Value: p.marker}},
		VolumeMounts: []corev1.VolumeMount{{Name: "orka-workspace", MountPath: "/durable/orka-workspace"}}}
	request := &api.WorkloadRequest{Sequence: 1, Key: api.AllocationKey{Namespace: w.Namespace, Name: w.Name, WorkspaceUID: w.UID, ProviderUID: p.installed.UID},
		Image: container.Image, Command: container.Command, ParametersRef: &api.TypedObjectReference{Group: profile.GroupVersion.Group, Kind: "SubstrateWorkspaceProfile", Name: p.profile.Name},
		ParametersBinding: &api.ImmutableObjectBinding{Name: p.profile.Name, UID: p.profile.UID, Generation: p.profile.Generation, ProfileHash: hash},
		Runtime: &api.RuntimeWorkload{BootstrapPort: 80, PoolBinding: api.ImmutableObjectBinding{Name: name + "-pool", UID: types.UID(name + "-pool-fixture"), Generation: 1, ProfileHash: p.class.ProfileHash},
			ClassBinding: p.class, Protocol: "orka.harness.v2", ContainerName: container.Name, RequiredFeatures: []api.ExecutionWorkspaceFeature{api.WorkspaceFeatureSuspend, api.WorkspaceFeatureCheckpoint, api.WorkspaceFeatureRestore},
			Template:      corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace}, Spec: corev1.PodSpec{AutomountServiceAccountToken: new(false), RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{container}}},
			NetworkPolicy: &networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}, Egress: []networkingv1.NetworkPolicyEgressRule{{}}}}}
	if cp != nil {
		request.RestoreFrom = &api.WorkloadCheckpointReference{Name: cp.Name, UID: cp.UID, Digest: cp.Status.Digest}
	}
	request.Revision, err = api.WorkloadRevision(*request)
	if err != nil {
		return nil, err
	}
	w.Spec.Workload = request
	w.Spec.CoreAdmission = &api.ExecutionWorkspaceCoreAdmission{ClassBinding: w.Spec.ClassBinding, ProviderBinding: w.Spec.ProviderBinding, AdmittedGeneration: w.Generation + 1}
	if err := p.c.Update(ctx, w); err != nil {
		return nil, err
	}
	w.Status.Conditions = []metav1.Condition{{Type: string(api.ConditionWorkspaceAdmitted), Status: metav1.ConditionTrue, Reason: string(api.ReasonReady), ObservedGeneration: w.Generation, LastTransitionTime: metav1.Now()}}
	if err := p.c.Status().Update(ctx, w); err != nil {
		return nil, err
	}
	return w, nil
}

func (p *proof) verifyReady(ctx context.Context, w *api.ExecutionWorkspace) (challenge, error) {
	var public challenge
	observed := w.Status.Allocation
	if observed == nil {
		return public, fmt.Errorf("missing allocation")
	}
	if err := api.ValidateStartup(*w.Spec.Workload, *observed); err != nil {
		return public, err
	}
	process := observed.Startup.Process
	if process == nil || observed.Startup.Pod != nil {
		return public, fmt.Errorf("native process evidence missing")
	}
	actor, err := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: &pb.ObjectRef{Atespace: process.Namespace, Name: process.Name}})
	if err != nil {
		return public, err
	}
	if actor.Metadata.Uid != process.UID || actor.Metadata.Version != process.Version || actor.Status.State != pb.ActorState_ACTOR_STATE_RUNNING {
		return public, fmt.Errorf("native Actor UID/version fence differs")
	}
	worker, err := p.native.GetWorker(ctx, &pb.GetWorkerRequest{Worker: actor.Status.WorkerAssignment.Worker})
	if err != nil {
		return public, err
	}
	if worker.WorkerNamespace != process.Worker.Namespace || worker.WorkerPod != process.Worker.Name || worker.WorkerPodUid != string(process.Worker.UID) || worker.Status.Capacity.Actors != 1 {
		return public, fmt.Errorf("native exact worker/capacity fence differs")
	}
	pod := &corev1.Pod{}
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: process.Worker.Namespace, Name: process.Worker.Name}, pod); err != nil {
		return public, err
	}
	if pod.UID != process.Worker.UID || pod.Labels["substrate.workspace.orka.ai/allocation-id"] != observed.Identity.AllocationID || pod.Labels["substrate.workspace.orka.ai/instance-id"] != observed.Identity.InstanceID {
		return public, fmt.Errorf("real worker Pod UID/private labels differ")
	}
	if err := getJSONWithDNSRetry(ctx, observed.Startup.Endpoint+"/v2/credential-bootstrap", &public); err != nil {
		return public, err
	}
	canonical, err := json.Marshal(public)
	if err != nil {
		return public, err
	}
	if public.Actor.UID != process.UID || public.Actor.Name != process.Name || public.Actor.Atespace != process.Namespace || digest(canonical) != process.ChallengeSHA256 {
		return public, fmt.Errorf("direct public challenge differs from provider evidence")
	}
	again, err := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: actorRef(actor)})
	if err != nil || again.GetMetadata().GetUid() != actor.Metadata.Uid || again.GetMetadata().GetVersion() != actor.Metadata.Version || !proto.Equal(again.GetStatus().GetWorkerAssignment(), actor.Status.WorkerAssignment) {
		return public, fmt.Errorf("native fence changed after direct challenge: %v", err)
	}
	template, err := p.native.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: actor.ActorTemplate})
	if err != nil {
		return public, err
	}
	if template.Metadata.Uid != actor.Status.CurrentActorTemplateUid || template.SnapshotsConfig.OnPause != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || template.SnapshotsConfig.OnCommit != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || template.SnapshotsConfig.OnResume.FromData != pb.ResumeSource_RESUME_SOURCE_COLD_BOOT {
		return public, fmt.Errorf("full-memory gate or exact template fence differs")
	}
	var data struct {
		Marker    string `json:"marker"`
		BootNonce string `json:"bootNonce"`
	}
	if err := getJSONWithDNSRetry(ctx, observed.Startup.Endpoint+"/proof/data", &data); err != nil {
		return public, err
	}
	if data.Marker != p.marker || data.BootNonce != public.BootNonce {
		return public, fmt.Errorf("durable marker or process boot differs")
	}
	return public, nil
}
func actorRef(actor *pb.Actor) *pb.ObjectRef {
	return &pb.ObjectRef{Atespace: actor.Metadata.Atespace, Name: actor.Metadata.Name}
}

func (p *proof) retire(ctx context.Context, w *api.ExecutionWorkspace, action api.WorkloadRetirementAction) error {
	w.Spec.Retirement = &api.WorkloadRetirement{Sequence: w.Spec.Workload.Sequence, Identity: w.Status.Allocation.Identity, Action: action}
	if action == api.WorkloadRetirementSuspend {
		w.Spec.DesiredState = api.ExecutionWorkspaceDesiredSuspended
	} else {
		w.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	}
	if err := p.c.Update(ctx, w); err != nil {
		return err
	}
	target := api.AllocationStopped
	if action == api.WorkloadRetirementDelete {
		target = api.AllocationDeleted
	}
	return p.reconcileWorkspace(ctx, w, target)
}

func (p *proof) run(ctx context.Context) error {
	p.basePool = &unstructured.Unstructured{}
	p.basePool.SetAPIVersion("ate.dev/v1alpha1")
	p.basePool.SetKind("WorkerPool")
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: "native-data"}, p.basePool); err != nil {
		return err
	}
	p.installed = &api.ExecutionWorkspaceProvider{}
	if err := p.c.Get(ctx, client.ObjectKey{Name: "substrate"}, p.installed); err != nil {
		return err
	}
	_, err := p.native.CreateAtespace(ctx, &pb.CreateAtespaceRequest{Atespace: &pb.Atespace{Metadata: &pb.ResourceMetadata{Name: p.namespace}}})
	if err != nil {
		return err
	}
	_, err = p.native.CreateActorTemplate(ctx, &pb.CreateActorTemplateRequest{ActorTemplate: &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: p.namespace, Name: "infrastructure"},
		Containers:      []*pb.Container{{Name: "runtime", Image: p.image}},
		WorkerSelector:  &pb.Selector{MatchLabels: map[string]string{"orka.workspace.e2e/pool": p.namespace}},
		SandboxConfig:   &pb.SandboxConfig{SandboxClass: pb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		SnapshotsConfig: &pb.SnapshotsConfig{StorageLocation: "s3://ate-snapshots/" + p.namespace + "/"}}})
	if err != nil {
		return err
	}
	p.profile = &profile.SubstrateWorkspaceProfile{TypeMeta: metav1.TypeMeta{APIVersion: profile.GroupVersion.String(), Kind: "SubstrateWorkspaceProfile"}, ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace, Name: "data"}, Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Namespace: p.namespace, Name: "infrastructure"}, Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly}}}
	if err := p.c.Create(ctx, p.profile); err != nil {
		return err
	}
	source, err := p.newWorkspace(ctx, "source", true, nil)
	if err != nil {
		return err
	}
	if err := p.reconcileWorkspace(ctx, source, api.AllocationReady); err != nil {
		return err
	}
	first, err := p.verifyReady(ctx, source)
	if err != nil {
		return err
	}
	firstProcess := source.Status.Allocation.Startup.Process.DeepCopy()
	p.passed("native Actor wrote durable marker; exact Actor version, single-capacity worker Pod and public challenge verified")
	if err := p.retire(ctx, source, api.WorkloadRetirementSuspend); err != nil {
		return err
	}
	if source.Status.State != api.ExecutionWorkspaceStateSuspended || source.Status.Allocation.RetainedData == nil {
		return fmt.Errorf("suspension omitted verified retained data")
	}
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: firstProcess.Worker.Namespace, Name: firstProcess.Worker.Name}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		return fmt.Errorf("source exact worker still present: %v", err)
	}
	if _, err := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: &pb.ObjectRef{Atespace: firstProcess.Namespace, Name: firstProcess.Name}}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("source Actor remains after suspension: %v", err)
	}
	p.passed("actual Data capture completed before exact source worker Pod and Actor termination")
	cp := &api.ExecutionWorkspaceCheckpoint{ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace, Name: "independent"}, Spec: api.ExecutionWorkspaceCheckpointSpec{WorkspaceRef: api.ObjectIdentityReference{Name: source.Name, UID: source.UID}}}
	if err := p.c.Create(ctx, cp); err != nil {
		return err
	}
	checkpointController := &provider.CheckpointReconciler{Client: p.c, Control: p.native, Config: p.config}
	if err := poll(ctx, func() (bool, error) {
		if _, err := checkpointController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			return false, err
		}
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
			return false, err
		}
		return cp.Status.Phase == "Ready", nil
	}); err != nil {
		return err
	}
	if cp.Status.Digest == "" || cp.Status.ClassBinding == nil || *cp.Status.ClassBinding != p.class {
		return fmt.Errorf("public export provenance missing")
	}
	if err := p.retire(ctx, source, api.WorkloadRetirementDelete); err != nil {
		return err
	}
	if err := p.c.Delete(ctx, source, client.Preconditions{UID: &source.UID}); err != nil {
		return err
	}
	if err := poll(ctx, func() (bool, error) {
		err := p.c.Get(ctx, client.ObjectKeyFromObject(source), &api.ExecutionWorkspace{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	}); err != nil {
		return err
	}
	p.passed("independent checkpoint retained native Data after source allocation and public workspace deletion")
	fork, err := p.newWorkspace(ctx, "imported", false, cp)
	if err != nil {
		return err
	}
	if err := p.reconcileWorkspace(ctx, fork, api.AllocationReady); err != nil {
		return err
	}
	second, err := p.verifyReady(ctx, fork)
	if err != nil {
		return err
	}
	secondProcess := fork.Status.Allocation.Startup.Process
	if secondProcess.UID == firstProcess.UID || secondProcess.Worker.UID == firstProcess.Worker.UID || second.BootNonce == first.BootNonce || second.PublicKey == first.PublicKey || second.Nonce == first.Nonce || secondProcess.ChallengeSHA256 == firstProcess.ChallengeSHA256 {
		return fmt.Errorf("import did not rotate Actor, worker, process or bootstrap incarnation")
	}
	p.passed("cold import into fresh Actor/process read original marker without initializing data; challenge and exact worker UID rotated")
	if err := p.c.Delete(ctx, cp, client.Preconditions{UID: &cp.UID}); err != nil {
		return err
	}
	if err := poll(ctx, func() (bool, error) {
		if _, err := checkpointController.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			return false, err
		}
		err := p.c.Get(ctx, client.ObjectKeyFromObject(cp), &api.ExecutionWorkspaceCheckpoint{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	}); err != nil {
		return err
	}
	if _, err := p.verifyReady(ctx, fork); err != nil {
		return err
	}
	p.passed("import acquired durable ownership before boot; independent public checkpoint deletion preserved inherited Data")
	if err := p.retire(ctx, fork, api.WorkloadRetirementDelete); err != nil {
		return err
	}
	tags, err := p.native.ListTags(ctx, &pb.ListTagsRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil {
		return err
	}
	if len(tags.Tags) != 0 {
		return fmt.Errorf("last owner retains %d native Tags", len(tags.Tags))
	}
	templates, err := p.native.ListActorTemplates(ctx, &pb.ListActorTemplatesRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil {
		return err
	}
	if len(templates.ActorTemplates) != 1 || templates.ActorTemplates[0].Metadata.Name != "infrastructure" {
		return fmt.Errorf("provider-owned native templates remain after deletion")
	}
	p.passed("last owner deletion collected native Tags and runtime templates; full-memory restoration stayed closed")
	report := map[string]any{"nativeCommit": provider.UpstreamCommit, "namespace": p.namespace, "checks": p.checks, "dataSHA256": digest([]byte(p.marker)),
		"runtimeImage": p.image, "proofImage": os.Getenv("PROOF_IMAGE"), "workerImage": os.Getenv("WORKER_IMAGE"),
		"sourceProcess": firstProcess, "importedProcess": secondProcess, "checkpointUID": cp.UID, "checkpointDigest": cp.Status.Digest,
		"runtime":       "deterministic native Data/process challenge fixture; no authenticated ACP execution claim",
		"coreAdmission": "constructed fixture; core integration and ownership admission verified separately",
		"networkPolicy": "exact worker selection verified; kind CNI does not prove packet enforcement"}
	encoded, err := json.Marshal(report)
	if err != nil {
		return err
	}
	fmt.Println("NATIVE_DATA_PROOF", string(encoded))
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	config, err := rest.InClusterConfig()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, api.AddToScheme, profile.AddToScheme} {
		if err == nil {
			err = add(scheme)
		}
	}
	var c client.Client
	if err == nil {
		c, err = client.New(config, client.Options{Scheme: scheme})
	}
	transport := provider.Config{APIEndpoint: "api.ate-system.svc:443", CAFile: "/native/server/trust-bundle.pem", CertFile: "/native/client/credential-bundle.pem", KeyFile: "/native/client/credential-bundle.pem", ActorDNSSuffix: "actors.resources.substrate.ate.dev", DirectEgressEnabled: true}
	if err == nil {
		conn, dialErr := provider.Dial(transport)
		err = dialErr
		if err == nil {
			defer conn.Close()
			p := &proof{c: c, native: pb.NewControlClient(conn), config: transport, namespace: os.Getenv("POD_NAMESPACE"), image: os.Getenv("RUNTIME_IMAGE"), marker: randomValue(), sourceNonce: randomValue(),
				class: api.ImmutableObjectBinding{Name: "native-data-class", UID: "native-data-class-fixture", Generation: 1, ProfileHash: "sha256:" + strings.Repeat("a", 64)}}
			p.native = &guardedControl{ControlClient: p.native, proof: p}
			if os.Getenv("SUBSTRATE_E2E_CATALOG_NAMESPACE") != "" {
				err = p.collectCatalog(ctx, os.Getenv("SUBSTRATE_E2E_CATALOG_NAMESPACE"), os.Getenv("SUBSTRATE_E2E_CATALOG_NAME"), os.Getenv("SUBSTRATE_E2E_CATALOG_UID"))
			} else if os.Getenv("SUBSTRATE_E2E_CLEANUP") == "1" {
				err = p.cleanup(ctx)
			} else {
				err = p.run(ctx)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "native Data proof failed:", err)
		os.Exit(1)
	}
}
