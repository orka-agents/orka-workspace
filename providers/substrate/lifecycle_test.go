package substrate

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type nativeFixture struct {
	pb.ControlClient
	client                   client.Client
	templates                map[string]*pb.ActorTemplate
	actors                   map[string]*pb.Actor
	workers                  map[string]*pb.Worker
	tags                     map[string]*pb.Tag
	boots, suspends, deletes int
	failAfter                string
	foreignAssignment        bool
	challengeSuffix          string
	beforeResume             func(context.Context, *pb.Actor) error
}

func (f *nativeFixture) effect(name string) error {
	if f.failAfter == name {
		f.failAfter = ""
		return status.Error(codes.Unavailable, "response lost")
	}
	return nil
}
func cloneTemplate(in *pb.ActorTemplate) *pb.ActorTemplate {
	return proto.Clone(in).(*pb.ActorTemplate)
}
func refKey(ref *pb.ObjectRef) string          { return ref.GetAtespace() + "/" + ref.GetName() }
func metaKey(meta *pb.ResourceMetadata) string { return meta.GetAtespace() + "/" + meta.GetName() }
func (f *nativeFixture) GetActorTemplate(_ context.Context, req *pb.GetActorTemplateRequest, _ ...grpc.CallOption) (*pb.ActorTemplate, error) {
	v := f.templates[refKey(req.ActorTemplate)]
	if v == nil {
		return nil, status.Error(codes.NotFound, "template absent")
	}
	return cloneTemplate(v), nil
}
func (f *nativeFixture) CreateActorTemplate(_ context.Context, req *pb.CreateActorTemplateRequest, _ ...grpc.CallOption) (*pb.ActorTemplate, error) {
	key := metaKey(req.ActorTemplate.Metadata)
	if f.templates[key] != nil {
		return nil, status.Error(codes.AlreadyExists, "template exists")
	}
	v := cloneTemplate(req.ActorTemplate)
	v.Metadata.Uid = "template-" + v.Metadata.Name
	v.Metadata.Version = 1
	f.templates[key] = v
	return cloneTemplate(v), f.effect("template")
}
func (f *nativeFixture) DeleteActorTemplate(_ context.Context, req *pb.DeleteActorTemplateRequest, _ ...grpc.CallOption) (*pb.ActorTemplate, error) {
	key := refKey(req.ActorTemplate)
	v := f.templates[key]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	delete(f.templates, key)
	return cloneTemplate(v), f.effect("deleteTemplate")
}
func (f *nativeFixture) GetActor(_ context.Context, req *pb.GetActorRequest, _ ...grpc.CallOption) (*pb.Actor, error) {
	v := f.actors[refKey(req.Actor)]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return proto.Clone(v).(*pb.Actor), nil
}
func (f *nativeFixture) CreateActor(_ context.Context, req *pb.CreateActorRequest, _ ...grpc.CallOption) (*pb.Actor, error) {
	key := metaKey(req.Actor.Metadata)
	if f.actors[key] != nil {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	v := proto.Clone(req.Actor).(*pb.Actor)
	v.Metadata.Uid = "actor-" + v.Metadata.Name
	v.Metadata.Version = 1
	v.Status = &pb.ActorStatus{State: pb.ActorState_ACTOR_STATE_SUSPENDED}
	if v.SourceTag != nil {
		tag := f.tags[refKey(v.SourceTag)]
		if tag == nil {
			return nil, status.Error(codes.FailedPrecondition, "tag absent")
		}
		v.Status.CurrentActorTemplateUid = tag.Status.ActorTemplateUid
		v.Status.ExternalSnapshot = proto.Clone(tag.Status.Snapshot).(*pb.ExternalSnapshot)
	}
	f.actors[key] = v
	return proto.Clone(v).(*pb.Actor), f.effect("actor")
}
func (f *nativeFixture) UpdateActor(_ context.Context, req *pb.UpdateActorRequest, _ ...grpc.CallOption) (*pb.Actor, error) {
	key := metaKey(req.Actor.Metadata)
	v := f.actors[key]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	if v.Metadata.Uid != req.Actor.Metadata.Uid || v.Metadata.Version != req.Actor.Metadata.Version {
		return nil, status.Error(codes.Aborted, "CAS failed")
	}
	v = proto.Clone(req.Actor).(*pb.Actor)
	v.Metadata.Version++
	f.actors[key] = v
	return proto.Clone(v).(*pb.Actor), f.effect("update")
}
func (f *nativeFixture) ResumeActor(ctx context.Context, req *pb.ResumeActorRequest, _ ...grpc.CallOption) (*pb.ResumeActorResponse, error) {
	v := f.actors[refKey(req.Actor)]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	if v.Status.State != pb.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, status.Error(codes.FailedPrecondition, "not suspended")
	}
	if f.beforeResume != nil {
		if err := f.beforeResume(ctx, v); err != nil {
			return nil, err
		}
	}
	f.boots++
	var w *pb.Worker
	for _, candidate := range f.workers {
		if candidate.Status.State != pb.WorkerState_WORKER_STATE_ACTIVE {
			continue
		}
		matches := true
		for key, value := range f.templates[refKey(v.ActorTemplate)].WorkerSelector.MatchLabels {
			if candidate.Labels[key] != value {
				matches = false
			}
		}
		if matches {
			w = candidate
			break
		}
	}
	if w == nil {
		return nil, status.Error(codes.Unavailable, "private worker not prepared")
	}
	v.Metadata.Version++
	v.Status.State = pb.ActorState_ACTOR_STATE_RUNNING
	v.Status.CurrentActorTemplateUid = f.templates[refKey(v.ActorTemplate)].Metadata.Uid
	v.Status.WorkerAssignment = &pb.WorkerAssignment{Worker: &pb.ObjectRef{Name: w.Metadata.Name}, WorkerNamespace: w.WorkerNamespace, WorkerPool: w.WorkerPool, WorkerPod: w.WorkerPod, WorkerPodUid: w.WorkerPodUid}
	return &pb.ResumeActorResponse{Actor: proto.Clone(v).(*pb.Actor)}, f.effect("resume")
}
func (f *nativeFixture) SuspendActor(_ context.Context, req *pb.SuspendActorRequest, _ ...grpc.CallOption) (*pb.SuspendActorResponse, error) {
	v := f.actors[refKey(req.Actor)]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	f.suspends++
	v.Metadata.Version++
	v.Status.State = pb.ActorState_ACTOR_STATE_SUSPENDED
	v.Status.WorkerAssignment = nil
	v.Status.ExternalSnapshot = &pb.ExternalSnapshot{SnapshotUri: fmt.Sprintf("gs://native/data/%s/%d", v.Metadata.Uid, f.suspends), ContentScope: pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA}
	return &pb.SuspendActorResponse{Actor: proto.Clone(v).(*pb.Actor)}, f.effect("suspend")
}
func (f *nativeFixture) DeleteActor(_ context.Context, req *pb.DeleteActorRequest, _ ...grpc.CallOption) (*pb.Actor, error) {
	key := refKey(req.Actor)
	v := f.actors[key]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	delete(f.actors, key)
	f.deletes++
	return proto.Clone(v).(*pb.Actor), f.effect("deleteActor")
}
func (f *nativeFixture) GetWorker(_ context.Context, req *pb.GetWorkerRequest, _ ...grpc.CallOption) (*pb.Worker, error) {
	v := f.workers[req.Worker.Name]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return proto.Clone(v).(*pb.Worker), nil
}
func (f *nativeFixture) ListWorkers(ctx context.Context, _ *pb.ListWorkersRequest, _ ...grpc.CallOption) (*pb.ListWorkersResponse, error) {
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1alpha1")
	pools.SetKind("WorkerPoolList")
	if err := f.client.List(ctx, pools); err != nil {
		return nil, err
	}
	for _, pool := range pools.Items {
		if pool.GetLabels()[workerInstanceLabel] == "" || pool.GetDeletionTimestamp() != nil {
			continue
		}
		name := pool.GetName() + "-worker"
		if f.workers[name] != nil {
			continue
		}
		labels, _, err := unstructured.NestedStringMap(pool.Object, "spec", "template", "labels")
		if err != nil {
			return nil, err
		}
		labels[workerPoolLabel] = pool.GetName()
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pool.GetNamespace(), Name: name, UID: types.UID(name + "-uid"), Labels: labels}}
		if err := f.client.Create(ctx, pod); err != nil {
			return nil, err
		}
		f.workers[name] = &pb.Worker{Metadata: &pb.ResourceMetadata{Name: name, Uid: name + "-native-uid", Version: 1}, Labels: maps.Clone(pool.GetLabels()), WorkerNamespace: pod.Namespace, WorkerPool: pool.GetName(), WorkerPod: pod.Name, WorkerPodUid: string(pod.UID), Status: &pb.WorkerStatus{State: pb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &pb.WorkerResources{Actors: 1}}}
	}
	out := &pb.ListWorkersResponse{}
	for _, worker := range f.workers {
		out.Workers = append(out.Workers, proto.Clone(worker).(*pb.Worker))
	}
	return out, nil
}
func (f *nativeFixture) DrainWorker(_ context.Context, req *pb.DrainWorkerRequest, _ ...grpc.CallOption) (*pb.Worker, error) {
	v := f.workers[req.Worker.Name]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	v.Status.State = pb.WorkerState_WORKER_STATE_DRAINING
	return proto.Clone(v).(*pb.Worker), f.effect("drain")
}
func (f *nativeFixture) ListWorkerActorAssignments(_ context.Context, req *pb.ListWorkerActorAssignmentsRequest, _ ...grpc.CallOption) (*pb.ListWorkerActorAssignmentsResponse, error) {
	out := &pb.ListWorkerActorAssignmentsResponse{}
	for _, v := range f.actors {
		if v.Status.GetWorkerAssignment().GetWorker().GetName() == req.Worker.Name {
			out.ActorAssignments = append(out.ActorAssignments, &pb.ActorAssignment{Actor: &pb.ObjectRef{Atespace: v.Metadata.Atespace, Name: v.Metadata.Name}, ActorUid: v.Metadata.Uid})
		}
	}
	if f.foreignAssignment {
		out.ActorAssignments = append(out.ActorAssignments, &pb.ActorAssignment{Actor: &pb.ObjectRef{Atespace: "foreign", Name: "foreign"}, ActorUid: "foreign"})
	}
	return out, nil
}
func (f *nativeFixture) CreateTag(_ context.Context, req *pb.CreateTagRequest, _ ...grpc.CallOption) (*pb.Tag, error) {
	key := metaKey(req.Tag.Metadata)
	if f.tags[key] != nil {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	actor := f.actors[refKey(req.Tag.SourceActor)]
	if actor == nil {
		return nil, status.Error(codes.NotFound, "actor absent")
	}
	v := proto.Clone(req.Tag).(*pb.Tag)
	v.Metadata.Uid = "tag-" + v.Metadata.Name
	v.Status = &pb.TagStatus{Snapshot: proto.Clone(actor.Status.ExternalSnapshot).(*pb.ExternalSnapshot), SourceActorUid: actor.Metadata.Uid, ActorTemplateUid: actor.Status.CurrentActorTemplateUid}
	f.tags[key] = v
	return proto.Clone(v).(*pb.Tag), f.effect("tag")
}
func (f *nativeFixture) GetTag(_ context.Context, req *pb.GetTagRequest, _ ...grpc.CallOption) (*pb.Tag, error) {
	v := f.tags[refKey(req.Tag)]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return proto.Clone(v).(*pb.Tag), nil
}
func (f *nativeFixture) DeleteTag(_ context.Context, req *pb.DeleteTagRequest, _ ...grpc.CallOption) (*pb.Tag, error) {
	key := refKey(req.Tag)
	v := f.tags[key]
	if v == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	delete(f.tags, key)
	return proto.Clone(v).(*pb.Tag), f.effect("deleteTag")
}
func (f *nativeFixture) Get(_ context.Context, endpoint string) (bootstrapChallenge, error) {
	actorName := strings.Split(strings.TrimPrefix(endpoint, "http://"), ".")[0]
	var out bootstrapChallenge
	for _, actor := range f.actors {
		if actor.Metadata.Name != actorName {
			continue
		}
		out.Schema = "orka.harness.v2/sealed-bootstrap/v1"
		out.Actor.Atespace = actor.Metadata.Atespace
		out.Actor.Name = actorName
		out.Actor.UID = actor.Metadata.Uid
		for _, env := range f.templates[refKey(actor.ActorTemplate)].Containers[0].Env {
			if env.Name == "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE" {
				out.Nonce = env.Value
			}
		}
		out.PublicKey = base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
		out.BootNonce = base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("b", 32)))
		if f.challengeSuffix != "" {
			out.Nonce += f.challengeSuffix
		}
		return out, nil
	}
	return out, fmt.Errorf("no process")
}

func fixture(t *testing.T, suspend bool) (client.Client, *nativeFixture, sdk.WorkloadRequest) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, api.AddToScheme, profile.AddToScheme} {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	class := api.ImmutableObjectBinding{Name: "class", UID: "class-uid", Generation: 1, ProfileHash: "sha256:" + strings.Repeat("a", 64)}
	provider := &api.ExecutionWorkspaceProvider{ObjectMeta: metav1.ObjectMeta{Name: "substrate", UID: "provider-uid", Generation: 1}, Spec: api.ExecutionWorkspaceProviderSpec{ControllerName: ControllerName, LifecycleState: api.ExecutionWorkspaceProviderActive}}
	workspace := &api.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "workspace", UID: "workspace-uid", Generation: 1}, Spec: api.ExecutionWorkspaceSpec{ClassBinding: class, ProviderBinding: api.ImmutableObjectBinding{Name: provider.Name, UID: provider.UID, Generation: 1}, DesiredState: api.ExecutionWorkspaceDesiredReady}}
	workspace.Spec.Mode = api.ExecutionWorkspaceModeInteractive
	workspace.Spec.Lifecycle.DeletionPolicy = api.ExecutionWorkspaceDeletionPolicy{ProviderResources: api.WorkspaceDeletionActionDelete, PersistentVolumes: api.WorkspaceDeletionActionDelete, Checkpoints: api.WorkspaceDeletionActionDelete}
	if suspend {
		workspace.Spec.SessionRef = &api.ObjectIdentityReference{Name: "session", UID: "session-uid"}
		workspace.Spec.Lifecycle.AllowedOnDetach = []api.WorkspaceOnDetach{api.WorkspaceOnDetachSuspend}
		workspace.Spec.Lifecycle.MaxLifetime = &metav1.Duration{Duration: time.Hour}
	}
	workspace.Spec.CoreAdmission = &api.ExecutionWorkspaceCoreAdmission{ClassBinding: class, ProviderBinding: workspace.Spec.ProviderBinding, AdmittedGeneration: 1}
	workspace.Status.Conditions = []metav1.Condition{{Type: string(api.ConditionWorkspaceAdmitted), Status: metav1.ConditionTrue, Reason: string(api.ReasonReady), ObservedGeneration: 1, LastTransitionTime: metav1.Now()}}
	parameter := &profile.SubstrateWorkspaceProfile{TypeMeta: metav1.TypeMeta{APIVersion: profile.GroupVersion.String(), Kind: "SubstrateWorkspaceProfile"}, ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "native", UID: "parameter-uid", Generation: 1}, Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Name: "base", Namespace: "native"}}}
	if suspend {
		parameter.Spec.Suspend = &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly}
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(parameter)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := sdk.ParametersProfileHash(&unstructured.Unstructured{Object: raw})
	if err != nil {
		t.Fatal(err)
	}
	image := "fixture.invalid/runtime@sha256:" + strings.Repeat("0", 64)
	container := corev1.Container{Name: "runtime", Image: image, Command: []string{"/usr/local/bin/orka-acp-runtime"}, Env: []corev1.EnvVar{{Name: "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE", Value: "public-nonce"}, {Name: "ORKA_ACP_CREDENTIAL_BOOTSTRAP_PUBLIC_KEY", Value: "public-key"}, {Name: "ORKA_ACP_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}}}}
	if suspend {
		container.Env = append(container.Env, corev1.EnvVar{Name: "ORKA_ACP_DURABLE_WORKSPACE_DIR", Value: durableMountPath}, corev1.EnvVar{Name: "ORKA_ACP_DURABLE_WORKSPACE_KEY", Value: "shared"})
		container.VolumeMounts = []corev1.VolumeMount{{Name: durableVolumeName, MountPath: durableMountPath}}
	}
	request := sdk.WorkloadRequest{Sequence: 1, Key: sdk.AllocationKey{Namespace: workspace.Namespace, Name: workspace.Name, WorkspaceUID: workspace.UID, ProviderUID: provider.UID}, Image: image, Command: container.Command, ParametersRef: &api.TypedObjectReference{Group: profile.GroupVersion.Group, Kind: "SubstrateWorkspaceProfile", Name: parameter.Name}, ParametersBinding: &api.ImmutableObjectBinding{Name: parameter.Name, UID: parameter.UID, Generation: 1, ProfileHash: hash}, Runtime: &sdk.RuntimeWorkload{BootstrapPort: 80, PoolBinding: api.ImmutableObjectBinding{Name: "pool", UID: "pool-uid", Generation: 1, ProfileHash: class.ProfileHash}, ClassBinding: class, Protocol: "orka.harness.v2", ContainerName: container.Name, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Namespace: "runtime"}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: new(bool), Containers: []corev1.Container{container}}}, NetworkPolicy: &networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}}}}
	request.Revision, _ = sdk.WorkloadRevision(request)
	workspace.Spec.Workload = &request
	pool := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ate.dev/v1alpha1", "kind": "WorkerPool", "metadata": map[string]any{"namespace": "native-workers", "name": "native-workers", "uid": "worker-pool-uid", "labels": map[string]any{"selected": "native"}}, "spec": map[string]any{"replicas": int64(3), "workerImage": "fixture.invalid/native-worker:pin", "sandboxClass": "gvisor"}}}
	uid := 0
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(provider, workspace, parameter, pool).WithStatusSubresource(workspace, provider, &api.ExecutionWorkspaceCheckpoint{}).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if obj.GetUID() == "" {
			uid++
			obj.SetUID(types.UID(fmt.Sprintf("api-uid-%d", uid)))
		}
		return c.Create(ctx, obj, opts...)
	}}).Build()
	native := &nativeFixture{client: c, templates: map[string]*pb.ActorTemplate{"native/base": {Metadata: &pb.ResourceMetadata{Atespace: "native", Name: "base", Uid: "base-uid"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{"selected": "native"}}, SandboxConfig: &pb.SandboxConfig{SandboxClass: pb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor"}, SnapshotsConfig: &pb.SnapshotsConfig{StorageLocation: "gs://native/snapshots"}}}, actors: map[string]*pb.Actor{}, workers: map[string]*pb.Worker{}, tags: map[string]*pb.Tag{}}
	return c, native, request
}
func driver(c client.Client, native *nativeFixture) *Lifecycle {
	d := New(c, native, Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true})
	d.challengeClient = native
	return d
}
func ready(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest) sdk.AllocationObservation {
	t.Helper()
	for range 12 {
		observed, err := driver(c, native).EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			return observed
		}
	}
	t.Fatal("never ready")
	return sdk.AllocationObservation{}
}
func retired(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest, identity sdk.InstanceIdentity, suspend bool) sdk.AllocationObservation {
	t.Helper()
	for range 12 {
		var out sdk.AllocationObservation
		var err error
		if suspend {
			out, err = driver(c, native).SuspendInstance(t.Context(), request.Key, identity)
		} else {
			out, err = driver(c, native).StopInstance(t.Context(), request.Key, identity)
		}
		if err != nil {
			t.Fatal(err)
		}
		if out.State == sdk.AllocationStopped {
			return out
		}
	}
	t.Fatal("never stopped")
	return sdk.AllocationObservation{}
}
func admit(c client.Client) conformance.AdmitRequest {
	return func(ctx context.Context, request sdk.WorkloadRequest) error {
		w := &api.ExecutionWorkspace{}
		if err := c.Get(ctx, clientKey(request.Key), w); err != nil {
			return err
		}
		w.Spec.Workload = &request
		w.Spec.Retirement = nil
		return c.Update(ctx, w)
	}
}
func TestLifecycleConformance(t *testing.T) {
	for _, test := range []string{"lifecycle", "replacement", "suspension"} {
		t.Run(test, func(t *testing.T) {
			c, native, request := fixture(t, test == "suspension")
			factory := func() sdk.Lifecycle { return driver(c, native) }
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var err error
			switch test {
			case "lifecycle":
				err = conformance.Check(ctx, factory, request)
			case "replacement":
				err = conformance.CheckReplacement(ctx, factory, request, admit(c))
			case "suspension":
				err = conformance.CheckSuspension(ctx, factory, request, admit(c))
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLostNativeResponsesRecoverSameLifetime(t *testing.T) {
	for _, operation := range []string{"template", "actor", "resume"} {
		t.Run(operation, func(t *testing.T) {
			c, native, request := fixture(t, false)
			native.failAfter = operation
			_, err := driver(c, native).EnsureAllocation(t.Context(), request)
			if err == nil {
				t.Fatal("expected lost response")
			}
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			identity := record.Observation.Identity
			observed := ready(t, c, native, request)
			if observed.Identity != identity || native.boots != 1 || len(native.actors) != 1 {
				t.Fatalf("lost response changed lifetime: %#v, boots=%d", observed, native.boots)
			}
		})
	}
	for _, operation := range []string{"suspend", "tag", "deleteActor"} {
		t.Run(operation, func(t *testing.T) {
			c, native, request := fixture(t, true)
			first := ready(t, c, native, request)
			native.failAfter = operation
			lost := false
			var stopped sdk.AllocationObservation
			for range 16 {
				observed, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity)
				if err != nil {
					if lost {
						t.Fatal(err)
					}
					lost = true
					continue
				}
				if observed.State == sdk.AllocationStopped {
					stopped = observed
					break
				}
			}
			if !lost || stopped.RetainedData == nil || native.suspends != 1 || native.deletes != 1 {
				t.Fatalf("replayed uncertain mutation or lost data: %#v, suspends=%d, deletes=%d", stopped, native.suspends, native.deletes)
			}
		})
	}
}

func TestNativeJournalLossStopsAdmissionAndCleanup(t *testing.T) {
	c, native, request := fixture(t, false)
	first := ready(t, c, native, request)
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("journal loss recreated workload")
	}
	w := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(request.Key), w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	w.Spec.Retirement = &api.WorkloadRetirement{Sequence: 1, Identity: first.Identity, Action: api.WorkloadRetirementDelete}
	w.Status.Allocation = &first
	if err := c.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
	if _, err := r.reconcileLifecycle(t.Context(), w); err == nil {
		t.Fatal("journal loss authorized deletion")
	}
	if len(native.actors) != 1 || native.deletes != 0 {
		t.Fatal("journal loss mutated native workload")
	}
}

func TestStartupRejectsReplacedWorkerActorTemplateAndProcess(t *testing.T) {
	for _, change := range []string{"actorUID", "workerUID", "workerPodUID", "workerCapacity", "templateUID", "memoryPolicy", "process", "version", "networkPolicy"} {
		t.Run(change, func(t *testing.T) {
			c, native, request := fixture(t, false)
			ready(t, c, native, request)
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			actor := native.actors[record.Atespace+"/"+record.Actor.Name]
			worker := native.workers[record.Worker.Name]
			template := native.templates[record.Atespace+"/"+record.Template.Name]
			switch change {
			case "actorUID":
				actor.Metadata.Uid += "foreign"
			case "workerUID":
				worker.Metadata.Uid += "foreign"
			case "workerPodUID":
				worker.WorkerPodUid += "foreign"
			case "workerCapacity":
				worker.Status.Capacity.Actors = 2
			case "templateUID":
				template.Metadata.Uid += "foreign"
			case "memoryPolicy":
				template.SnapshotsConfig.OnPause = pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
			case "process":
				native.challengeSuffix = "another-process"
			case "version":
				actor.Metadata.Version++
			case "networkPolicy":
				policy := &networkingv1.NetworkPolicy{}
				if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Placement.Namespace, Name: record.NetworkPolicy.Name}, policy); err != nil {
					t.Fatal(err)
				}
				policy.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
				if err := c.Update(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
			}
			var errObserve error
			if change == "networkPolicy" {
				_, errObserve = driver(c, native).EnsureAllocation(t.Context(), request)
			} else {
				_, errObserve = driver(c, native).Observe(t.Context(), request.Key)
			}
			if errObserve == nil {
				t.Fatal("changed exact instance passed attestation")
			}
		})
	}
}

func TestSuspensionRequiresExactWorkerAbsenceAndNoForeignAssignment(t *testing.T) {
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	native.foreignAssignment = true
	if _, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
		t.Fatal("shared worker teardown accepted")
	}
	if native.suspends != 0 {
		t.Fatal("shared worker checkpoint mutation occurred")
	}
	native.foreignAssignment = false
	for range 3 {
		observed, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationStopped {
			t.Fatal("suspension reported before exact worker absence")
		}
	}
	stopped := retired(t, c, native, request, first.Identity, true)
	if stopped.RetainedData == nil {
		t.Fatal("no retained lineage")
	}
}

func TestColdResumeRequiresTagProvenanceAndFreshBoot(t *testing.T) {
	for _, change := range []string{"tagUID", "sourceUID", "templateUID", "scope", "snapshotScope", "valid"} {
		t.Run(change, func(t *testing.T) {
			c, native, request := fixture(t, true)
			first := ready(t, c, native, request)
			stopped := retired(t, c, native, request, first.Identity, true)
			next := *request.DeepCopy()
			next.Sequence++
			next.PreviousInstance = &first.Identity
			next.RetainedData = stopped.RetainedData
			next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
			next.Revision, _ = sdk.WorkloadRevision(next)
			if err := admit(c)(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			tag := native.tags[record.Checkpoint.Atespace+"/"+record.Checkpoint.Tag.Name]
			switch change {
			case "tagUID":
				tag.Metadata.Uid += "foreign"
			case "sourceUID":
				tag.Status.SourceActorUid += "foreign"
			case "templateUID":
				tag.Status.ActorTemplateUid += "foreign"
			case "scope":
				tag.Scope = pb.TagScope_TAG_SCOPE_PUBLISHED
			case "snapshotScope":
				tag.Status.Snapshot.ContentScope = pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
			}
			if change == "valid" {
				second := ready(t, c, native, next)
				if second.Identity.InstanceID == first.Identity.InstanceID || native.boots != 2 {
					t.Fatal("cold resume reused exact instance")
				}
				if _, err := driver(c, native).StopInstance(t.Context(), request.Key, first.Identity); err != sdk.ErrStaleIdentity {
					t.Fatal("old identity stopped successor")
				}
			} else {
				if _, err := driver(c, native).EnsureAllocation(t.Context(), next); err == nil {
					t.Fatal("changed native data provenance accepted")
				}
				if native.boots != 1 {
					t.Fatal("invalid restore booted an Actor")
				}
			}
		})
	}
}

func TestRetirementRequiresCoreExactAuthorization(t *testing.T) {
	c, native, request := fixture(t, false)
	first := ready(t, c, native, request)
	w := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(request.Key), w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	w.Status.Allocation = &first
	if err := c.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
	for _, auth := range []*api.WorkloadRetirement{nil, {Sequence: 1, Identity: sdk.InstanceIdentity{AllocationID: "foreign", InstanceID: first.Identity.InstanceID, RequestRevision: first.Identity.RequestRevision}, Action: api.WorkloadRetirementDelete}} {
		if err := c.Get(t.Context(), clientKey(request.Key), w); err != nil {
			t.Fatal(err)
		}
		w.Spec.Retirement = auth
		if err := c.Update(t.Context(), w); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileLifecycle(t.Context(), w); err != nil {
			t.Fatal(err)
		}
		if native.deletes != 0 {
			t.Fatal("retirement ran without exact core fence")
		}
	}
}

func TestReadyWorkspaceRetiresThroughSuspensionBeforeRollover(t *testing.T) {
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	w := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(request.Key), w); err != nil {
		t.Fatal(err)
	}
	w.Spec.Retirement = &api.WorkloadRetirement{Sequence: request.Sequence, Identity: first.Identity, Action: api.WorkloadRetirementSuspend}
	w.Status.Allocation = &first
	if err := c.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
	for range 12 {
		if err := c.Get(t.Context(), clientKey(request.Key), w); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileLifecycle(t.Context(), w); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), clientKey(request.Key), w); err != nil {
			t.Fatal(err)
		}
		if w.Status.Allocation != nil && w.Status.Allocation.State == sdk.AllocationStopped {
			break
		}
	}
	if w.Spec.DesiredState != api.ExecutionWorkspaceDesiredReady || w.Status.Allocation == nil || w.Status.Allocation.State != sdk.AllocationStopped || w.Status.Allocation.RetainedData == nil {
		t.Fatalf("rollover did not retain data while Ready: %#v", w.Status.Allocation)
	}
	next := *request.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &first.Identity
	next.RetainedData = w.Status.Allocation.RetainedData
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	second := ready(t, c, native, next)
	if second.Identity.InstanceID == first.Identity.InstanceID || native.suspends != 1 {
		t.Fatal("rollover reused process or lost data capture")
	}
}

func TestClosedBootstrapUsesOnlyPersistedExactLifetime(t *testing.T) {
	c, native, request := fixture(t, false)
	first := ready(t, c, native, request)
	d := driver(c, native)
	d.challengeClient = closedChallengeGetter{}
	observed, err := d.Observe(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Identity != first.Identity || observed.State != sdk.AllocationReady {
		t.Fatal("closed seeded bootstrap lost exact lifetime")
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	native.actors[record.Atespace+"/"+record.Actor.Name].Metadata.Version++
	if _, err := d.Observe(t.Context(), request.Key); err == nil {
		t.Fatal("closed bootstrap admitted changed native version")
	}
}

type closedChallengeGetter struct{}

func (closedChallengeGetter) Get(context.Context, string) (bootstrapChallenge, error) {
	return bootstrapChallenge{}, errChallengeClosed
}

func cappedRequest(t *testing.T, c client.Client, request sdk.WorkloadRequest, limit int32) sdk.WorkloadRequest {
	t.Helper()
	p := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: request.Key.Namespace, Name: request.ParametersRef.Name}, p); err != nil {
		t.Fatal(err)
	}
	p.Spec.Retention = &profile.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
	if err := c.Update(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
	if err != nil {
		t.Fatal(err)
	}
	raw["apiVersion"] = profile.GroupVersion.String()
	raw["kind"] = "SubstrateWorkspaceProfile"
	hash, err := sdk.ParametersProfileHash(&unstructured.Unstructured{Object: raw})
	if err != nil {
		t.Fatal(err)
	}
	request.ParametersBinding.ProfileHash = hash
	request.Revision, _ = sdk.WorkloadRevision(request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	return request
}
func TestSuspendedQuotaLossAndZeroCapacityFailBeforeMutation(t *testing.T) {
	for _, zero := range []bool{true, false} {
		t.Run(fmt.Sprintf("zero=%v", zero), func(t *testing.T) {
			c, native, request := fixture(t, true)
			limit := int32(1)
			if zero {
				limit = 0
			}
			request = cappedRequest(t, c, request, limit)
			first := ready(t, c, native, request)
			if zero {
				if _, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
					t.Fatal("zero retention cap allowed suspension")
				}
				if native.suspends != 0 {
					t.Fatal("quota rejection mutated native data")
				}
				return
			}
			stopped := retired(t, c, native, request, first.Identity, true)
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			cm := &corev1.ConfigMap{}
			if err := c.Get(t.Context(), retentionKey(record), cm); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), cm); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
				t.Fatal("missing occupied quota journal silently reset")
			}
			next := *request.DeepCopy()
			next.Sequence++
			next.PreviousInstance = &first.Identity
			next.RetainedData = stopped.RetainedData
			next.Revision, _ = sdk.WorkloadRevision(next)
			if err := admit(c)(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), next); err == nil {
				t.Fatal("resume ignored missing occupied quota journal")
			}
			if native.boots != 1 {
				t.Fatal("quota loss booted a replacement")
			}
		})
	}
}
func TestNativeWorkerPolicyTranslatesOnlyAdmittedEgress(t *testing.T) {
	c, native, request := fixture(t, false)
	request.Runtime.NetworkPolicy.Egress = []networkingv1.NetworkPolicyEgressRule{{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}}}}}
	request.Revision, _ = sdk.WorkloadRevision(request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	policy := &networkingv1.NetworkPolicy{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Placement.Namespace, Name: record.NetworkPolicy.Name}, policy); err != nil {
		t.Fatal(err)
	}
	if len(policy.Spec.Ingress) != 0 || len(policy.Spec.PolicyTypes) != 1 || policy.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress || len(policy.Spec.Egress) != 1 || policy.Spec.Egress[0].To[0].IPBlock.CIDR != "192.0.2.0/24" || policy.Spec.PodSelector.MatchLabels[workerAllocationLabel] != record.Observation.Identity.AllocationID || policy.Spec.PodSelector.MatchLabels[workerInstanceLabel] != record.Observation.Identity.InstanceID || policy.Spec.PodSelector.MatchLabels[workerPoolLabel] != "" {
		t.Fatalf("worker policy changed admitted egress or applied runtime ingress: %#v", policy.Spec)
	}
}

func TestSuspensionRejectsUnstableDurableKeyBeforeMaterialization(t *testing.T) {
	for _, value := range []string{"", "session-uid"} {
		t.Run(value, func(t *testing.T) {
			c, native, request := fixture(t, true)
			container := &request.Runtime.Template.Spec.Containers[0]
			for i := range container.Env {
				if container.Env[i].Name == "ORKA_ACP_DURABLE_WORKSPACE_KEY" {
					if value == "" {
						container.Env = append(container.Env[:i], container.Env[i+1:]...)
					} else {
						container.Env[i].Value = value
					}
					break
				}
			}
			request.Revision, _ = sdk.WorkloadRevision(request)
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("unstable durable workspace layout accepted")
			}
			if len(native.actors) != 0 || len(native.templates) != 1 || native.boots != 0 {
				t.Fatal("invalid durable layout materialized native resources")
			}
		})
	}
}
