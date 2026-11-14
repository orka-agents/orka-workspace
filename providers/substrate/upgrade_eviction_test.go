package substrate

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/proto"
	ctrl "sigs.k8s.io/controller-runtime"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	pinnedWorkerImageA = "registry.invalid/ateworker-gvisor@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pinnedWorkerImageB = "registry.invalid/ateworker-gvisor@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func sourcePool(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	pool := poolObject()
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "native-workers", Name: "native-workers"}, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func mutateSourcePool(t *testing.T, c client.Client, mutate func(*unstructured.Unstructured) error) {
	t.Helper()
	pool := sourcePool(t, c)
	if err := mutate(pool); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
}

func setSourceWorkerImage(image string) func(*unstructured.Unstructured) error {
	return func(pool *unstructured.Unstructured) error {
		return unstructured.SetNestedField(pool.Object, image, "spec", "workerImage")
	}
}

// suspendedSuccessor suspends the first instance and admits its data-only
// continuation without starting it.
func suspendedSuccessor(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest) sdk.WorkloadRequest {
	t.Helper()
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
	return next
}

func privatePoolSpec(t *testing.T, c client.Client) map[string]any {
	t.Helper()
	pools := privateWorkerPools(t, c)
	if len(pools) != 1 {
		t.Fatalf("private worker pools = %d, want 1", len(pools))
	}
	spec, _, err := unstructured.NestedMap(pools[0].Object, "spec")
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestPrivatePoolPinsTheOperatorPoolResolvedSandboxConfig(t *testing.T) {
	c, native, request := fixture(t, true)
	ready(t, c, native, request)
	classes, _, err := unstructured.NestedSlice(privatePoolSpec(t, c), "sandboxClasses")
	if err != nil || len(classes) != 1 {
		t.Fatalf("private sandbox classes = %v, %v", classes, err)
	}
	if name, _, _ := unstructured.NestedString(classes[0].(map[string]any), "configRef", "name"); name != fixtureSandboxConfig {
		t.Fatalf("private pool SandboxConfig = %q, want the operator pool's resolved %q", name, fixtureSandboxConfig)
	}
}

func TestOperatorSandboxConfigMustBeResolvedAndStable(t *testing.T) {
	const generation = 3
	status := func(entry map[string]any) map[string]any {
		return map[string]any{"sandboxClasses": []any{entry}}
	}
	resolution := func(config, state string, observed int64) map[string]any {
		entry := resolvedSandboxClassStatus(config)
		entry["conditions"] = []any{map[string]any{"type": "Resolved", "status": state, "reason": "Resolved", "observedGeneration": observed}}
		return entry
	}
	resolved := func(config string) map[string]any { return resolution(config, "True", generation) }
	for _, test := range []struct {
		name    string
		class   map[string]any
		status  map[string]any
		want    string
		wantErr string
	}{
		{name: "class default resolved", class: map[string]any{"name": "gvisor"}, status: status(resolved("gvisor-1")), want: "gvisor-1"},
		{name: "declared and resolved", class: map[string]any{"name": "gvisor", "configRef": map[string]any{"name": "gvisor-1"}}, status: status(resolved("gvisor-1")), want: "gvisor-1"},
		{name: "declared before first resolution", class: map[string]any{"name": "gvisor", "configRef": map[string]any{"name": "gvisor-1"}}, wantErr: "has not resolved"},
		{name: "unresolved class default", class: map[string]any{"name": "gvisor"}, wantErr: "has not resolved"},
		// Agent Substrate keeps the last resolved configRef while the class
		// does not resolve, e.g. a removed default or a deleting config.
		{name: "kept after resolution failed", class: map[string]any{"name": "gvisor"}, status: status(resolution("gvisor-1", "False", generation)), wantErr: "has not resolved"},
		{name: "declared kept after resolution failed", class: map[string]any{"name": "gvisor", "configRef": map[string]any{"name": "gvisor-1"}}, status: status(resolution("gvisor-1", "False", generation)), wantErr: "has not resolved"},
		{name: "resolution of an older generation", class: map[string]any{"name": "gvisor"}, status: status(resolution("gvisor-1", "True", generation-1)), wantErr: "has not resolved"},
		{name: "no resolution condition", class: map[string]any{"name": "gvisor"}, status: status(map[string]any{"name": "gvisor", "configRef": map[string]any{"name": "gvisor-1"}}), wantErr: "has not resolved"},
		{name: "switching", class: map[string]any{"name": "gvisor"}, status: status(map[string]any{"name": "gvisor", "configRef": map[string]any{"name": "gvisor-1"}, "pendingConfigRef": map[string]any{"name": "gvisor-2"}}), wantErr: "switching"},
		{name: "declared differs from resolved", class: map[string]any{"name": "gvisor", "configRef": map[string]any{"name": "gvisor-2"}}, status: status(resolved("gvisor-1")), wantErr: "switching"},
		{name: "malformed reference", class: map[string]any{"name": "gvisor", "configRef": map[string]any{"name": "gvisor-1", "namespace": "x"}}, wantErr: "exactly one SandboxConfig"},
		{name: "empty reference", class: map[string]any{"name": "gvisor", "configRef": map[string]any{"name": ""}}, wantErr: "exactly one SandboxConfig"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := poolObject()
			source.SetGeneration(generation)
			if test.status != nil {
				source.Object["status"] = test.status
			}
			got, err := pinnedSandboxConfig(source, test.class)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("pinnedSandboxConfig = %q, %v; want error containing %q", got, err, test.wantErr)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("pinnedSandboxConfig = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

// After an Agent Substrate minor upgrade rolls the operator pool to a new
// worker image, a data-only resume cold boots on that current image instead
// of the old one (which a newer API server must not run with) and records the
// transition on the lineage.
func TestDataOnlyResumeFollowsDigestPinnedOperatorWorkerImage(t *testing.T) {
	c, native, request := fixture(t, true)
	mutateSourcePool(t, c, setSourceWorkerImage(pinnedWorkerImageA))
	next := suspendedSuccessor(t, c, native, request)
	_, before, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	previousSpec := append(json.RawMessage(nil), before.RuntimePoolSpec...)
	mutateSourcePool(t, c, setSourceWorkerImage(pinnedWorkerImageB))
	ready(t, c, native, next)
	if image, _, _ := unstructured.NestedString(privatePoolSpec(t, c), "workerImage"); image != pinnedWorkerImageB {
		t.Fatalf("resumed private worker image = %q, want the operator pool's current %q", image, pinnedWorkerImageB)
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.WorkerImageProvenance) != 1 {
		t.Fatalf("worker image provenance = %#v, want one transition", record.WorkerImageProvenance)
	}
	transition := record.WorkerImageProvenance[0]
	if transition.From != pinnedWorkerImageA || transition.To != pinnedWorkerImageB || transition.PreviousRuntimePoolDigest != digest(previousSpec) || transition.ObservedAt.IsZero() {
		t.Fatalf("worker image transition = %#v", transition)
	}
	if record.InheritedCheckpoint == nil || native.boots != 2 {
		t.Fatal("resume did not restore the retained data exactly once")
	}
}

func TestDataOnlyResumeOntoNewWorkerImageFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*unstructured.Unstructured) error
	}{
		{name: "tag image", mutate: setSourceWorkerImage("registry.invalid/ateworker-gvisor:v1.1.0")},
		{name: "unqualified digest", mutate: setSourceWorkerImage("registry.invalid/ateworker-gvisor@sha256:short")},
		{name: "image and resources", mutate: func(pool *unstructured.Unstructured) error {
			if err := setSourceWorkerImage(pinnedWorkerImageB)(pool); err != nil {
				return err
			}
			return unstructured.SetNestedField(pool.Object, "different-node", "spec", "template", "nodeSelector", "node")
		}},
		{name: "image and sandbox config", mutate: func(pool *unstructured.Unstructured) error {
			if err := setSourceWorkerImage(pinnedWorkerImageB)(pool); err != nil {
				return err
			}
			return unstructured.SetNestedSlice(pool.Object, []any{resolvedSandboxClassStatus("gvisor-next")}, "status", "sandboxClasses")
		}},
		{name: "image and operator labels", mutate: func(pool *unstructured.Unstructured) error {
			if err := setSourceWorkerImage(pinnedWorkerImageB)(pool); err != nil {
				return err
			}
			labels := pool.GetLabels()
			labels["operator.example/tier"] = "changed"
			pool.SetLabels(labels)
			return nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, true)
			mutateSourcePool(t, c, setSourceWorkerImage(pinnedWorkerImageA))
			next := suspendedSuccessor(t, c, native, request)
			mutateSourcePool(t, c, test.mutate)
			if _, err := driver(c, native).EnsureAllocation(t.Context(), next); err == nil {
				t.Fatal("data-only resume accepted a change beyond the operator pool's digest-pinned image")
			}
			if native.boots != 1 || len(native.actors) != 0 {
				t.Fatal("rejected worker infrastructure reached native execution")
			}
		})
	}
}

func TestDataResumeRuntimePoolLeavesUnchangedSpecWithoutTransition(t *testing.T) {
	spec := json.RawMessage(`{"replicas":1,"workerImage":"registry.invalid/w:tag","sandboxClasses":[{"name":"gvisor","configRef":{"name":"c"}}],"template":{"labels":{"` + workerAllocationLabel + `":"a","` + workerInstanceLabel + `":"i"},"resources":{"limits":{"cpu":"1","memory":"1Gi"}}}}`)
	transition, err := dataResumeRuntimePool(spec, spec, metav1.Now())
	if err != nil || transition != nil {
		t.Fatalf("unchanged spec produced %#v, %v", transition, err)
	}
	if _, err := inheritWorkerImageProvenance(make([]workerImageTransition, maxWorkerImageTransitions), &workerImageTransition{}); err == nil {
		t.Fatal("worker image provenance exceeded its bound")
	}
}

func TestDataCheckpointRecordsSandboxRuntime(t *testing.T) {
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	retired(t, c, native, request, first.Identity, true)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	want, err := stampSandboxRuntime(gvisorSandboxClass, native.runtimeVersion())
	if err != nil || want == nil {
		t.Fatalf("fixture runtime stamp = %v, %v", want, err)
	}
	if record.Checkpoint == nil || record.Checkpoint.SandboxRuntime == nil || *record.Checkpoint.SandboxRuntime != *want {
		t.Fatalf("checkpoint sandbox runtime = %#v, want %#v", record.Checkpoint, want)
	}
	if err := driver(c, native).verifyCheckpoint(t.Context(), record.Checkpoint); err != nil {
		t.Fatalf("recorded runtime does not verify against its Tag: %v", err)
	}
	altered := *record.Checkpoint
	altered.SandboxRuntime = &sandboxRuntimeStamp{Class: gvisorSandboxClass, SchemaVersion: "v1", Digest: "sha256:other"}
	if err := driver(c, native).verifyCheckpoint(t.Context(), &altered); !errors.Is(err, sdk.ErrStaleIdentity) {
		t.Fatalf("altered runtime provenance verified: %v", err)
	}
	legacy := *record.Checkpoint
	legacy.SandboxRuntime = nil
	if err := driver(c, native).verifyCheckpoint(t.Context(), &legacy); err != nil {
		t.Fatalf("a checkpoint recorded before runtime stamping no longer verifies: %v", err)
	}
}

func TestCheckpointSandboxRuntimeRejectsAnotherSandboxClass(t *testing.T) {
	if stamp, err := checkpointSandboxRuntime(&pb.Snapshot{}); err != nil || stamp != nil {
		t.Fatalf("unstamped snapshot = %v, %v", stamp, err)
	}
	if _, err := checkpointSandboxRuntime(&pb.Snapshot{SandboxRuntime: &pb.ActorSandboxRuntime{SandboxClass: "microvm", Version: &pb.VersionedSandboxCompat{SchemaVersion: "v1"}}}); err == nil {
		t.Fatal("a microVM snapshot was accepted as a gVisor data checkpoint")
	}
}

// A worker process that restarted after the crash keeps its UID but registers
// a new epoch and may run another runtime, so its current default runtime
// says nothing about the one that performed the restore.
func TestRestoreCrashOnARestartedWorkerIsNotClassifiedByItsNewRuntime(t *testing.T) {
	c, native, request := fixture(t, true)
	next := suspendedSuccessor(t, c, native, request)
	native.crashOnResume = true
	native.beforeResume = func(context.Context, *pb.Actor) error {
		native.beforeResume = nil
		// The restore runs under the original runtime; afterwards the worker
		// restarts with an upgraded one under a new registration.
		native.afterCrash = func(worker *pb.Worker) {
			worker.Status.RegisteredEpoch++
			worker.Status.SandboxClasses[0].DefaultRuntime.Version = &pb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: []*pb.AttributeEntry{{Key: "sandbox_bundle_hash", Value: "runsc-release-2"}}}
		}
		return nil
	}
	var workspace *api.ExecutionWorkspace
	for range 12 {
		workspace = reconcileFailureFixture(t, c, native, next)
		if workspace.Status.State == api.ExecutionWorkspaceStateFailed {
			break
		}
	}
	provisioned := sdk.FindCondition(workspace.Status.Conditions, string(api.ConditionWorkspaceProvisioned))
	if workspace.Status.State != api.ExecutionWorkspaceStateFailed || provisioned == nil || provisioned.Reason != instanceFailedReason {
		t.Fatalf("restore crash on a restarted worker = %s %+v; want %s without runtime classification", workspace.Status.State, provisioned, instanceFailedReason)
	}
}

func restoreCrashCondition(t *testing.T, c client.Client, native *nativeFixture, next sdk.WorkloadRequest) *metav1.Condition {
	t.Helper()
	// A synchronous crash fails the Resume RPC once; the next pass observes
	// the CRASHED Actor.
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	workspace := &api.ExecutionWorkspace{}
	for range 12 {
		_, _ = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(next.Key)})
		if err := c.Get(t.Context(), clientKey(next.Key), workspace); err != nil {
			t.Fatal(err)
		}
		if workspace.Status.State == api.ExecutionWorkspaceStateFailed {
			break
		}
	}
	provisioned := sdk.FindCondition(workspace.Status.Conditions, string(api.ConditionWorkspaceProvisioned))
	if workspace.Status.State != api.ExecutionWorkspaceStateFailed || provisioned == nil {
		t.Fatalf("crashed restore reported %s %+v", workspace.Status.State, provisioned)
	}
	return provisioned
}

// SetWorkerSandboxRuntimes may replace a Worker's default runtime within one
// registration epoch, so a later default says nothing about the runtime that
// performed the restore.
func TestRestoreCrashIgnoresASameEpochDefaultRuntimeChange(t *testing.T) {
	c, native, request := fixture(t, true)
	next := suspendedSuccessor(t, c, native, request)
	native.crashOnResume = true
	native.afterCrash = func(worker *pb.Worker) {
		worker.Status.SandboxClasses[0].DefaultRuntime.Version = &pb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: []*pb.AttributeEntry{{Key: "sandbox_bundle_hash", Value: "runsc-release-2"}}}
	}
	if provisioned := restoreCrashCondition(t, c, native, next); provisioned.Reason != instanceFailedReason {
		t.Fatalf("same-runtime restore crash after a same-epoch default change = %+v; want %s", provisioned, instanceFailedReason)
	}
}

// A synchronous restore crash clears the assignment before ResumeActor
// returns, so the fenced Worker's default runtime read before Resume is the
// evidence, accepted only while that Worker still reports the same default.
func TestSynchronousRestoreCrashIsClassifiedFromTheFencedWorker(t *testing.T) {
	c, native, request := fixture(t, true)
	next := suspendedSuccessor(t, c, native, request)
	native.runtime = &pb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: []*pb.AttributeEntry{{Key: "sandbox_bundle_hash", Value: "runsc-release-2"}}}
	native.crashOnResume = true
	native.crashBeforeAssignment = true
	if provisioned := restoreCrashCondition(t, c, native, next); provisioned.Reason != sandboxRuntimeRestoreFailedReason {
		t.Fatalf("synchronous cross-runtime restore crash = %+v; want %s", provisioned, sandboxRuntimeRestoreFailedReason)
	}
	if native.boots != 2 {
		t.Fatalf("crashed restore was replayed: %d boots", native.boots)
	}
}

// If the fenced Worker's default changes between Resume and classification,
// the pre-Resume reading no longer proves the restore's runtime.
func TestSynchronousRestoreCrashAfterADefaultChangeIsNotClassified(t *testing.T) {
	c, native, request := fixture(t, true)
	next := suspendedSuccessor(t, c, native, request)
	original := proto.Clone(native.runtimeVersion()).(*pb.VersionedSandboxCompat)
	native.runtime = &pb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: []*pb.AttributeEntry{{Key: "sandbox_bundle_hash", Value: "runsc-release-2"}}}
	native.crashOnResume = true
	native.crashBeforeAssignment = true
	native.afterCrash = func(worker *pb.Worker) {
		worker.Status.SandboxClasses[0].DefaultRuntime.Version = proto.Clone(original).(*pb.VersionedSandboxCompat)
	}
	if provisioned := restoreCrashCondition(t, c, native, next); provisioned.Reason != instanceFailedReason {
		t.Fatalf("restore crash after a default change = %+v; want %s", provisioned, instanceFailedReason)
	}
}

// ResumeActor places by selector, so once the private pool holds another
// Worker the fenced Worker's reading no longer proves which one restored.
func TestSynchronousRestoreCrashWithAReplacementWorkerIsNotClassified(t *testing.T) {
	c, native, request := fixture(t, true)
	next := suspendedSuccessor(t, c, native, request)
	native.runtime = &pb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: []*pb.AttributeEntry{{Key: "sandbox_bundle_hash", Value: "runsc-release-2"}}}
	native.crashOnResume = true
	native.crashBeforeAssignment = true
	native.afterCrash = func(worker *pb.Worker) {
		replacement := proto.Clone(worker).(*pb.Worker)
		replacement.Metadata.Name += "-replacement"
		replacement.Metadata.Uid += "-replacement"
		native.workers[replacement.Metadata.Name] = replacement
	}
	if provisioned := restoreCrashCondition(t, c, native, next); provisioned.Reason != instanceFailedReason {
		t.Fatalf("restore crash with a replacement Worker in the pool = %+v; want %s", provisioned, instanceFailedReason)
	}
}

func TestRestoreCrashUnderAnotherSandboxRuntimeIsClassified(t *testing.T) {
	for _, upgraded := range []bool{true, false} {
		name := "same runtime"
		if upgraded {
			name = "upgraded runtime"
		}
		t.Run(name, func(t *testing.T) {
			c, native, request := fixture(t, true)
			next := suspendedSuccessor(t, c, native, request)
			if upgraded {
				native.runtime = &pb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: []*pb.AttributeEntry{{Key: "sandbox_bundle_hash", Value: "runsc-release-2"}}}
			}
			native.crashOnResume = true
			var workspace *api.ExecutionWorkspace
			for range 12 {
				workspace = reconcileFailureFixture(t, c, native, next)
				if workspace.Status.State == api.ExecutionWorkspaceStateFailed {
					break
				}
			}
			if workspace.Status.State != api.ExecutionWorkspaceStateFailed {
				t.Fatalf("crashed restore reported %s", workspace.Status.State)
			}
			provisioned := sdk.FindCondition(workspace.Status.Conditions, string(api.ConditionWorkspaceProvisioned))
			if provisioned == nil {
				t.Fatal("crashed restore has no Provisioned condition")
			}
			if upgraded {
				if provisioned.Reason != sandboxRuntimeRestoreFailedReason || !strings.Contains(provisioned.Message, "different sandbox runtime") || !strings.Contains(provisioned.Message, "never replayed") {
					t.Fatalf("cross-runtime restore crash condition = %+v", provisioned)
				}
			} else if provisioned.Reason != instanceFailedReason {
				t.Fatalf("same-runtime restore crash condition = %+v", provisioned)
			}
			if native.boots != 2 {
				t.Fatalf("crashed restore was replayed: %d boots", native.boots)
			}
		})
	}
}

// A worker that begins graceful termination keeps its exact running Actor
// observable, so Core can drain it and authorize a synchronous data-only
// suspension inside the eviction window.
func TestTerminatingFencedWorkerRemainsObservableAndSuspendable(t *testing.T) {
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil || record.Worker == nil {
		t.Fatalf("ready instance has no worker fence: %v", err)
	}
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: record.Worker.Namespace, Name: record.Worker.Pod}
	if err := c.Get(t.Context(), key, pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = append(pod.Finalizers, "test.orka.ai/worker-shutdown")
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	observed, err := driver(c, native).EnsureAllocation(t.Context(), request)
	if err != nil || observed.State != sdk.AllocationReady || observed.Startup == nil || observed.Identity != first.Identity || observed.Startup.Process.Worker.UID != pod.UID {
		t.Fatalf("terminating fenced worker hid the running instance: %+v, %v", observed, err)
	}
	// A worker with no fence is never adopted while terminating.
	actor := native.actors[record.Atespace+"/"+record.Actor.Name]
	unfenced := *record
	unfenced.Worker = nil
	if _, err := driver(c, native).worker(t.Context(), &unfenced, actor); !errors.Is(err, sdk.ErrStaleIdentity) {
		t.Fatalf("unfenced terminating worker was adopted: %v", err)
	}
	var stopped sdk.AllocationObservation
	for i := range 12 {
		stopped, err = driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == sdk.AllocationStopped {
			break
		}
		if i == 2 {
			// The worker exits once its only Actor is suspended.
			if err := c.Get(t.Context(), key, pod); err != nil {
				t.Fatal(err)
			}
			pod.Finalizers = nil
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
		}
	}
	if stopped.State != sdk.AllocationStopped || stopped.RetainedData == nil || native.suspends != 1 {
		t.Fatalf("eviction suspension = %+v (suspends %d)", stopped, native.suspends)
	}
}

// A public checkpoint restore is the same data-only cold boot: it follows the
// operator pool's current digest-pinned image and nothing else.
func TestCheckpointRestoreFollowsDigestPinnedOperatorWorkerImage(t *testing.T) {
	for _, pinned := range []bool{true, false} {
		name := "tag image"
		if pinned {
			name = "pinned image"
		}
		t.Run(name, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			request := restoreRequest(t, c, source, cp)
			image := "registry.invalid/ateworker-gvisor:v1.1.0"
			if pinned {
				image = pinnedWorkerImageB
			}
			mutateSourcePool(t, c, setSourceWorkerImage(image))
			if !pinned {
				if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
					t.Fatal("checkpoint restore accepted an image that is not pinned by digest")
				}
				if native.boots != 1 {
					t.Fatal("rejected restore reached native execution")
				}
				return
			}
			ready(t, c, native, request)
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if len(record.WorkerImageProvenance) != 1 || record.WorkerImageProvenance[0].From != "fixture.invalid/native-worker:pin" || record.WorkerImageProvenance[0].To != pinnedWorkerImageB {
				t.Fatalf("restore worker image provenance = %#v", record.WorkerImageProvenance)
			}
		})
	}
}

func setSourceOperatorLabel(pool *unstructured.Unstructured) error {
	labels := pool.GetLabels()
	labels["operator.example/tier"] = "changed"
	pool.SetLabels(labels)
	return nil
}

// The operator pool's own labels are immutable for a data-only resume even
// when its worker image is unchanged.
func TestDataOnlyResumeRejectsChangedOperatorPoolLabels(t *testing.T) {
	c, native, request := fixture(t, true)
	next := suspendedSuccessor(t, c, native, request)
	mutateSourcePool(t, c, setSourceOperatorLabel)
	if _, err := driver(c, native).EnsureAllocation(t.Context(), next); err == nil || !strings.Contains(err.Error(), "operator worker pool labels") {
		t.Fatalf("data-only resume accepted changed operator pool labels: %v", err)
	}
	if native.boots != 1 || len(native.actors) != 0 {
		t.Fatal("rejected operator pool labels reached native execution")
	}
}

// A checkpoint carries the operator pool labels it was taken under; a restore
// under changed labels fails before acquiring or restoring the checkpoint,
// with or without a worker image transition.
func TestCheckpointRestoreRejectsChangedOperatorPoolLabels(t *testing.T) {
	for _, image := range []string{"", pinnedWorkerImageB} {
		name := "same image"
		if image != "" {
			name = "pinned image transition"
		}
		t.Run(name, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			request := restoreRequest(t, c, source, cp)
			_, sourceRecord, err := driver(c, native).read(t.Context(), source.Key)
			if err != nil {
				t.Fatal(err)
			}
			catalog := catalogKey(source.Key.Namespace, sourceRecord.Checkpoint.Tag.UID)
			_, artifact, err := driver(c, native).readCatalog(t.Context(), catalog)
			if err != nil {
				t.Fatal(err)
			}
			if artifact.RuntimePoolOperatorLabels["selected"] != "native" {
				t.Fatalf("checkpoint did not record the operator pool labels: %v", artifact.RuntimePoolOperatorLabels)
			}
			if image != "" {
				mutateSourcePool(t, c, setSourceWorkerImage(image))
			}
			mutateSourcePool(t, c, setSourceOperatorLabel)
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "operator worker pool labels") {
				t.Fatalf("checkpoint restore accepted changed operator pool labels: %v", err)
			}
			if _, artifact, err = driver(c, native).readCatalog(t.Context(), catalog); err != nil {
				t.Fatal(err)
			}
			if _, acquired := artifact.Owners[workspaceArtifactOwner(request.Key)]; acquired || native.boots != 1 {
				t.Fatal("rejected restore acquired the checkpoint or reached native execution")
			}
		})
	}
}

// A suspension whose catalog write committed before runtime stamping existed,
// but whose journal write was lost, retries after the upgrade with a stamped
// record. It recovers the published unstamped checkpoint instead of
// conflicting forever.
func TestRegisterCheckpointRecoversUnstampedCatalog(t *testing.T) {
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	retired(t, c, native, request, first.Identity, true)
	d := driver(c, native)
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Checkpoint == nil || record.Checkpoint.SandboxRuntime == nil {
		t.Fatal("fixture checkpoint is not stamped")
	}
	stamped := *record.Checkpoint
	// Rewrite the catalog as a pre-stamping provider published it.
	catalogCM, artifact, err := d.readCatalog(t.Context(), catalogKey(request.Key.Namespace, stamped.Tag.UID))
	if err != nil {
		t.Fatal(err)
	}
	artifact.Checkpoint.SandboxRuntime = nil
	if artifact.Digest, err = artifactDigest(artifact.Checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := d.saveCatalog(t.Context(), catalogCM, artifact); err != nil {
		t.Fatal(err)
	}
	retry := stamped
	reference, err := d.registerCheckpoint(t.Context(), record, &retry)
	if err != nil {
		t.Fatalf("retry conflicted with the unstamped catalog: %v", err)
	}
	if reference.Digest != artifact.Digest || retry.SandboxRuntime != nil {
		t.Fatalf("recovery changed the published checkpoint: %v, %v", reference, retry.SandboxRuntime)
	}
	// A stamp that differs from a stamped catalog still conflicts.
	catalogCM, artifact, err = d.readCatalog(t.Context(), catalogKey(request.Key.Namespace, stamped.Tag.UID))
	if err != nil {
		t.Fatal(err)
	}
	artifact.Checkpoint.SandboxRuntime = &sandboxRuntimeStamp{Class: gvisorSandboxClass, SchemaVersion: "v1", Digest: "sha256:other"}
	if artifact.Digest, err = artifactDigest(artifact.Checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := d.saveCatalog(t.Context(), catalogCM, artifact); err != nil {
		t.Fatal(err)
	}
	retry = stamped
	if _, err := d.registerCheckpoint(t.Context(), record, &retry); !errors.Is(err, sdk.ErrRequestConflict) {
		t.Fatalf("a differing runtime stamp did not conflict: %v", err)
	}
}

// A catalog published before operator labels were recorded keeps restoring
// under its original rule, an identical pool specification, but cannot
// prove its labels across a worker image transition.
func TestCheckpointRestoreOfCatalogWithoutRecordedLabels(t *testing.T) {
	for _, transition := range []bool{false, true} {
		name := "same image"
		if transition {
			name = "pinned image transition"
		}
		t.Run(name, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			request := restoreRequest(t, c, source, cp)
			d := driver(c, native)
			_, sourceRecord, err := d.read(t.Context(), source.Key)
			if err != nil {
				t.Fatal(err)
			}
			catalogCM, artifact, err := d.readCatalog(t.Context(), catalogKey(source.Key.Namespace, sourceRecord.Checkpoint.Tag.UID))
			if err != nil {
				t.Fatal(err)
			}
			artifact.RuntimePoolOperatorLabels = nil
			if err := d.saveCatalog(t.Context(), catalogCM, artifact); err != nil {
				t.Fatal(err)
			}
			if !transition {
				ready(t, c, native, request)
				return
			}
			mutateSourcePool(t, c, setSourceWorkerImage(pinnedWorkerImageB))
			if _, err := d.EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "recorded operator worker pool labels") {
				t.Fatalf("restore without recorded labels accepted an image transition: %v", err)
			}
			if native.boots != 1 {
				t.Fatal("rejected restore reached native execution")
			}
		})
	}
	// An unlabeled operator pool records an explicit empty set, never nil.
	data, err := json.Marshal(checkpointArtifact{RuntimePoolOperatorLabels: map[string]string{}})
	if err != nil || !strings.Contains(string(data), `"runtimePoolOperatorLabels":{}`) {
		t.Fatalf("empty operator labels were not recorded explicitly: %s, %v", data, err)
	}
}
