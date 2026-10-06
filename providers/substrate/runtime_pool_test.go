package substrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestPrivateWorkerIsConfinedBeforeNativeResumeIncludingLostResponse(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			c, native, request := fixture(t, true)
			checked := 0
			native.beforeResume = func(ctx context.Context, _ *pb.Actor) error {
				_, record, err := driver(c, native).read(ctx, request.Key)
				if err != nil {
					return err
				}
				if record.Worker == nil || record.RuntimePool.UID == "" || record.NetworkPolicy.UID == "" || !record.BootRequested {
					return fmt.Errorf("native Resume preceded durable worker/pool/policy ownership")
				}
				pool := poolObject()
				if err := c.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool); err != nil {
					return err
				}
				if replicas, found := pool.Object["spec"].(map[string]any)["replicas"]; !found || fmt.Sprint(replicas) != "1" {
					return fmt.Errorf("private native pool is not single-replica")
				}
				if err := driver(c, native).verifyInfrastructureReadOnly(ctx, record); err != nil {
					return err
				}
				if err := driver(c, native).bindWorker(ctx, record); err != nil {
					return err
				}
				pod := &corev1.Pod{}
				if err := c.Get(ctx, client.ObjectKey{Namespace: record.Worker.Namespace, Name: record.Worker.Pod}, pod); err != nil {
					return err
				}
				policy := &networkingv1.NetworkPolicy{}
				if err := c.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: record.NetworkPolicy.Name}, policy); err != nil {
					return err
				}
				for key, value := range policy.Spec.PodSelector.MatchLabels {
					if pod.Labels[key] != value {
						return fmt.Errorf("worker lacked confinement labels at birth")
					}
				}
				checked++
				return nil
			}
			if lost {
				native.failAfter = "resume"
				if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
					t.Fatal("expected lost Resume response")
				}
			}
			ready(t, c, native, request)
			if checked != 1 || native.boots != 1 {
				t.Fatal("boot was replayed or confinement was not checked before native execution")
			}
		})
	}
}

func TestColdResumeRejectsChangedSourceWorkerSpecification(t *testing.T) {
	for _, field := range []string{"workerImage", "resources", "nodeSelector", "operatorLabels"} {
		t.Run(field, func(t *testing.T) {
			c, native, request := fixture(t, true)
			first := ready(t, c, native, request)
			stopped := retired(t, c, native, request, first.Identity, true)
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			base := poolObject()
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.Placement.Name}, base); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "workerImage":
				err = unstructured.SetNestedField(base.Object, "changed-worker-image", "spec", "workerImage")
			case "resources":
				err = unstructured.SetNestedField(base.Object, "2Gi", "spec", "template", "resources", "limits", "memory")
			case "nodeSelector":
				err = unstructured.SetNestedField(base.Object, "different-node", "spec", "template", "nodeSelector", "node")
			case "operatorLabels":
				err = unstructured.SetNestedField(base.Object, "different-management-selector", "spec", "template", "labels", "operator")
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Update(t.Context(), base); err != nil {
				t.Fatal(err)
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
				t.Fatal("cold resume accepted changed operator worker infrastructure")
			}
			if native.boots != 1 || len(native.actors) != 0 {
				t.Fatal("changed worker infrastructure reached native execution")
			}
		})
	}
}

func TestRuntimePoolRestoreAllowsOnlyGeneratedLabelRotation(t *testing.T) {
	c, native, request := fixture(t, true)
	ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	var spec map[string]any
	if err := json.Unmarshal(record.RuntimePoolSpec, &spec); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{workerAllocationLabel, workerInstanceLabel} {
		if err := unstructured.SetNestedField(spec, "rotated-private-id", "template", "labels", key); err != nil {
			t.Fatal(err)
		}
	}
	rotated, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !compatibleRuntimePool(record.RuntimePoolSpec, rotated) || compatibleRuntimePool(nil, rotated) || compatibleRuntimePool(record.RuntimePoolSpec, json.RawMessage(`{}`)) {
		t.Fatal("worker infrastructure normalization widened immutable settings or accepted missing provenance")
	}
}

func TestWorkerReplacementBetweenFenceAndResumeRejectsStartupButCanRetire(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprint(foreign), func(t *testing.T) {
			c, native, request := fixture(t, false)
			var original workerFence
			var replacementKey client.ObjectKey
			native.beforeResume = func(ctx context.Context, _ *pb.Actor) error {
				_, record, err := driver(c, native).read(ctx, request.Key)
				if err != nil {
					return err
				}
				original = *record.Worker
				old := native.workers[original.Name]
				pod := &corev1.Pod{}
				if err := c.Get(ctx, client.ObjectKey{Namespace: original.Namespace, Name: original.Pod}, pod); err != nil {
					return err
				}
				if err := c.Delete(ctx, pod); err != nil {
					return err
				}
				delete(native.workers, old.Metadata.Name)
				replacement := pod.DeepCopy()
				replacement.Name += "-replacement"
				replacement.UID = types.UID("replacement-pod-uid")
				replacement.ResourceVersion = ""
				if err := c.Create(ctx, replacement); err != nil {
					return err
				}
				replacementKey = client.ObjectKeyFromObject(replacement)
				worker := proto.Clone(old).(*pb.Worker)
				worker.Metadata.Name = replacement.Name
				worker.Metadata.Uid = "replacement-worker-uid"
				worker.WorkerPod = replacement.Name
				worker.WorkerPodUid = string(replacement.UID)
				native.workers[worker.Metadata.Name] = worker
				return nil
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatalf("replacement was accepted for startup: %v", err)
			}
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if *record.Worker != original || native.boots != 1 {
				t.Fatal("startup fence was changed or boot replayed")
			}
			if foreign {
				pod := &corev1.Pod{}
				if err := c.Get(t.Context(), replacementKey, pod); err != nil {
					t.Fatal(err)
				}
				pod.Labels[workerInstanceLabel] = "foreign-instance"
				if err := c.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				if _, err := driver(c, native).StopInstance(t.Context(), request.Key, record.Observation.Identity); !errors.Is(err, sdk.ErrStaleIdentity) {
					t.Fatal("foreign replacement ownership was allowed")
				}
				pool := poolObject()
				if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool); err != nil || pool.GetDeletionTimestamp() != nil || len(native.actors) != 1 {
					t.Fatal("foreign replacement was mutated")
				}
				return
			}
			retired(t, c, native, request, record.Observation.Identity, false)
			_, final, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if *final.Worker != original || len(final.RetirementWorkers) != 1 || len(native.actors) != 0 || native.boots != 1 {
				t.Fatal("owned replacement did not retire independently of original startup fence")
			}
		})
	}
}

func TestLostPrivatePoolCreateRecoversBeforeNativeEffects(t *testing.T) {
	c, native, request := fixture(t, false)
	lost := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if !lost && obj.GetLabels()[workerInstanceLabel] != "" && obj.GetObjectKind().GroupVersionKind().Kind == "WorkerPool" {
			if err := inner.Create(ctx, obj, opts...); err != nil {
				return err
			}
			lost = true
			return fmt.Errorf("lost private WorkerPool creation response")
		}
		return inner.Create(ctx, obj, opts...)
	}})
	native.client = wrapped
	if _, err := driver(wrapped, native).EnsureAllocation(t.Context(), request); err == nil || !lost {
		t.Fatal("private pool response not lost")
	}
	if native.boots != 0 || len(native.actors) != 0 {
		t.Fatal("native effects preceded recoverable worker confinement")
	}
	ready(t, wrapped, native, request)
	if native.boots != 1 {
		t.Fatal("private pool recovery duplicated native execution")
	}
}

func TestPrivatePoolFinalizerBlocksTerminalObservation(t *testing.T) {
	c, native, request := fixture(t, false)
	first := ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	pool := poolObject()
	key := client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}
	if err := c.Get(t.Context(), key, pool); err != nil {
		t.Fatal(err)
	}
	pool.SetFinalizers([]string{"fixture.test/hold"})
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		out, err := driver(c, native).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if out.State == sdk.AllocationStopped {
			t.Fatal("pool finalizer was treated as completed retirement")
		}
	}
	if err := c.Get(t.Context(), key, pool); err != nil {
		t.Fatal(err)
	}
	if pool.GetDeletionTimestamp() == nil {
		t.Fatal("private pool deletion was not issued")
	}
	pool.SetFinalizers(nil)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	retired(t, c, native, request, first.Identity, false)
	base := poolObject()
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.Placement.Name}, base); err != nil {
		t.Fatal(err)
	}
	if base.GetUID() != record.Placement.UID {
		t.Fatal("retirement changed operator pool ownership")
	}
}

func TestMissingBirthLabelsRejectsBootWithoutPodPatching(t *testing.T) {
	c, native, request := fixture(t, false)
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := inner.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if pod, ok := obj.(*corev1.Pod); ok {
			pod.Labels = map[string]string{workerPoolLabel: pod.Labels[workerPoolLabel]}
		}
		return nil
	}})
	native.client = wrapped
	if _, err := driver(wrapped, native).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("worker without birth confinement was allowed")
	}
	if native.boots != 0 {
		t.Fatal("unconfined worker booted")
	}
}
