package substrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type retirementCreateControl struct {
	*nativeFixture
	t   *testing.T
	key sdk.AllocationKey
}

func (c *retirementCreateControl) CreateActorTemplate(context.Context, *pb.CreateActorTemplateRequest, ...grpc.CallOption) (*pb.ActorTemplate, error) {
	c.t.Fatal("retirement created a native template")
	return nil, nil
}

func (c *retirementCreateControl) CreateActor(context.Context, *pb.CreateActorRequest, ...grpc.CallOption) (*pb.Actor, error) {
	c.t.Fatal("retirement created a native Actor")
	return nil, nil
}

func (c *retirementCreateControl) DeleteActorTemplate(ctx context.Context, request *pb.DeleteActorTemplateRequest, options ...grpc.CallOption) (*pb.ActorTemplate, error) {
	_, saved, err := driver(c.client, c.nativeFixture).read(ctx, c.key)
	if err != nil {
		return nil, err
	}
	template := c.templates[refKey(request.ActorTemplate)]
	if template == nil || saved.Template.UID == "" || saved.Template.UID != template.GetMetadata().GetUid() {
		c.t.Fatal("native template delete preceded durable UID recovery")
	}
	return c.nativeFixture.DeleteActorTemplate(ctx, request, options...)
}

func interruptedResource(record *journalRecord, resource string) *nativeReference {
	switch resource {
	case "pool":
		return &record.RuntimePool
	case "anchor":
		return &record.Anchor
	case "policy":
		return &record.NetworkPolicy
	case "template":
		return &record.Template
	default:
		panic("unknown interrupted resource")
	}
}

// Fail through Ensure, then discard its in-memory journal. Retirement must
// recover from the persisted empty-UID creation intent without another Ensure.
func interruptedResourceCreate(t *testing.T, resource, failure string) (client.Client, *nativeFixture, sdk.WorkloadRequest, *journalRecord) {
	t.Helper()
	c, native, request := fixture(t, false)
	failed := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.CreateOption) error {
			if err := inner.Create(ctx, object, options...); err != nil {
				return err
			}
			matches := false
			switch object := object.(type) {
			case *unstructured.Unstructured:
				matches = resource == "pool" && object.GetKind() == "WorkerPool" && object.GetLabels()[workerInstanceLabel] != ""
			case *corev1.ConfigMap:
				matches = resource == "anchor" && object.Data["journalUID"] != ""
			case *networkingv1.NetworkPolicy:
				matches = resource == "policy"
			}
			if failure == "create response" && matches && !failed {
				failed = true
				return fmt.Errorf("lost %s creation response", resource)
			}
			return nil
		},
		Update: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			if cm, ok := object.(*corev1.ConfigMap); ok && cm.Data[journalDataKey] != "" && failure == "UID save" && !failed {
				var record journalRecord
				if err := json.Unmarshal([]byte(cm.Data[journalDataKey]), &record); err != nil {
					return err
				}
				if interruptedResource(&record, resource).UID != "" {
					failed = true
					return fmt.Errorf("lost %s UID save", resource)
				}
			}
			return inner.Update(ctx, object, options...)
		},
	})
	if resource == "template" && failure == "create response" {
		native.failAfter = "template"
	}
	native.client = wrapped
	if _, err := driver(wrapped, native).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("creation failure was not injected")
	}
	if !failed && !(resource == "template" && failure == "create response" && native.failAfter == "") {
		t.Fatal("failure did not reach the selected resource")
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	ref := interruptedResource(record, resource)
	if !ref.CreateIssued || ref.UID != "" || record.Operation != "ensure" || native.boots != 0 || len(native.actors) != 0 {
		t.Fatalf("did not persist ambiguous creation before native execution: %#v", ref)
	}
	return c, native, request, record
}

func authorizeInterruptedDeletion(t *testing.T, c client.Client, request sdk.WorkloadRequest, record *journalRecord) *api.ExecutionWorkspace {
	t.Helper()
	workspace := bindingWorkspace(t, c, request.Key)
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: request.Sequence, Identity: record.Observation.Identity, Action: api.WorkloadRetirementDelete}
	if err := sdk.ValidateWorkloadRetirement(&request, &record.Observation, workspace.Spec.Retirement, api.WorkloadRetirementStop); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func TestLostCreatedResourceCancellationFullReconcile(t *testing.T) {
	for _, resource := range []string{"pool", "anchor", "policy", "template"} {
		for _, failure := range []string{"create response", "UID save"} {
			t.Run(resource+"/"+failure, func(t *testing.T) {
				c, native, request, record := interruptedResourceCreate(t, resource, failure)
				workspace := authorizeInterruptedDeletion(t, c, request, record)
				deletes := 0
				wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
					Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
						t.Fatal("retirement created a resource")
						return nil
					},
					Delete: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.DeleteOption) error {
						_, saved, err := driver(c, native).read(ctx, request.Key)
						if err != nil {
							return err
						}
						var ref *nativeReference
						switch object.(type) {
						case *unstructured.Unstructured:
							ref = &saved.RuntimePool
						case *networkingv1.NetworkPolicy:
							ref = &saved.NetworkPolicy
						case *corev1.ConfigMap:
							ref = &saved.Anchor
						}
						if ref == nil || ref.UID == "" || ref.UID != string(object.GetUID()) {
							t.Fatal("delete preceded durable resource UID recovery")
						}
						deleteOptions := (&client.DeleteOptions{}).ApplyOptions(options)
						if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.UID == nil || string(*deleteOptions.Preconditions.UID) != ref.UID {
							t.Fatal("delete lost its exact UID precondition")
						}
						deletes++
						return inner.Delete(ctx, object, options...)
					},
				})
				native.client = wrapped
				control := &retirementCreateControl{nativeFixture: native, t: t, key: request.Key}
				r := &ExecutionWorkspaceReconciler{Client: wrapped, Control: control, Config: Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
				for range 12 {
					if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
						t.Fatal(err)
					}
					workspace = bindingWorkspace(t, c, request.Key)
					d := driver(wrapped, native)
					d.control = control
					if _, err := d.EnsureAllocation(t.Context(), request); err != nil {
						t.Fatal(err)
					}
					if workspace.Status.State == api.ExecutionWorkspaceStateDeleted {
						break
					}
				}
				if workspace.Status.Allocation == nil || workspace.Status.Allocation.State != sdk.AllocationDeleted || workspace.Status.Allocation.Identity != record.Observation.Identity || workspace.Status.Allocation.Startup != nil || workspace.Status.Disposition == nil {
					t.Fatalf("cancellation stranded the allocation: %#v", workspace.Status.Allocation)
				}
				_, saved, err := driver(c, native).read(t.Context(), request.Key)
				if err != nil || interruptedResource(saved, resource).UID == "" {
					t.Fatalf("retirement did not persist the recovered lifetime: %v", err)
				}
				if _, err := driver(wrapped, native).EnsureAllocation(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				if deletes == 0 || native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 {
					t.Fatal("retirement failed to remove owned resources without executing a workload")
				}
				pool := poolObject()
				if err := c.Get(t.Context(), client.ObjectKey{Namespace: saved.Placement.Namespace, Name: saved.RuntimePool.Name}, pool); !apierrors.IsNotFound(err) {
					t.Fatalf("private pool survived retirement: %v", err)
				}
			})
		}
	}
}

func TestLostCreatedResourceRetirementFailsClosed(t *testing.T) {
	for _, resource := range []string{"pool", "anchor", "policy", "template"} {
		for _, mutation := range []string{"missing", "spec", "ownership", "empty UID", "UID save"} {
			t.Run(resource+"/"+mutation, func(t *testing.T) {
				c, native, request, record := interruptedResourceCreate(t, resource, "create response")
				ref := interruptedResource(record, resource)
				var object client.Object
				if resource == "template" {
					key := record.Atespace + "/" + ref.Name
					template := native.templates[key]
					switch mutation {
					case "missing":
						delete(native.templates, key)
					case "spec":
						template.SandboxConfig.ConfigName = "foreign"
					case "ownership":
						template.Metadata.Name = "foreign"
					case "empty UID":
						template.Metadata.Uid = ""
					}
				} else {
					switch resource {
					case "pool":
						object = poolObject()
					case "anchor":
						object = &corev1.ConfigMap{}
					case "policy":
						object = &networkingv1.NetworkPolicy{}
					}
					if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Placement.Namespace, Name: ref.Name}, object); err != nil {
						t.Fatal(err)
					}
					switch mutation {
					case "missing":
						if err := c.Delete(t.Context(), object); err != nil {
							t.Fatal(err)
						}
					case "spec":
						switch object := object.(type) {
						case *unstructured.Unstructured:
							object.Object["spec"].(map[string]any)["workerImage"] = "foreign"
						case *corev1.ConfigMap:
							object.Data["journalName"] = "foreign"
						case *networkingv1.NetworkPolicy:
							object.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
						}
					case "ownership":
						object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "foreign", UID: "foreign"}})
					case "empty UID":
						object.SetUID("")
					}
					if mutation != "missing" {
						if err := c.Update(t.Context(), object); err != nil {
							t.Fatal(err)
						}
					}
				}
				workspace := authorizeInterruptedDeletion(t, c, request, record)
				failUIDSave := mutation == "UID save"
				wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
					Update: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.UpdateOption) error {
						if cm, ok := object.(*corev1.ConfigMap); ok && failUIDSave && cm.Data[journalDataKey] != "" {
							var saved journalRecord
							if err := json.Unmarshal([]byte(cm.Data[journalDataKey]), &saved); err != nil {
								return err
							}
							if interruptedResource(&saved, resource).UID != "" {
								return fmt.Errorf("recovery UID save unavailable")
							}
						}
						return inner.Update(ctx, object, options...)
					},
				})
				r := &ExecutionWorkspaceReconciler{Client: wrapped, Control: native, Config: Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
				failures := 0
				for range 6 {
					_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
					if err == nil {
						continue // Earlier owned infrastructure can retire first.
					}
					failures++
					if mutation == "missing" && !strings.Contains(err.Error(), "creation outcome is unresolved") || mutation == "UID save" && !strings.Contains(err.Error(), "recovery UID save unavailable") || mutation != "missing" && mutation != "UID save" && !errors.Is(err, sdk.ErrStaleIdentity) {
						t.Fatalf("unexpected retirement failure: %v", err)
					}
				}
				_, saved, err := driver(c, native).read(t.Context(), request.Key)
				if err != nil || failures == 0 || interruptedResource(saved, resource).UID != "" || saved.Observation.State == sdk.AllocationDeleted || native.boots != 0 {
					t.Fatalf("unresolved resource was accepted as retired: %v", err)
				}
				if resource == "template" {
					if mutation != "missing" && native.templates[record.Atespace+"/"+ref.Name] == nil {
						t.Fatal("unverified native template was deleted")
					}
				} else if mutation != "missing" {
					current := object.DeepCopyObject().(client.Object)
					if err := c.Get(t.Context(), client.ObjectKeyFromObject(object), current); err != nil || !reflect.DeepEqual(object, current) {
						t.Fatalf("unverified resource was mutated or deleted: %v", err)
					}
				}
				if mutation == "UID save" {
					failUIDSave = false
					for range 12 {
						if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
							t.Fatal(err)
						}
						workspace = bindingWorkspace(t, c, request.Key)
						if workspace.Status.State == api.ExecutionWorkspaceStateDeleted {
							break
						}
					}
					_, saved, err := driver(c, native).read(t.Context(), request.Key)
					if err != nil || workspace.Status.State != api.ExecutionWorkspaceStateDeleted || interruptedResource(saved, resource).UID == "" || native.boots != 0 {
						t.Fatalf("UID save retry did not retire the original allocation: %v", err)
					}
				}
			})
		}
	}
}
