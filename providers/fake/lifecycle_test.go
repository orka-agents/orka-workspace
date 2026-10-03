package fake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	fakev1alpha1 "github.com/orka-agents/orka-workspace/providers/fake/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func fixture(t *testing.T) (client.Client, workspaceprovider.WorkloadRequest) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, workspacev1alpha1.AddToScheme, fakev1alpha1.AddToScheme} {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}, &workspacev1alpha1.ExecutionWorkspaceProvider{}, &workspacev1alpha1.ExecutionWorkspacePool{}).Build()
	request, err := ConformanceFixture(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	return c, request
}

func deletionPolicy() workspacev1alpha1.ExecutionWorkspaceDeletionPolicy {
	return workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionRetain, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
}

func TestLifecycleConformance(t *testing.T) {
	c, request := fixture(t)
	if err := conformance.Check(t.Context(), func() workspaceprovider.Lifecycle { return New(c) }, request); err != nil {
		t.Fatal(err)
	}
}

// faultClient can lose a response after committing, fail before committing, or
// return one resourceVersion conflict. It injects at a specific journal write.
type faultClient struct {
	client.Client
	failAt   int
	writes   int
	commit   bool
	conflict bool
}

func (c *faultClient) Update(ctx context.Context, object client.Object, options ...client.UpdateOption) error {
	if _, ok := object.(*corev1.ConfigMap); !ok {
		return c.Client.Update(ctx, object, options...)
	}
	c.writes++
	if c.writes != c.failAt {
		return c.Client.Update(ctx, object, options...)
	}
	if c.conflict {
		return apierrors.NewConflict(corev1.Resource("configmaps"), object.GetName(), errors.New("competing writer"))
	}
	if c.commit {
		if err := c.Client.Update(ctx, object, options...); err != nil {
			return err
		}
	}
	return errors.New("connection lost")
}

func TestEnsureRecoversDurableIntentAfterLostResponse(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint("commit=", commit), func(t *testing.T) {
			c, request := fixture(t)
			driver := New(&faultClient{Client: c, failAt: 1, commit: commit})
			if _, err := driver.EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("expected lost response")
			}
			persisted, err := New(c).Observe(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := New(c).EnsureAllocation(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Identity != persisted.Identity {
				t.Fatal("retry replaced committed instance identity")
			}
			if err := workspaceprovider.ValidateStartup(request, recovered); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetirementIntentWithdrawsReadinessBeforeLostResponse(t *testing.T) {
	for _, operation := range []string{"stop", "delete"} {
		for _, write := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s-write-%d", operation, write), func(t *testing.T) {
				c, request := fixture(t)
				created, err := New(c).EnsureAllocation(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				if operation == "delete" {
					if _, err := New(c).StopInstance(t.Context(), request.Key, created.Identity); err != nil {
						t.Fatal(err)
					}
				}
				fault := New(&faultClient{Client: c, failAt: write, commit: true})
				if operation == "stop" {
					_, err = fault.StopInstance(t.Context(), request.Key, created.Identity)
				} else {
					_, err = fault.DeleteAllocation(t.Context(), request.Key, created.Identity, deletionPolicy())
				}
				if err == nil {
					t.Fatal("expected lost response")
				}
				for _, read := range []func() (workspaceprovider.AllocationObservation, error){
					func() (workspaceprovider.AllocationObservation, error) {
						return New(c).Observe(t.Context(), request.Key)
					},
					func() (workspaceprovider.AllocationObservation, error) {
						return New(c).EnsureAllocation(t.Context(), request)
					},
				} {
					observed, err := read()
					if err != nil {
						t.Fatal(err)
					}
					if observed.State == workspaceprovider.AllocationReady || observed.Startup != nil {
						t.Fatal("durable retirement intent still advertises startup readiness")
					}
					if observed.Identity != created.Identity {
						t.Fatal("lost identity during retirement")
					}
				}
				var recovered workspaceprovider.AllocationObservation
				if operation == "stop" {
					recovered, err = New(c).StopInstance(t.Context(), request.Key, created.Identity)
				} else {
					recovered, err = New(c).DeleteAllocation(t.Context(), request.Key, created.Identity, deletionPolicy())
				}
				if err != nil {
					t.Fatal(err)
				}
				expected := workspaceprovider.AllocationStopped
				if operation == "delete" {
					expected = workspaceprovider.AllocationDeleted
				}
				if recovered.State != expected {
					t.Fatalf("recovered state = %q", recovered.State)
				}
			})
		}
	}
}

func TestCASConflictRetainsIdentity(t *testing.T) {
	c, request := fixture(t)
	created, err := New(&faultClient{Client: c, failAt: 1, conflict: true}).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := New(c).Observe(t.Context(), request.Key)
	if err != nil || created.Identity != observed.Identity {
		t.Fatalf("CAS retry changed identity: %v", err)
	}
}

func TestJournalBoundAndRuntimeRejection(t *testing.T) {
	for _, kind := range []string{"oversize", "runtime"} {
		t.Run(kind, func(t *testing.T) {
			c, request := fixture(t)
			if kind == "oversize" {
				request.Args = []string{strings.Repeat("x", MaxJournalBytes)}
			} else {
				request.Runtime = &workspaceprovider.RuntimeWorkload{}
			}
			request.Revision, _ = workspaceprovider.WorkloadRevision(request)
			if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("unsupported request accepted")
			}
			var journals corev1.ConfigMapList
			if err := c.List(t.Context(), &journals); err != nil {
				t.Fatal(err)
			}
			if len(journals.Items) != 0 {
				t.Fatal("rejected request wrote a journal")
			}
		})
	}
}

func TestDeletedTombstoneRetainsOwnerAndPolicy(t *testing.T) {
	c, request := fixture(t)
	created, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, created.Identity); err != nil {
		t.Fatal(err)
	}
	deleted, err := New(c).DeleteAllocation(t.Context(), request.Key, created.Identity, deletionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := workspaceprovider.ValidateDeletedDisposition(deleted.Disposition, deletionPolicy()); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].UID != request.Key.WorkspaceUID {
		t.Fatal("journal tombstone lost workspace ownership")
	}
	changedPolicy := deletionPolicy()
	changedPolicy.PersistentVolumes = workspacev1alpha1.WorkspaceDeletionActionDelete
	if _, err := New(c).DeleteAllocation(t.Context(), request.Key, created.Identity, changedPolicy); !errors.Is(err, workspaceprovider.ErrRequestConflict) {
		t.Fatalf("changed deletion policy accepted: %v", err)
	}
}
