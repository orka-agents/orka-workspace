package fake

import (
	"encoding/json"
	"reflect"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPodDeletionDoesNotClaimCoreCredentialCleanup(t *testing.T) {
	c, request := runtimeFixture(t)
	secret := createCoreCredentialFixture(t, c, request.Key.Namespace)
	created, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, created.Identity); err != nil {
		t.Fatal(err)
	}
	// A distinct observation confirms exact Pod absence after deletion.
	stopped, err := New(c).StopInstance(t.Context(), request.Key, created.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != workspaceprovider.AllocationStopped {
		t.Fatal("exact Pod absence did not close the stopped fence")
	}
	deleted, err := New(c).DeleteAllocation(t.Context(), request.Key, created.Identity, deletionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	assertUnownedCredentialDisposition(t, deleted.Disposition)
	if deleted.State != workspaceprovider.AllocationDeleted {
		t.Fatal("provider compute did not reach Deleted")
	}
	pods := &corev1.PodList{}
	if err := c.List(t.Context(), pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatal("deleted allocation still has a Pod")
	}
	// Both read-only recovery and a repeated deletion must preserve the honest
	// disposition after a provider restart.
	for _, operation := range []func() (workspaceprovider.AllocationObservation, error){
		func() (workspaceprovider.AllocationObservation, error) {
			return New(c).Observe(t.Context(), request.Key)
		},
		func() (workspaceprovider.AllocationObservation, error) {
			return New(c).DeleteAllocation(t.Context(), request.Key, created.Identity, deletionPolicy())
		},
	} {
		observation, err := operation()
		if err != nil {
			t.Fatal(err)
		}
		assertUnownedCredentialDisposition(t, observation.Disposition)
	}
	workspace := workspaceFor(t, c, request.Key)
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: created.Identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
	workspace.Spec.Lifecycle.DeletionPolicy = deletionPolicy()
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	reconciler := &FakeExecutionWorkspaceReconciler{Client: c}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
		t.Fatal(err)
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateDeleted || workspace.Status.Allocation == nil {
		t.Fatal("controller did not publish the deleted allocation")
	}
	assertUnownedCredentialDisposition(t, workspace.Status.Disposition)
	assertUnownedCredentialDisposition(t, workspace.Status.Allocation.Disposition)
	assertCoreCredentialFixtureUnchanged(t, c, secret)
}

func TestNeverAllocatedDeletionDoesNotClaimCoreCredentialCleanup(t *testing.T) {
	c, request := fixture(t)
	secret := createCoreCredentialFixture(t, c, request.Key.Namespace)
	workspace := workspaceFor(t, c, request.Key)
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Lifecycle.DeletionPolicy = deletionPolicy()
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	reconciler := &FakeExecutionWorkspaceReconciler{Client: c}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
		t.Fatal(err)
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateDeleted || workspace.Status.Allocation != nil {
		t.Fatal("never-allocated workspace did not publish deletion without an allocation")
	}
	assertUnownedCredentialDisposition(t, workspace.Status.Disposition)
	assertCoreCredentialFixtureUnchanged(t, c, secret)
}

func TestLegacyDeletedCredentialDispositionRecoveryIsReadOnly(t *testing.T) {
	c, request, tombstone, legacy := legacyCredentialTombstone(t)
	want := legacy.Observation
	disposition := *want.Disposition
	disposition.AccessCredentials = workspacev1alpha1.DispositionNotApplicable
	disposition.EphemeralSecrets = workspacev1alpha1.DispositionNotApplicable
	want.Disposition = &disposition
	for _, operation := range []func() (workspaceprovider.AllocationObservation, error){
		func() (workspaceprovider.AllocationObservation, error) {
			return New(c).Observe(t.Context(), request.Key)
		},
		func() (workspaceprovider.AllocationObservation, error) {
			return New(c).DeleteAllocation(t.Context(), request.Key, legacy.Observation.Identity, deletionPolicy())
		},
		func() (workspaceprovider.AllocationObservation, error) {
			return New(c).EnsureAllocation(t.Context(), request)
		},
	} {
		observation, err := operation()
		if err != nil {
			t.Fatal(err)
		}
		assertUnownedCredentialDisposition(t, observation.Disposition)
		if !reflect.DeepEqual(want, observation) {
			t.Fatal("legacy recovery changed evidence beyond the two unowned credential categories")
		}
		assertCredentialJournalUnchanged(t, c, tombstone)
	}
}

func TestLegacyDeletedCredentialDispositionHistoryRecoveryIsReadOnly(t *testing.T) {
	c, request, tombstone, legacy := legacyCredentialTombstone(t)
	history := tombstone.DeepCopy()
	history.Name = historyKey(request.Key, request.Sequence).Name
	history.ResourceVersion = ""
	history.UID = "legacy-history-uid"
	if err := c.Create(t.Context(), history); err != nil {
		t.Fatal(err)
	}
	// Seed a later current sequence to exercise public recovery of the prior
	// tombstone. Recovery must never resurrect the historical Deleted instance.
	next := request
	next.Sequence++
	next.PreviousInstance = &legacy.Observation.Identity
	var err error
	next.Revision, err = workspaceprovider.WorkloadRevision(next)
	if err != nil {
		t.Fatal(err)
	}
	current, err := newRecord(next, legacy.Observation.Identity.AllocationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := New(c).save(t.Context(), tombstone, current); err != nil {
		t.Fatal(err)
	}
	observation, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertUnownedCredentialDisposition(t, observation.Disposition)
	if observation.State != workspaceprovider.AllocationDeleted || observation.Identity != legacy.Observation.Identity || observation.Sequence != request.Sequence {
		t.Fatal("history recovery changed the retired instance fence")
	}
	assertCredentialJournalUnchanged(t, c, history)
	assertCredentialJournalUnchanged(t, c, tombstone)
}

func TestLegacyDeletedCredentialDispositionRejectsMissingOrMalformedEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*journalRecord)
	}{
		{"missing disposition", func(record *journalRecord) { record.Observation.Disposition = nil }},
		{"missing policy", func(record *journalRecord) { record.DeletionPolicy = nil }},
		{"credentials pending", func(record *journalRecord) {
			record.Observation.Disposition.AccessCredentials = workspacev1alpha1.DispositionPending
		}},
		{"ephemeral secrets failed", func(record *journalRecord) {
			record.Observation.Disposition.EphemeralSecrets = workspacev1alpha1.DispositionFailed
		}},
		{"compute active", func(record *journalRecord) {
			record.Observation.Disposition.Compute = workspacev1alpha1.DispositionActive
		}},
		{"storage policy mismatch", func(record *journalRecord) {
			record.Observation.Disposition.PersistentVolumes = workspacev1alpha1.DispositionDeleted
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, request, tombstone, legacy := legacyCredentialTombstone(t)
			tc.mutate(legacy)
			if err := New(c).save(t.Context(), tombstone, legacy); err != nil {
				t.Fatal(err)
			}
			for _, operation := range []func() (workspaceprovider.AllocationObservation, error){
				func() (workspaceprovider.AllocationObservation, error) {
					return New(c).Observe(t.Context(), request.Key)
				},
				func() (workspaceprovider.AllocationObservation, error) {
					return New(c).DeleteAllocation(t.Context(), request.Key, legacy.Observation.Identity, deletionPolicy())
				},
			} {
				observation, err := operation()
				if err == nil || observation.State == workspaceprovider.AllocationDeleted || observation.Disposition != nil {
					t.Fatal("legacy recovery synthesized cleanup evidence from an invalid disposition")
				}
				assertCredentialJournalUnchanged(t, c, tombstone)
			}
		})
	}
}

func legacyCredentialTombstone(t *testing.T) (client.Client, workspaceprovider.WorkloadRequest, *corev1.ConfigMap, *journalRecord) {
	t.Helper()
	c, request := fixture(t)
	created, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, created.Identity); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).DeleteAllocation(t.Context(), request.Key, created.Identity, deletionPolicy()); err != nil {
		t.Fatal(err)
	}
	tombstone := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(request.Key), tombstone); err != nil {
		t.Fatal(err)
	}
	var legacy journalRecord
	if err := json.Unmarshal([]byte(tombstone.Data[journalDataKey]), &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Observation.Disposition.AccessCredentials = workspacev1alpha1.DispositionRevoked
	legacy.Observation.Disposition.EphemeralSecrets = workspacev1alpha1.DispositionDeleted
	if err := New(c).save(t.Context(), tombstone, &legacy); err != nil {
		t.Fatal(err)
	}
	return c, request, tombstone, &legacy
}

func assertCredentialJournalUnchanged(t *testing.T, c client.Client, before *corev1.ConfigMap) {
	t.Helper()
	current := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(before), current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, current) {
		t.Fatal("credential disposition recovery rewrote the journal tombstone")
	}
}

func assertUnownedCredentialDisposition(t *testing.T, disposition *workspacev1alpha1.ExecutionWorkspaceDisposition) {
	t.Helper()
	if disposition == nil || disposition.AccessCredentials != workspacev1alpha1.DispositionNotApplicable || disposition.EphemeralSecrets != workspacev1alpha1.DispositionNotApplicable {
		t.Fatal("provider claimed cleanup of core-owned credentials")
	}
	if err := workspaceprovider.ValidateDeletedDisposition(disposition, deletionPolicy()); err != nil {
		t.Fatalf("provider disposition violates lifecycle cleanup contract: %v", err)
	}
	if err := workspaceprovider.ValidateInteractiveDeletedDisposition(disposition, deletionPolicy()); err == nil {
		t.Fatal("provider disposition incorrectly certifies core interactive credential cleanup")
	}
}

func createCoreCredentialFixture(t *testing.T, c client.Client, namespace string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "core-attachment-credential", UID: "core-credential-uid", Labels: map[string]string{"workspace.orka.ai/controller-name": "core"}}, Data: map[string][]byte{"token": []byte("fixture-only-token")}}
	if err := c.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	return secret.DeepCopy()
}

func assertCoreCredentialFixtureUnchanged(t *testing.T, c client.Client, before *corev1.Secret) {
	t.Helper()
	current := &corev1.Secret{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(before), current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, current) {
		t.Fatal("provider changed the independently owned credential Secret")
	}
}
