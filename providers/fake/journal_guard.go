package fake

import (
	"context"
	"fmt"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const journalRequiredAnnotation = "fake.workspace.orka.ai/journal-required"

// guardJournal commits a provider-owned recovery obligation before native effects.
// Its random allocation ID survives sequence changes and detects a missing or
// replaced journal even when a create response was lost before status publication.
func (d *Lifecycle) guardJournal(ctx context.Context, key workspaceprovider.AllocationKey, record *journalRecord) error {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != key.ProviderUID {
		return workspaceprovider.ErrStaleIdentity
	}
	existing := workspace.Annotations[journalRequiredAnnotation]
	if existing != "" {
		if existing != record.Observation.Identity.AllocationID {
			return fmt.Errorf("required allocation journal was replaced: %w", workspaceprovider.ErrStaleIdentity)
		}
		return nil
	}
	before := workspace.DeepCopy()
	if workspace.Annotations == nil {
		workspace.Annotations = map[string]string{}
	}
	workspace.Annotations[journalRequiredAnnotation] = record.Observation.Identity.AllocationID
	return d.client.Patch(ctx, workspace, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (d *Lifecycle) journalRequired(ctx context.Context, key workspaceprovider.AllocationKey) (bool, error) {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		return false, err
	}
	if workspace.UID != key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != key.ProviderUID {
		return false, workspaceprovider.ErrStaleIdentity
	}
	return workspace.Annotations[journalRequiredAnnotation] != "" || workspace.Status.Allocation != nil || workspace.Status.ExternalID != "", nil
}

func (d *Lifecycle) validateJournalGuard(ctx context.Context, key workspaceprovider.AllocationKey, record *journalRecord) error {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != key.ProviderUID {
		return workspaceprovider.ErrStaleIdentity
	}
	required := workspace.Annotations[journalRequiredAnnotation]
	if required != "" && required != record.Observation.Identity.AllocationID {
		return fmt.Errorf("required allocation journal identity changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	return nil
}
