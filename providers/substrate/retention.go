package substrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A bounded provider-owned ConfigMap serializes suspended occupancy. A pending
// capture reserves its slot before any native effect; resume/deletion release it.
// Missing occupied records fail closed rather than silently reset the quota.
func retentionKey(record *journalRecord) types.NamespacedName {
	sum := sha256.Sum256([]byte(string(record.Request.Key.ProviderUID) + "\x00" + string(record.Request.Runtime.ClassBinding.UID)))
	return types.NamespacedName{Namespace: record.Request.Key.Namespace, Name: "substrate-retention-" + hex.EncodeToString(sum[:20])}
}
func (d *Lifecycle) reserveSuspended(ctx context.Context, record *journalRecord) error {
	if record.MaxSuspended == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		key := retentionKey(record)
		cm := &corev1.ConfigMap{}
		err := d.client.Get(ctx, key, cm)
		if apierrors.IsNotFound(err) {
			var journals corev1.ConfigMapList
			if err := d.client.List(ctx, &journals, client.InNamespace(key.Namespace), client.MatchingLabels{providerLabel: string(record.Request.Key.ProviderUID)}); err != nil {
				return err
			}
			for _, object := range journals.Items {
				var existing journalRecord
				if json.Unmarshal([]byte(object.Data[journalDataKey]), &existing) == nil && existing.Version == journalVersion && existing.Request.Runtime != nil && existing.Request.Runtime.ClassBinding.UID == record.Request.Runtime.ClassBinding.UID && existing.Operation == "suspend" && existing.Observation.State != sdk.AllocationDeleted {
					return fmt.Errorf("suspended occupancy journal is missing while retained lifecycle evidence remains")
				}
			}
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{providerLabel: string(record.Request.Key.ProviderUID), "substrate.workspace.orka.ai/class-uid": string(record.Request.Runtime.ClassBinding.UID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspaceClass", Name: record.Request.Runtime.ClassBinding.Name, UID: record.Request.Runtime.ClassBinding.UID}}}, Data: map[string]string{"limit": strconv.Itoa(int(*record.MaxSuspended))}}
			if err := d.client.Create(ctx, cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, err)
				}
				return err
			}
		} else if err != nil {
			return err
		}
		if cm.Labels[providerLabel] != string(record.Request.Key.ProviderUID) || cm.Labels["substrate.workspace.orka.ai/class-uid"] != string(record.Request.Runtime.ClassBinding.UID) || cm.Data["limit"] != strconv.Itoa(int(*record.MaxSuspended)) {
			return sdk.ErrStaleIdentity
		}
		owner := "workspace-" + string(record.Request.Key.WorkspaceUID)
		if cm.Data[owner] == journalKey(record.Request.Key).Name {
			return nil
		}
		if record.Operation == "suspend" {
			return fmt.Errorf("suspended workspace lost its reserved occupancy identity")
		}
		if len(cm.Data)-1 >= int(*record.MaxSuspended) {
			return fmt.Errorf("native class suspended workspace capacity is full")
		}
		cm.Data[owner] = journalKey(record.Request.Key).Name
		return d.client.Update(ctx, cm)
	})
}
func (d *Lifecycle) releaseSuspended(ctx context.Context, record *journalRecord) error {
	if record.MaxSuspended == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm := &corev1.ConfigMap{}
		if err := d.client.Get(ctx, retentionKey(record), cm); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if cm.Labels[providerLabel] != string(record.Request.Key.ProviderUID) || cm.Labels["substrate.workspace.orka.ai/class-uid"] != string(record.Request.Runtime.ClassBinding.UID) {
			return sdk.ErrStaleIdentity
		}
		delete(cm.Data, "workspace-"+string(record.Request.Key.WorkspaceUID))
		return d.client.Update(ctx, cm)
	})
}
