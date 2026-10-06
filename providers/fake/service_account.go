package fake

import (
	"context"
	"fmt"

	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// verifyRuntimeServiceAccount uses the uncached lifecycle client before any
// allocation effects. Token automount does not control image-pull credentials.
func (d *Lifecycle) verifyRuntimeServiceAccount(ctx context.Context, request workspaceprovider.WorkloadRequest) error {
	if request.Runtime == nil {
		return nil
	}
	namespace := request.Runtime.Template.Namespace
	if namespace == "" {
		namespace = request.Key.Namespace
	}
	spec := &request.Runtime.Template.Spec
	name := spec.ServiceAccountName
	if name == "" {
		name = spec.DeprecatedServiceAccount
	}
	if name == "" {
		name = "default"
	}
	account := &corev1.ServiceAccount{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, account); err != nil {
		return fmt.Errorf("read runtime ServiceAccount: %w", err)
	}
	if len(account.ImagePullSecrets) != 0 {
		return fmt.Errorf("runtime ServiceAccount contains image-pull credentials")
	}
	return nil
}
