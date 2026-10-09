# Shared installation bundle

`kubectl apply --server-side -k config` installs the five shared CRDs and the field-ownership admission policy. Server-side apply avoids the client-side annotation size limit for the embedded Pod schema. The platform owner installs this bundle once. Provider deployments install their own configuration kinds and RBAC, and do not uninstall the shared bundle.

The standalone live proof passes on Kubernetes v1.37.0, including ownership denials and two-replica leader election. Other server versions are not certified. The bundle requires `admissionregistration.k8s.io/v1` `ValidatingAdmissionPolicy` and its CEL authorizer library. Tests also compile and evaluate the shipped policy with the Kubernetes `v0.37.0` API-server implementation.

Attachment intent must match its positive monotonic counter. Changing a Task, credential, or expiry advances the epoch; revocation preserves the high-water mark. Unchanged legacy intent can still be revoked and deleted. Providers refuse positive acknowledgement of a stored counter mismatch, while Core clears it with the preserved high-water counter.

Configure authorization before controllers begin reconciling:

1. Bind the [core admission role](rbac/core-admission-role.yaml) to the Orka core ServiceAccount. It adds virtual `admit` permission; keep the controller's ordinary API permissions separately.
2. Bind the [configuration role](rbac/operator-role.yaml) to the intended operators. Configuration writes require `configure` in addition to ordinary create/update/patch permissions.
3. Customize and bind the [provider role](rbac/provider-status-role.yaml) for each exact provider registration name and ServiceAccount. Its `provider-status` grant authorizes only observations for that registration. Add namespaced journal, leader-election, typed configuration and native backend permissions in the provider's deployment.
4. Install the bundle and the compatible controllers. Verify that an unauthorized ServiceAccount receives Forbidden for provider status, routing changes and core admission. Granting a virtual verb without installing its policy does not enforce field ownership.

RBAC templates are deliberately excluded from the kustomization because their subjects and provider registration names are installation-specific. Cluster-admin already holds these virtual verbs through wildcard permission. Do not distribute that authority to a provider controller.

This policy is for the extracted ownership contract. It is not a replacement for Orka's legacy policy while unadapted in-tree controllers are running. The old fake and ACP controllers have condition-ownership conflicts described in [ADR 0001](../docs/adr/0001-ownership.md). Legacy ACP materialization markers and Substrate checkpoint finalizers remain protected by the original Orka policy until the explicit migration. Installing this bundle does not migrate stored RuntimePools or transfer ownership of retained data.

For provider-owned metadata, core sets a stable DNS-compatible `workspace.orka.ai/controller-name` label. The corresponding annotation/finalizer prefix is that value plus `/`. A raw controller name containing `/` is not a valid label value. Checkpoints also require core to establish `workspace.orka.ai/provider-name` from the exact source workspace before the provider can publish status. Both routing labels become immutable once assigned.
