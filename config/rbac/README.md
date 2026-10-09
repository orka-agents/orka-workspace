# Workspace authorization templates

The shared policy requires ordinary API permissions plus a virtual verb. Kubernetes never handles an HTTP `admit`, `configure`, or `provider-status` operation. CEL asks the authorizer whether the authenticated request identity holds that verb.

- Core gets `admit` on shared resources for admission, intent, owner references, routing labels, core conditions and finalizers. Bind `core-admission-role.yaml` to the existing core ServiceAccount.
- Operators get `configure` for configuration specs and descriptive metadata. Bind `operator-role.yaml` to the intended operator identities.
- Providers get `provider-status` on `executionworkspaceproviders` with `resourceNames` set to their exact provider registration. Customize both names and the ServiceAccount namespace in `provider-status-role.yaml`. A namespaced RoleBinding cannot authorize this cluster-scoped check; use the shown ClusterRoleBinding.

Provider RBAC also needs its own namespaced journal ConfigMap and leader-election Lease permissions, configuration reads, and backend-native permissions. Those belong to the provider deployment, not this shared template. Providers receive neither `admit` nor `configure` and cannot change another registration's status.

Core sets `workspace.orka.ai/controller-name` to a stable DNS-compatible label value. A provider may manage observation annotations and cleanup finalizers under that value followed by `/`. The raw `spec.controllerName` can contain `/` and must not be copied into a Kubernetes label. For checkpoints, core must additionally pin `workspace.orka.ai/provider-name` after resolving the exact source workspace. A checkpoint without that routing label cannot receive provider status updates.

Install the policy only with all required grants and controller changes. The baseline in-tree fake writes core `Compatible`, and the ACP adapter writes core `Quarantined`; those conflicting writes are rejected. This bundle is for the extracted ownership model, not an in-place substitute for the baseline policy while unadapted controllers are running. Shared CRDs are installed once by the platform owner; provider uninstall leaves them and the shared policy intact.
