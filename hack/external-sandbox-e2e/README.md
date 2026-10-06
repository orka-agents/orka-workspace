# Installed Sandbox persistence proof

Run `bash scripts/external-sandbox-e2e.sh` from the shared repository. It uses
the existing kindctl `external-workspace` cluster, upstream agent-sandbox
v1.0.3, the external Sandbox provider, and the cluster's dynamically provisioned
`standard` StorageClass with Delete reclaim policy. It preserves the shared
ownership policy, fake provider, and concurrent Orka RuntimeSession proof.

The fixture creates a credential-free HTTP listener with one writable durable
mount. It writes data, authorizes exact DataOnly suspension, independently
observes the first Pod's absence, cold resumes with a new Pod UID and rotated
public bootstrap nonce, verifies unchanged PVC/PV identities and file contents,
then observes Pod/PVC/PV deletion. A separate service account with admission
authority limited to the fixture namespace supplies class status and workspace
admission. The installed core's watch namespace and permissions are unchanged.

This proves installed-backend allocation and filesystem persistence. The
listener does not perform Orka credential bootstrap or a RuntimeSession; the
separate fake/core proof covers that path. Reports and frozen source builds go
to a temporary artifact directory outside the repository. The cluster and
retired proof resources remain available for inspection; a successful run
removes its fixture service account and admission Role/RoleBinding.
