#!/usr/bin/env python3
"""Render an isolated Core/provider Task proof using the existing Core fixture."""
import json
import os
import pathlib
import subprocess
import sys

namespace, core_image, runtime_image, proof_image, provider_image, worker_image, certificate, core_role = sys.argv[1:]
fixture = pathlib.Path(__file__).resolve().parents[2] / "external-workspace-e2e"
base = json.loads(subprocess.check_output([
    sys.executable, str(fixture / "core-install.py"), core_image, runtime_image, certificate]))


def rewrite(value):
    if isinstance(value, dict):
        return {key: rewrite(item) for key, item in value.items()}
    if isinstance(value, list):
        return [rewrite(item) for item in value]
    if isinstance(value, str):
        return (value.replace("external-workspace-core", "external-substrate-core")
                .replace("orka-system", namespace).replace("orka-runtimes", namespace))
    return value


objects = rewrite(base["items"])
# Both the watched and runtime namespace are this run's single isolated scope.
seen = set()
objects = [obj for obj in objects if obj["kind"] != "Namespace" or
           not (obj["metadata"]["name"] in seen or seen.add(obj["metadata"]["name"]))]
for obj in objects:
    if obj["kind"] == "ClusterRole" and obj["metadata"]["name"] == "external-substrate-core-fixture":
        obj["rules"][0] = {"apiGroups": ["substrate.workspace.orka.ai"],
                           "resources": ["substrateproviderconfigs", "substrateworkspaceprofiles"],
                           "verbs": ["get", "list", "watch"]}
    if obj["kind"] == "Deployment":
        pod_spec = obj["spec"]["template"]["spec"]
        manager = pod_spec["containers"][0]
        manager["imagePullPolicy"] = "IfNotPresent"
        # Core reaches routed native runtimes only through atenet-router with
        # its allowlisted pod certificate (install-backend.sh) and verifies the
        # router against the Substrate service trust bundle.
        manager.setdefault("args", []).extend([
            "--substrate-api-cert-file=/run/substrate-client/credential-bundle.pem",
            "--substrate-api-key-file=/run/substrate-client/credential-bundle.pem",
            "--substrate-api-ca-file=/run/substrate-server/trust-bundle.pem",
            "--substrate-router-url=https://atenet-router.ate-system.svc:443"])
        # Optional Core tuning for latency runs (core/latency.sh), recorded in
        # the rendered Deployment: Task reconcile concurrency and log level.
        if os.environ.get("ORKA_TASK_MAX_CONCURRENT_RECONCILES"):
            manager["args"].append("--task-max-concurrent-reconciles=" + os.environ["ORKA_TASK_MAX_CONCURRENT_RECONCILES"])
        if os.environ.get("ORKA_CORE_LOG_LEVEL"):
            manager["args"].append("--zap-log-level=" + os.environ["ORKA_CORE_LOG_LEVEL"])
        # A Claude-typed fixture profile runs the same deterministic runtime
        # image: Core selects the provider through ORKA_ACP_PROVIDER, which
        # overrides the image's default.
        if os.environ.get("ORKA_CORE_CLAUDE_FIXTURE") == "1":
            manager["args"].append("--acp-claude-runtime-image=" + runtime_image)
        # Extra Core flags for a measured variant, space separated.
        manager["args"].extend(os.environ.get("ORKA_CORE_EXTRA_ARGS", "").split())
        manager.setdefault("volumeMounts", []).extend([
            {"name": "substrate-client", "mountPath": "/run/substrate-client", "readOnly": True},
            {"name": "substrate-server", "mountPath": "/run/substrate-server", "readOnly": True}])
        pod_spec.setdefault("volumes", []).extend([
            {"name": "substrate-client", "projected": {"sources": [{"podCertificate": {
                "signerName": "podidentity.podcert.ate.dev/identity", "keyType": "ECDSAP256",
                "credentialBundlePath": "credential-bundle.pem"}}]}},
            {"name": "substrate-server", "projected": {"sources": [{"clusterTrustBundle": {
                "signerName": "servicedns.podcert.ate.dev/identity",
                "labelSelector": {"matchLabels": {"podcert.ate.dev/canarying": "live"}},
                "path": "trust-bundle.pem"}}]}}])
        # Optional clean-room publication wiring for write-intent latency runs
        # (round 10): the Workspace/Publisher and its Secrets come from
        # ORKA_CORE_PRE_INSTALL_HOOK, so Core starts wired and never restarts.
        if os.environ.get("ORKA_CORE_PUBLICATION") == "1":
            manager.setdefault("env", []).extend([
                {"name": "ORKA_WORKSPACE_PUBLISHER_URL", "value": f"http://r10-publisher.{namespace}.svc:8080"},
                {"name": "ORKA_WORKSPACE_PUBLISHER_CONTROLLER_TOKEN_FILE", "value": "/var/run/orka/publisher-auth/controller-token"},
                {"name": "ORKA_WORKSPACE_PUBLISHER_CAPABILITY_SECRET_FILE", "value": "/var/run/orka/publisher-auth/operation-capability-secret"},
                {"name": "ORKA_ACP_ARTIFACT_CAPABILITY_SECRET_FILE", "value": "/var/run/orka/artifact/secret"}])
            manager["volumeMounts"].extend([
                {"name": "r10-publisher-auth", "mountPath": "/var/run/orka/publisher-auth", "readOnly": True},
                {"name": "r10-artifact", "mountPath": "/var/run/orka/artifact", "readOnly": True}])
            pod_spec["volumes"].extend([
                {"name": "r10-publisher-auth", "secret": {"secretName": "r10-publisher-auth", "defaultMode": 0o440}},
                {"name": "r10-artifact", "secret": {"secretName": "r10-artifact-capability", "defaultMode": 0o440}}])
        # Agent Substrate's DNS relay answers only allowlisted names, and the
        # sandbox resolver (ndots:5 with the worker Pod's search list) never
        # sends the absolute short "<svc>.<ns>.svc." form kube-dns resolves.
        # Core freezes these origins into the runtime egress allowlist, so
        # they must be cluster-domain FQDNs.
        manager["args"] = [arg.replace(".svc:8080", ".svc.cluster.local:8080")
                           if arg.startswith(("--acp-provider-proxy-base-url=", "--controller-url=")) else arg
                           for arg in manager["args"]]

# Core's API authenticates ServiceAccount callers (such as core/cleanup.sh's
# Session cleanup) by TokenReview, as the released chart grants through
# config/rbac/metrics_auth_role.yaml.
add_tokenreview = [
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": "external-substrate-core-tokenreview"},
     "rules": [{"apiGroups": ["authentication.k8s.io"], "resources": ["tokenreviews"], "verbs": ["create"]}]},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": {"name": "external-substrate-core-tokenreview"},
     "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "external-substrate-core-tokenreview"},
     "subjects": [{"kind": "ServiceAccount", "name": "orka-controller-manager", "namespace": namespace}]}]
objects.extend(add_tokenreview)

role = json.loads(pathlib.Path(core_role).read_text())
role["metadata"] = {"name": "external-substrate-core"}
objects.append(role)


def add(version, kind, name, ns=None, **fields):
    value = {"apiVersion": version, "kind": kind, "metadata": {"name": name}}
    if ns:
        value["metadata"]["namespace"] = ns
    value.update(fields)
    objects.append(value)
    return value


# The proof Job runs in the fixed client namespace whose pod-certificate
# identity install-backend.sh made a bootstrap owner and router client.
client_namespace = "external-substrate-e2e"
add("rbac.authorization.k8s.io/v1", "Role", "native-core-proof", namespace, rules=[
    {"apiGroups": ["core.orka.ai"], "resources": ["agents", "tasks"], "verbs": ["create", "get"]},
    {"apiGroups": ["core.orka.ai"], "resources": ["runtimepools"], "verbs": ["get", "list"]},
    {"apiGroups": ["workspace.orka.ai"], "resources": ["executionworkspaceclasses"], "verbs": ["create", "get", "configure", "use"]},
    {"apiGroups": ["workspace.orka.ai"], "resources": ["executionworkspaces"], "verbs": ["get", "list"]},
    {"apiGroups": ["substrate.workspace.orka.ai"], "resources": ["substrateworkspaceprofiles"], "verbs": ["create", "get"]},
    {"apiGroups": ["ate.dev"], "resources": ["workerpools"], "verbs": ["get", "list"]},
    {"apiGroups": [""], "resources": ["pods", "secrets"], "verbs": ["get", "list"]}])
add("rbac.authorization.k8s.io/v1", "RoleBinding", "native-core-proof", namespace,
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "native-core-proof"},
    subjects=[{"kind": "ServiceAccount", "name": "native-core-proof", "namespace": client_namespace}])
# The fixed namespace also names the manager's cluster-scoped RBAC, so the
# proof's cluster-scoped objects need their own names.
proof_cluster_rbac = namespace + "-proof"
add("rbac.authorization.k8s.io/v1", "ClusterRole", proof_cluster_rbac, rules=[
    {"apiGroups": ["workspace.orka.ai"], "resources": ["executionworkspaceproviders"], "resourceNames": ["substrate"], "verbs": ["get"]}])
add("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", proof_cluster_rbac,
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": proof_cluster_rbac},
    subjects=[{"kind": "ServiceAccount", "name": "native-core-proof", "namespace": client_namespace}])
# Operator source pool: it runs no workers; the provider sizes each private copy.
add("ate.dev/v1", "WorkerPool", "native-core", namespace, spec={
    "replicas": 0, "workerImage": worker_image,
    "sandboxClasses": [{"name": "gvisor"}]})["metadata"]["labels"] = {"orka.workspace.e2e/pool": namespace}

client_volume = {"name": "native-client", "projected": {"sources": [{"podCertificate": {
    "signerName": "podidentity.podcert.ate.dev/identity", "keyType": "ECDSAP256", "credentialBundlePath": "credential-bundle.pem"}}]}}
server_volume = {"name": "native-server", "projected": {"sources": [{"clusterTrustBundle": {
    "signerName": "servicedns.podcert.ate.dev/identity", "labelSelector": {"matchLabels": {"podcert.ate.dev/canarying": "live"}}, "path": "trust-bundle.pem"}}]}}
mounts = [{"name": "native-client", "mountPath": "/native/client", "readOnly": True},
          {"name": "native-server", "mountPath": "/native/server", "readOnly": True}]

# This Job uses the Kubernetes public APIs and native read-only observations.
# It never invokes the provider lifecycle/reconciler or writes workspace status.
add("batch/v1", "Job", namespace, client_namespace, spec={"backoffLimit": 0, "activeDeadlineSeconds": 900,
    "template": {"metadata": {"labels": {"orka.workspace.e2e/proof": "native-core"}}, "spec": {
        "serviceAccountName": "native-core-proof", "restartPolicy": "Never",
        "securityContext": {"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532},
        "containers": [{"name": "proof", "image": proof_image, "imagePullPolicy": "IfNotPresent",
                        "env": [{"name": "POD_NAMESPACE", "value": namespace}, {"name": "RUNTIME_IMAGE", "value": runtime_image}],
                        "volumeMounts": mounts}], "volumes": [client_volume, server_volume]}}})

provider_base = pathlib.Path(__file__).resolve().parents[3] / "providers/substrate/config"
# Read release manifests through kubectl rather than duplicating provider RBAC.
provider_output = subprocess.check_output([
    "kubectl", "create", "--dry-run=client", "-f", str(provider_base / "deployment.yaml"),
    "-f", str(provider_base / "rbac.yaml"), "-f", str(provider_base / "registration.yaml"), "-o", "json"], text=True)
provider_objects = []
decoder = json.JSONDecoder()
while provider_output.strip():
    value, end = decoder.raw_decode(provider_output.lstrip())
    provider_output = provider_output.lstrip()[end:]
    provider_objects.extend(value["items"] if value.get("kind") == "List" else [value])
for obj in provider_objects:
    if obj["kind"] == "SubstrateWorkspaceProfile":
        continue
    if obj["kind"] == "Deployment":
        # The shipped transport is kept: a projected ServiceAccount token for
        # ate-api (least-privilege access policies) and a pod certificate for
        # atenet-router (ingress allowlist).
        spec = obj["spec"]["template"]["spec"]
        spec["securityContext"].update({"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532})
        container = spec["containers"][0]
        container["image"], container["imagePullPolicy"] = provider_image, "IfNotPresent"
        # install-backend.sh installed the gateway with the k8s.io credential
        # provider and run.sh grants this run's Atespace its own namespace, so
        # the provider may attest credential injection. Core keeps its provider
        # Secret in the workload namespace; no extra namespace is listed.
        container["args"].append("--egress-credential-provider=k8s.io")
        # This kindctl cluster's Agent Substrate install serves only this run,
        # which provider ADR 0008 requires before any profile shares workers.
        # The gateway TTL flag keeps its default, the gateway's own default.
        if os.environ.get("SUBSTRATE_DEDICATED_INSTALL") == "1":
            container["args"].append("--substrate-dedicated-install")
    objects.append(obj)

# Applying a List silently replaces an earlier object with a later same-identity
# one; fail rendering instead of overwriting another component's permissions.
identities = set()
for obj in objects:
    identity = (obj["apiVersion"].split("/")[0] if "/" in obj["apiVersion"] else "", obj["kind"],
                obj["metadata"].get("namespace", ""), obj["metadata"]["name"])
    if identity in identities:
        sys.exit("duplicate rendered object %s" % "/".join(part for part in identity if part))
    identities.add(identity)

json.dump({"apiVersion": "v1", "kind": "List", "items": objects}, sys.stdout, indent=2)
