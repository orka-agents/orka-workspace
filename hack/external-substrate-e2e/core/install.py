#!/usr/bin/env python3
"""Render an isolated Core/provider Task proof using the existing Core fixture."""
import json
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
        obj["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"

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


add("v1", "ServiceAccount", "native-core-proof", namespace)
add("rbac.authorization.k8s.io/v1", "Role", "native-core-proof", namespace, rules=[
    {"apiGroups": ["core.orka.ai"], "resources": ["agents", "tasks"], "verbs": ["create", "get"]},
    {"apiGroups": ["core.orka.ai"], "resources": ["runtimepools"], "verbs": ["get", "list"]},
    {"apiGroups": ["workspace.orka.ai"], "resources": ["executionworkspaceclasses"], "verbs": ["create", "get", "configure", "use"]},
    {"apiGroups": ["workspace.orka.ai"], "resources": ["executionworkspaces"], "verbs": ["get", "list"]},
    {"apiGroups": ["substrate.workspace.orka.ai"], "resources": ["substrateworkspaceprofiles"], "verbs": ["create", "get"]},
    {"apiGroups": ["ate.dev"], "resources": ["workerpools"], "verbs": ["get", "list"]},
    {"apiGroups": [""], "resources": ["pods", "secrets"], "verbs": ["get", "list"]},
    {"apiGroups": ["networking.k8s.io"], "resources": ["networkpolicies"], "verbs": ["get", "list"]}])
add("rbac.authorization.k8s.io/v1", "RoleBinding", "native-core-proof", namespace,
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "native-core-proof"},
    subjects=[{"kind": "ServiceAccount", "name": "native-core-proof", "namespace": namespace}])
add("rbac.authorization.k8s.io/v1", "ClusterRole", namespace, rules=[
    {"apiGroups": ["workspace.orka.ai"], "resources": ["executionworkspaceproviders"], "resourceNames": ["substrate"], "verbs": ["get"]}])
add("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", namespace,
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": namespace},
    subjects=[{"kind": "ServiceAccount", "name": "native-core-proof", "namespace": namespace}])
add("ate.dev/v1alpha1", "WorkerPool", "native-core", namespace, spec={
    "replicas": 1, "workerImage": worker_image,
    "template": {"resources": {"requests": {"cpu": "250m", "memory": "512Mi"},
                               "limits": {"cpu": "4", "memory": "8Gi"}}}})["metadata"]["labels"] = {"orka.workspace.e2e/pool": namespace}

client_volume = {"name": "native-client", "projected": {"sources": [{"podCertificate": {
    "signerName": "podidentity.podcert.ate.dev/identity", "keyType": "ECDSAP256", "credentialBundlePath": "credential-bundle.pem"}}]}}
server_volume = {"name": "native-server", "projected": {"sources": [{"clusterTrustBundle": {
    "signerName": "servicedns.podcert.ate.dev/identity", "labelSelector": {"matchLabels": {"podcert.ate.dev/canarying": "live"}}, "path": "trust-bundle.pem"}}]}}
mounts = [{"name": "native-client", "mountPath": "/native/client", "readOnly": True},
          {"name": "native-server", "mountPath": "/native/server", "readOnly": True}]

# This Job uses the Kubernetes public APIs and native read-only observations.
# It never invokes the provider lifecycle/reconciler or writes workspace status.
add("batch/v1", "Job", "native-core-proof", namespace, spec={"backoffLimit": 0, "activeDeadlineSeconds": 900,
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
        spec = obj["spec"]["template"]["spec"]
        spec["securityContext"].update({"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532})
        spec["volumes"] = [client_volume, server_volume]
        container = spec["containers"][0]
        container["image"], container["imagePullPolicy"] = provider_image, "IfNotPresent"
        container["volumeMounts"] = mounts
        container["args"] = [value for value in container["args"] if not value.startswith(("--native-ca-file=", "--native-token-file=", "--native-direct-egress="))]
        container["args"] += ["--native-ca-file=/native/server/trust-bundle.pem",
                              "--native-cert-file=/native/client/credential-bundle.pem",
                              "--native-key-file=/native/client/credential-bundle.pem",
                              "--native-direct-egress=true"]
    objects.append(obj)

json.dump({"apiVersion": "v1", "kind": "List", "items": objects}, sys.stdout, indent=2)
