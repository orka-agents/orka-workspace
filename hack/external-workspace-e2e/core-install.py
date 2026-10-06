#!/usr/bin/env python3
"""Render an isolated local proof deployment without changing release files."""
import base64
import json
import pathlib
import sys

core_image, runtime_image, certificate = sys.argv[1:]
namespace = "orka-system"
objects = []


def add(version, kind, name, spec=None, ns=None, **fields):
    obj = {"apiVersion": version, "kind": kind, "metadata": {"name": name}}
    if ns:
        obj["metadata"]["namespace"] = ns
    if spec is not None:
        obj["spec"] = spec
    obj.update(fields)
    objects.append(obj)
    return obj


add("v1", "Namespace", namespace)["metadata"]["labels"] = {
    "orka.ai/controller-mode": "harness-v2"
}
add("v1", "Namespace", "orka-runtimes")
add("v1", "ServiceAccount", "orka-controller-manager", ns=namespace)
add("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "external-workspace-core",
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "external-workspace-core"},
    subjects=[{"kind": "ServiceAccount", "name": "orka-controller-manager", "namespace": namespace}])
add("rbac.authorization.k8s.io/v1", "ClusterRole", "external-workspace-core-fixture",
    rules=[{"apiGroups": ["fake.workspace.orka.ai"], "resources": ["fakeproviderconfigs", "fakepoolparameters"], "verbs": ["get", "list", "watch"]},
           {"apiGroups": ["apiextensions.k8s.io"], "resources": ["customresourcedefinitions"], "verbs": ["get"]},
           {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]}])
add("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "external-workspace-core-fixture",
    roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "external-workspace-core-fixture"},
    subjects=[{"kind": "ServiceAccount", "name": "orka-controller-manager", "namespace": namespace}])
labels = {"app": "external-workspace-core", "orka.ai/network-role": "controller"}
add("v1", "Service", "external-workspace-core", ns=namespace,
    spec={"selector": {"app": "external-workspace-core"}, "ports": [
        {"name": "api", "port": 8080, "targetPort": 8080},
        {"name": "webhook", "port": 443, "targetPort": 9443}]})
args = ["--leader-elect", "--health-probe-bind-address=:8081", "--metrics-bind-address=0",
        "--controller-mode=harness-v2", "--watch-namespace=orka-system", "--enforce-namespace-isolation=true",
        "--execution-mode-controller-usernames=system:serviceaccount:orka-system:orka-controller-manager",
        "--store-backend=sqlite", "--store-path=/data/orka.db",
        "--controller-url=http://external-workspace-core.orka-system.svc:8080",
        "--agent-execution-snapshot-key-file=/var/run/orka/fixture/key",
        "--webhook-cert-path=/var/run/orka/tls", "--workspace-class-use-admission-enabled=true",
        "--task-provenance-admission-enabled=true", "--enable-workspace-provider-api=true",
        "--acp-workspace-dispatch-enabled=true",
        "--acp-runtime-namespace=orka-runtimes", "--acp-codex-runtime-image=" + runtime_image,
        "--acp-provider-proxy-base-url=http://fixture-provider-proxy.orka-system.svc:8080",
        "--acp-provider-proxy-namespace=orka-system", "--acp-provider-proxy-token-file=/var/run/orka/fixture/token",
        "--gateway-enabled=false"]
add("apps/v1", "Deployment", "external-workspace-core", ns=namespace, spec={
    "replicas": 1, "selector": {"matchLabels": {"app": "external-workspace-core"}},
    "strategy": {"type": "Recreate"}, "template": {"metadata": {"labels": labels}, "spec": {
        "serviceAccountName": "orka-controller-manager",
        "securityContext": {"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532},
        "containers": [{"name": "manager", "image": core_image, "imagePullPolicy": "Never", "args": args,
                        "env": [{"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}}],
                        "ports": [{"containerPort": 8080}, {"containerPort": 9443}, {"containerPort": 8081}],
                        "readinessProbe": {"httpGet": {"path": "/readyz", "port": 8081}},
                        "volumeMounts": [{"name": "data", "mountPath": "/data"},
                                         {"name": "fixture", "mountPath": "/var/run/orka/fixture", "readOnly": True},
                                         {"name": "tls", "mountPath": "/var/run/orka/tls", "readOnly": True}]}],
        "volumes": [{"name": "data", "emptyDir": {}},
                    {"name": "fixture", "secret": {"secretName": "external-workspace-core-fixture"}},
                    {"name": "tls", "secret": {"secretName": "external-workspace-core-tls"}}]}}})

# The deterministic ACP agent does not make provider requests. A configured
# boundary is still required by production runtime admission. No listener is
# installed, so any accidental provider request fails rather than escapes.
add("v1", "Service", "fixture-provider-proxy", ns=namespace,
    spec={"selector": {"external-workspace-proof-no-provider": "true"}, "ports": [{"port": 8080}]})

ca = base64.b64encode(pathlib.Path(certificate).read_bytes()).decode()
webhooks = []
for name, path, group, resource, operations in [
    ("taskclass", "/validate-core-orka-ai-v1alpha1-task-workspace-class-use", "core.orka.ai", "tasks", ["CREATE", "UPDATE"]),
    ("taskprovenance", "/validate-core-orka-ai-v1alpha1-task-provenance", "core.orka.ai", "tasks", ["CREATE", "UPDATE"]),
    ("taskauthority", "/validate-core-orka-ai-v1alpha1-task-execution-authority", "core.orka.ai", "tasks", ["CREATE", "UPDATE"]),
    ("agentcontract", "/validate-core-orka-ai-v1alpha1-agent-contract", "core.orka.ai", "agents", ["CREATE", "UPDATE"]),
    ("agentruntimecontract", "/validate-core-orka-ai-v1alpha1-agentruntime-contract", "core.orka.ai", "agentruntimes", ["CREATE", "UPDATE"]),
    ("attachmentsecret", "/validate-v1-secret-workspace-attachment", "", "secrets", ["CREATE", "UPDATE", "DELETE"]),
]:
    hook = {"name": name + ".external-proof.orka.ai", "admissionReviewVersions": ["v1"], "sideEffects": "None", "failurePolicy": "Fail",
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
            "clientConfig": {"caBundle": ca, "service": {"name": "external-workspace-core", "namespace": namespace, "port": 443, "path": path}},
            "rules": [{"operations": operations, "apiGroups": [group], "apiVersions": ["v1" if not group else "v1alpha1"],
                       "resources": [resource, resource + "/status"] if name in ("taskprovenance", "taskauthority") else [resource], "scope": "Namespaced"}]}
    if name == "attachmentsecret":
        hook["objectSelector"] = {"matchExpressions": [{"key": "workspace.orka.ai/attachment-for", "operator": "Exists"}]}
    webhooks.append(hook)
add("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration", "external-workspace-core", webhooks=webhooks)
json.dump({"apiVersion": "v1", "kind": "List", "items": objects}, sys.stdout, indent=2)
