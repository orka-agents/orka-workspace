#!/usr/bin/env python3
"""Render an isolated Core/provider Task proof using the existing Core fixture."""
import json
import pathlib
import subprocess
import sys

(namespace, core_image, runtime_image, proof_image, provider_image, worker_image, certificate, core_role,
 proxy_image, collector_image) = sys.argv[1:]
# The proof's in-cluster origins. Agent Substrate runtimes resolve only fully
# qualified cluster-domain names (see the Deployment rewrite below).
proxy_service = "fixture-provider-proxy"
collector_service = "orka-otel-collector"
collector_endpoint = "http://%s.%s.svc.cluster.local:4317" % (collector_service, namespace)
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
        # Agent Substrate's DNS relay answers only allowlisted names, and the
        # sandbox resolver (ndots:5 with the worker Pod's search list) never
        # sends the absolute short "<svc>.<ns>.svc." form kube-dns resolves.
        # Core freezes these origins into the runtime egress allowlist, so
        # they must be cluster-domain FQDNs.
        manager["args"] = [arg.replace(".svc:8080", ".svc.cluster.local:8080")
                           if arg.startswith(("--acp-provider-proxy-base-url=", "--controller-url=")) else arg
                           for arg in manager["args"]]
        # Core and supervisor traces go to the proof's collector, in both
        # phases. Only the generic gRPC endpoint is set, the smallest
        # telemetry configuration: Core fits supervisor telemetry into the 32
        # environment entries Substrate admits and reports the outcome on the
        # RuntimePool's TelemetryReady condition, which the proof requires
        # True. The collector origin is admitted to the runtime egress
        # allowlist.
        manager["args"].extend(["--enable-telemetry", "--runtime-telemetry-collector-egress"])
        manager.setdefault("env", []).append({"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": collector_endpoint})
    if obj["kind"] == "Service" and obj["metadata"]["name"] == proxy_service:
        # Replace the listener-less boundary with the injected-token fixture.
        obj["spec"] = {"selector": {"app": proxy_service}, "ports": [
            {"name": "api", "port": 8080, "targetPort": 8080},
            {"name": "observe", "port": 8081, "targetPort": 8081}]}

# The provider-auth proxy origin. It accepts only Core's provider token, which
# the runtime never holds: the egress gateway must inject it. Port 8081 reports
# counts per Authorization class to the proof; Core admits only port 8080 to
# runtime egress.
restricted = {"runAsNonRoot": True, "runAsUser": 65532, "runAsGroup": 65532, "allowPrivilegeEscalation": False,
              "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
objects.append({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": proxy_service, "namespace": namespace}, "spec": {
    "replicas": 1, "selector": {"matchLabels": {"app": proxy_service}}, "template": {"metadata": {"labels": {"app": proxy_service}}, "spec": {
        "automountServiceAccountToken": False, "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "proxy", "image": proxy_image, "imagePullPolicy": "IfNotPresent",
                        "env": [{"name": "TOKEN_FILE", "value": "/var/run/fixture/token"}],
                        "ports": [{"containerPort": 8080}, {"containerPort": 8081}],
                        "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8081}},
                        "securityContext": restricted,
                        "volumeMounts": [{"name": "token", "mountPath": "/var/run/fixture", "readOnly": True}]}],
        "volumes": [{"name": "token", "secret": {"secretName": "external-substrate-core-fixture", "items": [{"key": "token", "path": "token"}]}}]}}}})
# OTLP collector with the debug exporter; run.sh parses its log for one
# trace that joins Core and supervisor spans.
collector_config = """receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
processors:
  batch:
    timeout: 1s
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
"""
objects.append({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": collector_service, "namespace": namespace},
                "data": {"collector.yaml": collector_config}})
objects.append({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": collector_service, "namespace": namespace}, "spec": {
    "replicas": 1, "selector": {"matchLabels": {"app": collector_service}}, "template": {"metadata": {"labels": {"app": collector_service}}, "spec": {
        "automountServiceAccountToken": False, "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "collector", "image": collector_image, "imagePullPolicy": "IfNotPresent",
                        "args": ["--config=/etc/otelcol/collector.yaml"], "ports": [{"containerPort": 4317}],
                        "securityContext": restricted,
                        "volumeMounts": [{"name": "config", "mountPath": "/etc/otelcol", "readOnly": True}]}],
        "volumes": [{"name": "config", "configMap": {"name": collector_service}}]}}}})
objects.append({"apiVersion": "v1", "kind": "Service", "metadata": {"name": collector_service, "namespace": namespace},
                "spec": {"selector": {"app": collector_service}, "ports": [{"name": "otlp-grpc", "port": 4317, "targetPort": 4317}]}})

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
# Core's API authenticates callers by TokenReview, which Orka's Helm and
# kustomize installs grant outside the generated role (metrics-auth-role).
# core/cleanup.sh archives the proof Session through that API.
role["rules"].append({"apiGroups": ["authentication.k8s.io"], "resources": ["tokenreviews"], "verbs": ["create"]})
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
    {"apiGroups": [""], "resources": ["pods", "secrets"], "verbs": ["get", "list"]},
    # The session phase evicts the exact attested worker Pod (UID precondition).
    {"apiGroups": [""], "resources": ["pods"], "verbs": ["delete"]}])
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
# Two Jobs, run in order by run.sh against the same Core with telemetry: the
# single Task, then the Session across a worker eviction.
for job, phase, deadline in [(namespace, "task", 1200), (namespace + "-session", "session", 1800)]:
    add("batch/v1", "Job", job, client_namespace, spec={"backoffLimit": 0, "activeDeadlineSeconds": deadline,
        "template": {"metadata": {"labels": {"orka.workspace.e2e/proof": "native-core"}}, "spec": {
            "serviceAccountName": "native-core-proof", "restartPolicy": "Never",
            "securityContext": {"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532},
            "containers": [{"name": "proof", "image": proof_image, "imagePullPolicy": "IfNotPresent",
                            "env": [{"name": "POD_NAMESPACE", "value": namespace}, {"name": "RUNTIME_IMAGE", "value": runtime_image},
                                    {"name": "PROOF_PHASE", "value": phase}],
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
