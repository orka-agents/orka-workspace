#!/usr/bin/env python3
"""Prove the stock controller startup gate against real, pruned API objects."""

import argparse
import copy
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[3]
TAG = "external-workspace"
ROUTING_LABEL = "workspace.orka.ai/controller-name"
LEGACY_CONTROLLER = "runtime-pool.acp.workspace.orka.ai"
PROOF_LABEL = "external-workspace-upgrade-proof"
DEPRECATED_FLAGS = ("--enable-fake-workspace-provider", "--agent-sandbox-enabled", "--substrate-enabled")


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-image", required=True, help="already loaded final stock core image")
    parser.add_argument("--core-source", required=True, type=pathlib.Path, help="frozen source used for that image")
    parser.add_argument("--old-crd", required=True, type=pathlib.Path, help="saved pre-pruning RuntimePool CRD")
    parser.add_argument("--build-proof", type=pathlib.Path, help="public build provenance for the supplied image")
    parser.add_argument("--artifact-dir", type=pathlib.Path)
    parser.add_argument("--kindctl", default=os.environ.get("KINDCTL_BIN") or "kindctl",
                        help="kindctl executable; defaults to KINDCTL_BIN or kindctl on PATH")
    parser.add_argument("--fixture-deployment", default="external-workspace-core")
    parser.add_argument("--fixture-namespace", default="orka-system")
    parser.add_argument("--timeout", type=int, default=180)
    return parser.parse_args()


class Proof:
    def __init__(self, args):
        self.args = args
        self.out = (args.artifact_dir or pathlib.Path(tempfile.mkdtemp(prefix="orka-workspace-upgrade-e2e."))).resolve()
        if self.out == ROOT or ROOT in self.out.parents:
            raise RuntimeError("proof artifacts must remain outside the source repository")
        self.out.mkdir(parents=True, exist_ok=True)
        self.private = self.out / "private"
        self.private.mkdir(mode=0o700, exist_ok=True)
        self.run_id = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self.namespace = "external-upgrade-" + self.run_id
        self.watch_namespace = "external-upgrade-watch-" + self.run_id
        self.deployment = "external-workspace-upgrade-" + self.run_id
        self.identities = []
        self.resources = []
        self.stages = []

    def k(self, *args, value=None, allow_error=False):
        result = subprocess.run([self.args.kindctl, "kubectl", "--tag", TAG, "--request-timeout=30s", *args], cwd=ROOT,
                                input=json.dumps(value) if value is not None else None,
                                capture_output=True, text=True)
        if result.returncode and not allow_error:
            raise RuntimeError("scoped kubectl failed: " + " ".join(args) + "\n" + result.stderr.strip())
        return result

    def get(self, resource, name=None, namespace=None):
        args = ["get", resource]
        if name:
            args.append(name)
        args.extend(["-n", namespace] if namespace else ["-A"])
        return json.loads(self.k(*args, "-o", "json").stdout)

    def write(self, name, value, private=False):
        path = (self.private if private else self.out) / name
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        if private:
            path.chmod(0o600)
        return path

    def remember(self, obj, resource):
        group, _, version = obj["apiVersion"].rpartition("/")
        if not group:
            version = obj["apiVersion"]
        metadata = obj["metadata"]
        self.identities.append(dict(Group=group, Version=version, Resource=resource,
                                    Namespace=metadata.get("namespace", ""), Name=metadata["name"], UID=metadata["uid"]))
        self.write("synthetic-identities.json", self.identities)
        return obj

    def create(self, obj, resource):
        created = json.loads(self.k("create", "--validate=false", "-f", "-", "-o", "json", value=obj).stdout)
        return self.remember(created, resource)

    def status(self, obj, resource, status):
        return json.loads(self.k("patch", resource, obj["metadata"]["name"], "-n", obj["metadata"]["namespace"],
                                 "--subresource=status", "--type=merge", "-p", json.dumps({"status": status}), "-o", "json").stdout)

    @staticmethod
    def stable(obj):
        metadata = obj["metadata"]
        return {"metadata": {key: metadata.get(key) for key in ("name", "namespace", "uid", "generation", "resourceVersion", "labels", "annotations", "ownerReferences", "finalizers")},
                "spec": obj.get("spec", {}), "status": obj.get("status", {})}

    def snapshot(self, name):
        values = {}
        for resource, object_name in self.resources:
            obj = self.get(resource, object_name, self.namespace)
            values[resource + "/" + object_name] = self.stable(obj)
            if obj["metadata"].get("finalizers") or obj["metadata"].get("ownerReferences"):
                raise RuntimeError("synthetic legacy object was adopted or finalized")
        inventory = {}
        for namespace in (self.namespace, self.watch_namespace):
            for resource in ("pods", "secrets", "networkpolicies", "persistentvolumeclaims", "deployments", "services"):
                items = self.get(resource, namespace=namespace)["items"]
                inventory[namespace + "/" + resource] = sorted((item["metadata"]["name"], item["metadata"]["uid"], item["metadata"]["resourceVersion"]) for item in items)
                if items:
                    raise RuntimeError("unexpected compute, credentials, network policy or infrastructure in proof namespace")
        values["inventory"] = inventory
        values["sharedBoundaries"] = self.boundaries()
        self.write(name, values)
        return values

    def boundaries(self):
        # Only metadata is retained for Secrets. The existing fixture's actual
        # credential values remain in Kubernetes and are never logged or saved.
        result = {}
        for resource in ("services", "secrets", "networkpolicies"):
            items = self.get(resource, namespace=self.args.fixture_namespace)["items"]
            result[resource] = [self.stable(item) if resource != "secrets" else {"metadata": {key: item["metadata"].get(key) for key in
                                ("name", "namespace", "uid", "resourceVersion", "ownerReferences", "finalizers")}} for item in items]
            result[resource].sort(key=lambda item: item["metadata"]["name"])
        for resource in ("validatingwebhookconfigurations", "mutatingwebhookconfigurations", "validatingadmissionpolicies", "validatingadmissionpolicybindings"):
            items = self.get(resource)["items"]
            result[resource] = [{"metadata": item["metadata"], "spec": item.get("spec"), "webhooks": item.get("webhooks")} for item in items]
            result[resource].sort(key=lambda item: item["metadata"]["name"])
        providers = self.get("executionworkspaceproviders")["items"]
        result["providers"] = []
        for provider in providers:
            metadata = provider["metadata"]
            status = copy.deepcopy(provider.get("status", {}))
            status.pop("lastHeartbeat", None)
            status["conditions"] = sorted(status.get("conditions", []), key=lambda condition: condition["type"])
            reserved_metadata = {key: metadata.get(key) for key in ("name", "uid", "generation", "finalizers", "ownerReferences")}
            for key in ("labels", "annotations"):
                reserved_metadata[key] = {name: value for name, value in metadata.get(key, {}).items() if name.startswith("workspace.orka.ai/")}
            result["providers"].append({"metadata": reserved_metadata, "spec": provider["spec"], "statusExceptLastHeartbeat": status})
        result["providers"].sort(key=lambda provider: provider["metadata"]["name"])
        return result

    def check_rejected(self, stage, baseline):
        deadline = time.monotonic() + self.args.timeout
        names = [self.namespace + "/" + name for _, name in self.resources]
        accepted = None
        while time.monotonic() < deadline:
            pods = self.get("pods", namespace=self.args.fixture_namespace)["items"]
            pods = [pod for pod in pods if pod["metadata"].get("labels", {}).get(PROOF_LABEL) == self.run_id
                    and pod["metadata"].get("annotations", {}).get("external-upgrade-proof-stage") == stage
                    and not pod["metadata"].get("deletionTimestamp")]
            for pod in pods:
                current = self.k("logs", pod["metadata"]["name"], "-n", self.args.fixture_namespace, allow_error=True)
                previous = self.k("logs", pod["metadata"]["name"], "-n", self.args.fixture_namespace, "--previous", allow_error=True)
                logs = current.stdout + previous.stdout
                self.write(stage + "-pod.json", self.stable(pod))
                path = self.private / (stage + "-controller.log")
                path.write_text(logs)
                path.chmod(0o600)
                statuses = pod.get("status", {}).get("containerStatuses", [])
                terminations = [state for entry in statuses for state in (entry.get("state", {}).get("terminated"), entry.get("lastState", {}).get("terminated")) if state]
                if any(state.get("exitCode") == 1 for state in terminations) and "workspace upgrade preflight rejected startup" in logs and all(name in logs for name in names):
                    accepted = {"pod": pod["metadata"]["name"], "podUID": pod["metadata"]["uid"], "imageID": statuses[0].get("imageID"), "exitCode": 1, "namedBlockers": names}
                    break
            if accepted:
                break
            time.sleep(1)
        if not accepted:
            raise RuntimeError("stock startup did not reject and name every synthetic legacy object; inspect private controller log")
        after = self.snapshot(stage + "-after.json")
        if after != baseline:
            raise RuntimeError("blocked controller changed legacy spec/status/metadata or proof namespace infrastructure")
        accepted.update(stage=stage, unchanged=True, createdComputeSecretsOrPolicies=False)
        self.stages.append(accepted)
        self.write("upgrade-proof-progress.json", self.stages)

    def stage_deployment(self, stage):
        obj = self.get("deployments", self.deployment, self.args.fixture_namespace)
        patch = [{"op": "test", "path": "/metadata/uid", "value": obj["metadata"]["uid"]},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": obj["metadata"]["resourceVersion"]},
                 {"op": "add", "path": "/spec/template/metadata/annotations", "value": {"external-upgrade-proof-stage": stage}}]
        self.k("patch", "deployment", self.deployment, "-n", self.args.fixture_namespace, "--type=json", "-p", json.dumps(patch))

    def cleanup(self, objects):
        path = self.write("cleanup-request.json", objects)
        result = subprocess.run([self.args.kindctl, "exec", "--tag", TAG, "--", "go", "-C", str(ROOT), "run",
                                 "./hack/external-workspace-e2e/upgrade-proof/cleanup", "--input", str(path)], cwd=ROOT, capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError("UID/resource-version guarded synthetic cleanup failed: " + result.stderr.strip())

    def run(self):
        new_crd = self.args.core_source / "config/crd/bases/core.orka.ai_runtimepools.yaml"
        if not new_crd.is_file() or not self.args.old_crd.is_file():
            raise RuntimeError("both frozen new and saved old RuntimePool CRDs are required")
        for name, path in (("runtimepools-old.yaml", self.args.old_crd), ("runtimepools-new.yaml", new_crd)):
            content = path.read_bytes()
            (self.out / name).write_bytes(content)
            parsed = json.loads(self.k("create", "--dry-run=client", "--validate=false", "-f", str(path), "-o", "json").stdout)
            if parsed.get("kind") != "CustomResourceDefinition" or parsed["metadata"]["name"] != "runtimepools.core.orka.ai":
                raise RuntimeError("CRD target is not runtimepools.core.orka.ai")
        node = self.get("nodes")["items"][0]
        fixture = self.get("deployment", self.args.fixture_deployment, self.args.fixture_namespace)
        if self.get("runtimepools")["items"]:
            raise RuntimeError("temporary old RuntimePool schema requires no existing RuntimePools in the scoped cluster")
        self.write("fixture-deployment-before.json", self.stable(fixture))
        self.write("runtimepool-crd-before.json", self.get("crd", "runtimepools.core.orka.ai"))
        for namespace in (self.namespace, self.watch_namespace):
            namespace_labels = {PROOF_LABEL: self.run_id}
            if namespace == self.watch_namespace:
                namespace_labels["orka.ai/controller-mode"] = "harness-v2"
            self.create({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace, "labels": namespace_labels}}, "namespaces")
        # The old schema is restored only in this isolated local cluster, so the
        # API server stores backend settings before the new schema can prune them.
        self.k("apply", "--server-side", "--force-conflicts", "-f", str(self.out / "runtimepools-old.yaml"))
        self.k("wait", "--for=condition=Established", "crd/runtimepools.core.orka.ai", "--timeout=60s")
        digest = "sha256:" + "1" * 64
        profile = {"protocolVersion": "orka.harness.v2", "digest": digest, "digestSchemaVersion": "1", "acpProfile": "acp.v1",
                   "adapterDigests": {"codex-acp": digest}, "providerKind": "codex", "model": "upgrade-proof", "workspaceIntent": "read",
                   "proxyCredentialRole": "provider-inference", "proxyCredentialScope": "model:upgrade-proof", "resourceClass": "standard"}
        for field in ("agentConfigurationDigest", "toolPolicyDigest", "approvalPolicyDigest", "mcpConfigurationDigest"):
            profile[field] = digest
        pool = self.create({"apiVersion": "core.orka.ai/v1alpha1", "kind": "RuntimePool", "metadata": {"name": "legacy-pool", "namespace": self.namespace, "labels": {PROOF_LABEL: self.run_id}},
                            "spec": {"trustDomain": {"namespace": self.namespace, "identity": self.namespace + "/legacy"}, "desiredReplicas": 1,
                                     "capacity": {"maxResidentSessions": 1, "maxRunningPrompts": 1},
                                     "runtime": {"image": "fixture.invalid/never-run@" + digest, "profile": profile},
                                     "executionWorkspace": {"provider": "agent-sandbox", "bindingDigest": digest,
                                                            "agentSandbox": {"suspendMode": "DataOnly", "suspendVolume": {"capacity": "1Gi"}}}}}, "runtimepools")
        if "agentSandbox" not in pool["spec"]["executionWorkspace"]:
            raise RuntimeError("old-shaped backend settings were not persisted by the API server")
        self.resources.append(("runtimepools", "legacy-pool"))
        for state in ("Ready", "Suspended", "Failed", "Deleted"):
            name = "legacy-" + state.lower()
            workspace = self.create({"apiVersion": "workspace.orka.ai/v1alpha1", "kind": "ExecutionWorkspace",
                                     "metadata": {"name": name, "namespace": self.namespace, "labels": {PROOF_LABEL: self.run_id, ROUTING_LABEL: LEGACY_CONTROLLER}},
                                     "spec": {"classBinding": {"name": "synthetic-legacy-class", "uid": "synthetic-class-uid", "generation": 1, "profileHash": digest},
                                              "providerBinding": {"name": "synthetic-legacy-provider", "uid": "synthetic-provider-uid", "generation": 1},
                                              "mode": "Interactive", "slot": "default", "desiredState": "Suspended" if state == "Suspended" else "Deleted" if state == "Deleted" else "Ready",
                                              "lifecycle": {"defaultOnDetach": "Delete", "allowedOnDetach": ["Delete"], "detachTimeout": "1m",
                                                            "deletionPolicy": {"persistentVolumes": "Delete", "checkpoints": "Delete", "providerResources": "Delete"}}}}, "executionworkspaces")
            self.status(workspace, "executionworkspaces", {"state": state, "observedGeneration": workspace["metadata"]["generation"], "externalID": "synthetic-unallocated-" + state.lower()})
            self.resources.append(("executionworkspaces", name))
        initial = self.snapshot("old-schema-before.json")
        deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": self.deployment, "namespace": self.args.fixture_namespace, "labels": {PROOF_LABEL: self.run_id}}, "spec": copy.deepcopy(fixture["spec"])}
        labels = {"app": self.deployment, PROOF_LABEL: self.run_id}
        deployment["spec"].update(replicas=1, selector={"matchLabels": labels}, strategy={"type": "Recreate"})
        deployment["spec"]["template"]["metadata"] = {"labels": labels, "annotations": {"external-upgrade-proof-stage": "old-schema"}}
        manager = next(container for container in deployment["spec"]["template"]["spec"]["containers"] if container["name"] == "manager")
        manager["image"], manager["imagePullPolicy"] = self.args.core_image, "Never"
        manager["args"] = [argument for argument in manager.get("args", []) if argument.split("=", 1)[0] not in DEPRECATED_FLAGS + ("--leader-elect", "--watch-namespace")]
        # Stock main requires leader election. Its namespace is the watch
        # namespace, so this manager cannot contend for the shared core Lease.
        manager["args"] += ["--leader-elect=true", "--watch-namespace=" + self.watch_namespace]
        self.write("isolated-deployment.json", deployment)
        self.create(deployment, "deployments")
        self.check_rejected("old-schema", initial)
        self.k("apply", "--server-side", "--force-conflicts", "-f", str(self.out / "runtimepools-new.yaml"))
        self.k("wait", "--for=condition=Established", "crd/runtimepools.core.orka.ai", "--timeout=60s")
        current_pool = self.get("runtimepools", "legacy-pool", self.namespace)
        current_pool["metadata"].setdefault("annotations", {})["external-upgrade-proof-persisted"] = "true"
        self.k("replace", "--validate=false", "-f", "-", value=current_pool)
        current_pool = self.get("runtimepools", "legacy-pool", self.namespace)
        binding = current_pool["spec"].get("executionWorkspace", {})
        if any(field in binding for field in ("agentSandbox", "substrate")) or any(field in binding for field in ("workload", "workspaceRef", "parametersRef", "parametersBinding")):
            raise RuntimeError("schema persistence did not prune legacy settings while retaining missing generic binding")
        self.write("pruned-runtimepool.json", self.stable(current_pool))
        pruned = self.snapshot("pruned-schema-before.json")
        self.stage_deployment("pruned-schema")
        self.check_rejected("pruned-schema", pruned)
        legacy = [item for item in self.identities if item["Resource"] in ("runtimepools", "executionworkspaces")]
        self.cleanup(legacy)
        self.stage_deployment("retired")
        self.k("rollout", "status", "deployment/" + self.deployment, "-n", self.args.fixture_namespace, "--timeout=" + str(self.args.timeout) + "s")
        for resource, name in self.resources:
            result = self.k("get", resource, name, "-n", self.namespace, allow_error=True)
            if result.returncode == 0 or "NotFound" not in result.stderr:
                raise RuntimeError("legacy exact object lifetime remains after guarded cleanup")
        self.resources = []
        self.snapshot("retired-after.json")
        if self.boundaries() != initial["sharedBoundaries"]:
            raise RuntimeError("stock startup changed shared credentials, networking, Services or admission boundaries")
        pods = [pod for pod in self.get("pods", namespace=self.args.fixture_namespace)["items"] if pod["metadata"].get("labels", {}).get(PROOF_LABEL) == self.run_id and not pod["metadata"].get("deletionTimestamp")]
        if len(pods) != 1 or not any(condition.get("type") == "Ready" and condition.get("status") == "True" for condition in pods[0].get("status", {}).get("conditions", [])):
            raise RuntimeError("retired resources did not permit stock manager startup")
        accepted = pods[0]
        self.write("accepted-pod.json", self.stable(accepted))
        final_fixture = self.get("deployment", self.args.fixture_deployment, self.args.fixture_namespace)
        if final_fixture["metadata"]["uid"] != fixture["metadata"]["uid"] or final_fixture["spec"] != fixture["spec"]:
            raise RuntimeError("upgrade proof changed the shared core fixture Deployment")
        remaining = [item for item in self.identities if item not in legacy]
        self.cleanup(sorted(remaining, key=lambda item: item["Resource"] == "namespaces"))
        report = {"clusterTag": TAG, "node": node["metadata"]["name"], "runID": self.run_id, "coreImage": self.args.core_image,
                  "coreSource": str(self.args.core_source.resolve()), "oldCRDSHA256": hashlib.sha256((self.out / "runtimepools-old.yaml").read_bytes()).hexdigest(),
                  "newCRDSHA256": hashlib.sha256((self.out / "runtimepools-new.yaml").read_bytes()).hexdigest(), "stages": self.stages,
                  "isolatedWatchAndLeaseNamespace": self.watch_namespace,
                  "acceptedPodUID": accepted["metadata"]["uid"], "acceptedImageID": accepted.get("status", {}).get("containerStatuses", [{}])[0].get("imageID"),
                  "schemaPruningDidNotAuthorizeRecreation": True, "allSyntheticLifetimesDeletedWithUIDAndResourceVersion": True,
                  "stockStartupAcceptedAfterLegacyRetirement": True, "sharedFixtureDeploymentUnchanged": True,
                  "servicesAndAdmissionUnchanged": True, "providerBoundariesAndStatusUnchangedExceptLastHeartbeat": True,
                  "passed": True}
        if self.args.build_proof:
            report["buildProof"] = json.loads(self.args.build_proof.read_text())
        self.write("upgrade-proof.json", report)
        print("Upgrade proof PASS: " + str(self.out / "upgrade-proof.json"))


if __name__ == "__main__":
    if os.environ.get("ORKA_UPGRADE_PROOF_RELEASED") != "1":
        if not any(value in ("--help", "-h") for value in sys.argv[1:]):
            sys.exit("Final stock core image/source release is required before cluster mutation")
    options = arguments()
    proof = Proof(options)
    print("Upgrade proof artifacts: " + str(proof.out), flush=True)
    try:
        proof.run()
    except Exception as error:
        proof.write("failure.json", {"passed": False, "error": str(error), "clusterTag": TAG, "syntheticResourcesPreserved": True})
        sys.exit("Upgrade proof failed; cluster evidence preserved at " + str(proof.out) + ": " + str(error))
