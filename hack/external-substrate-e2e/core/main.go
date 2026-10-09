// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Proves a real Core Task through a deployed standalone native provider.
// This client never calls the provider lifecycle or writes workload status.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type publicChallenge struct {
	Schema    string `json:"schema"`
	Nonce     string `json:"nonce"`
	BootNonce string `json:"bootNonce"`
	PublicKey string `json:"publicKey"`
	Actor     struct {
		Atespace string `json:"atespace"`
		Name     string `json:"name"`
		UID      string `json:"uid"`
	} `json:"actor"`
}

type proof struct {
	c             client.Client
	native        pb.ControlClient
	namespace     string
	image         string
	checks        []string
	challengeRead bool
}

func (p *proof) object(kind, name string, spec map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "core.orka.ai/v1alpha1", "kind": kind,
		"metadata": map[string]any{"name": name, "namespace": p.namespace}}}
	if spec != nil {
		u.Object["spec"] = spec
	}
	return u
}

func str(u *unstructured.Unstructured, path ...string) string {
	s, _, _ := unstructured.NestedString(u.Object, path...)
	return s
}

func wait(ctx context.Context, check func() (bool, error)) error {
	for {
		if done, err := check(); done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (p *proof) pass(text string) {
	p.checks = append(p.checks, text)
	fmt.Println("PASS", text)
}

func (p *proof) observeStartup(ctx context.Context, w *api.ExecutionWorkspace) (*pb.Actor, *corev1.Pod, *unstructured.Unstructured, []string, error) {
	if w.Spec.CoreAdmission == nil || w.Spec.CoreAdmission.ClassBinding != w.Spec.ClassBinding || w.Spec.CoreAdmission.ProviderBinding != w.Spec.ProviderBinding || w.Spec.Workload == nil || w.Spec.Workload.Runtime == nil || w.Status.Allocation.Sequence != w.Spec.Workload.Sequence || w.Status.Allocation.Identity.RequestRevision != w.Spec.Workload.Revision {
		return nil, nil, nil, nil, fmt.Errorf("actual Core admission and provider acknowledgement differ from the immutable request")
	}
	startup := w.Status.Allocation.Startup
	process := startup.Process
	if process == nil || startup.Pod != nil || process.Worker.UID == "" {
		return nil, nil, nil, nil, fmt.Errorf("provider omitted separate exact native process and worker identity")
	}
	endpoint, err := url.Parse(startup.Endpoint)
	if err != nil || endpoint.Scheme != "http" || (endpoint.Port() != "" && endpoint.Port() != "80") {
		return nil, nil, nil, nil, fmt.Errorf("native startup endpoint did not preserve supervisor port 80")
	}
	ref := &pb.ObjectRef{Atespace: process.Namespace, Name: process.Name}
	actor, err := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: ref})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if actor.GetMetadata().GetUid() != process.UID || actor.GetMetadata().GetVersion() != process.Version || assignment.GetWorkerPodUid() != string(process.Worker.UID) || assignment.GetWorkerPod() != process.Worker.Name || assignment.GetWorkerNamespace() != process.Worker.Namespace {
		return nil, nil, nil, nil, fmt.Errorf("startup native UID/version/worker fence differs")
	}
	pod := &corev1.Pod{}
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: process.Worker.Namespace, Name: process.Worker.Name}, pod); err != nil {
		return nil, nil, nil, nil, err
	}
	if pod.UID != process.Worker.UID || pod.DeletionTimestamp != nil {
		return nil, nil, nil, nil, fmt.Errorf("observed worker Pod was replaced")
	}
	pool := &unstructured.Unstructured{}
	pool.SetAPIVersion("ate.dev/v1alpha1")
	pool.SetKind("WorkerPool")
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: assignment.GetWorkerPool()}, pool); err != nil {
		return nil, nil, nil, nil, err
	}
	replicas, _, err := unstructured.NestedInt64(pool.Object, "spec", "replicas")
	if err != nil || replicas != 1 || pool.GetName() == "native-core" {
		return nil, nil, nil, nil, fmt.Errorf("native worker was not a private single-capacity pool")
	}
	for key, value := range map[string]string{"substrate.workspace.orka.ai/allocation-id": w.Status.Allocation.Identity.AllocationID, "substrate.workspace.orka.ai/instance-id": w.Status.Allocation.Identity.InstanceID} {
		if pool.GetLabels()[key] != value || pod.Labels[key] != value {
			return nil, nil, nil, nil, fmt.Errorf("private native worker birth labels differ")
		}
	}
	var policies networkingv1.NetworkPolicyList
	if err := p.c.List(ctx, &policies, client.InNamespace(pod.Namespace)); err != nil {
		return nil, nil, nil, nil, err
	}
	selected := []string{}
	preBirth := false
	for _, policy := range policies.Items {
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err == nil && selector.Matches(labels.Set(pod.Labels)) {
			selected = append(selected, policy.Name)
			if policy.Labels["substrate.workspace.orka.ai/workspace-uid"] == string(w.UID) && !policy.CreationTimestamp.After(pod.CreationTimestamp.Time) {
				preBirth = true
			}
		}
	}
	if !preBirth {
		return nil, nil, nil, nil, fmt.Errorf("provider egress policy did not exist before worker Pod birth")
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, startup.Endpoint+"/v2/credential-bootstrap", nil)
	response, err := (&http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}).Do(request)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer response.Body.Close()
	var challenge publicChallenge
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
		return nil, nil, nil, nil, fmt.Errorf("native public startup challenge returned %d", response.StatusCode)
	}
	// Core closes this one-time endpoint after sealed credential delivery. A
	// 404 is accepted only with the independent authenticated Serving proof
	// required below; the data proof separately captures the public challenge.
	if response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(io.LimitReader(response.Body, 32<<10)).Decode(&challenge); err != nil {
			return nil, nil, nil, nil, err
		}
		canonical, err := json.Marshal(challenge)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		nonce := ""
		for _, container := range w.Spec.Workload.Runtime.Template.Spec.Containers {
			for _, env := range container.Env {
				if env.Name == "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE" {
					nonce = env.Value
				}
			}
		}
		sum := sha256.Sum256(canonical)
		if "sha256:"+hex.EncodeToString(sum[:]) != process.ChallengeSHA256 || challenge.Schema != "orka.harness.v2/sealed-bootstrap/v1" || challenge.Nonce != nonce || challenge.Actor.Atespace != process.Namespace || challenge.Actor.Name != process.Name || challenge.Actor.UID != process.UID {
			return nil, nil, nil, nil, fmt.Errorf("native public challenge did not bind exact process identity and provider hash")
		}
		p.challengeRead = true
	}
	again, err := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: ref})
	if err != nil || again.GetMetadata().GetUid() != actor.GetMetadata().GetUid() || again.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() || !proto.Equal(again.GetStatus().GetWorkerAssignment(), assignment) {
		return nil, nil, nil, nil, fmt.Errorf("native fence changed while observing public challenge: %v", err)
	}
	template, err := p.native.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: actor.ActorTemplate})
	if err != nil || template.GetSnapshotsConfig().GetOnPause() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || template.GetSnapshotsConfig().GetOnCommit() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || template.GetSnapshotsConfig().GetOnResume().GetFromData() != pb.ResumeSource_RESUME_SOURCE_COLD_BOOT {
		return nil, nil, nil, nil, fmt.Errorf("native full-memory gate differs: %v", err)
	}
	return actor, pod, pool, selected, nil
}

func (p *proof) run(ctx context.Context) (map[string]any, error) {
	_, err := p.native.CreateAtespace(ctx, &pb.CreateAtespaceRequest{Atespace: &pb.Atespace{Metadata: &pb.ResourceMetadata{Name: p.namespace}}})
	if err != nil {
		return nil, err
	}
	_, err = p.native.CreateActorTemplate(ctx, &pb.CreateActorTemplateRequest{ActorTemplate: &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: p.namespace, Name: "infrastructure"},
		Containers: []*pb.Container{{Name: "runtime", Image: p.image}}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{"orka.workspace.e2e/pool": p.namespace}},
		SandboxConfig:   &pb.SandboxConfig{SandboxClass: pb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		SnapshotsConfig: &pb.SnapshotsConfig{StorageLocation: "s3://ate-snapshots/" + p.namespace + "/"}}})
	if err != nil {
		return nil, err
	}
	parameters := &profile.SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace, Name: "native-core"},
		Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Namespace: p.namespace, Name: "infrastructure"}}}
	if err := p.c.Create(ctx, parameters); err != nil {
		return nil, err
	}
	class := &api.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace, Name: "native-core"},
		Spec: api.ExecutionWorkspaceClassSpec{ProviderRef: &api.ClusterObjectReference{Name: "substrate"},
			ParametersRef: &api.TypedObjectReference{Group: profile.GroupVersion.Group, Kind: "SubstrateWorkspaceProfile", Name: parameters.Name},
			Mode:          api.ExecutionWorkspaceModeInteractive, RequiredFeatures: []api.ExecutionWorkspaceFeature{api.WorkspaceFeatureACPRuntime},
			AllowedReuseScopes: []api.WorkspaceReuseScope{api.WorkspaceReuseScopeNone}, Lifecycle: api.ExecutionWorkspaceLifecycle{
				DefaultOnDetach: api.WorkspaceOnDetachDelete, AllowedOnDetach: []api.WorkspaceOnDetach{api.WorkspaceOnDetachDelete}, DetachTimeout: metav1.Duration{Duration: time.Minute},
				DeletionPolicy: api.ExecutionWorkspaceDeletionPolicy{ProviderResources: api.WorkspaceDeletionActionDelete, PersistentVolumes: api.WorkspaceDeletionActionDelete, Checkpoints: api.WorkspaceDeletionActionDelete}}}}
	if err := p.c.Create(ctx, class); err != nil {
		return nil, err
	}
	if err := wait(ctx, func() (bool, error) {
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(class), class); err != nil {
			return false, err
		}
		for _, condition := range class.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == class.Generation {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return nil, fmt.Errorf("external native class readiness: %w; conditions=%v", err, class.Status.Conditions)
	}
	p.pass("actual Core resolves the installed external provider and current typed native profile")
	agent := p.object("Agent", "native-core", map[string]any{"runtime": map[string]any{"contractVersion": "orka.harness.v2", "type": "codex", "defaultMaxTurns": int64(1),
		"defaultAllowedTools": []any{"Glob", "Grep", "Read"}, "defaultAllowBash": false}, "model": map[string]any{"name": "gpt-5.4"}})
	if err := p.c.Create(ctx, agent); err != nil {
		return nil, fmt.Errorf("Agent admission: %w", err)
	}
	task := p.object("Task", "native-core", map[string]any{"type": "agent", "agentRef": map[string]any{"name": agent.GetName()},
		"prompt": "Run the deterministic external native workspace fixture.", "timeout": "5m",
		"execution":    map[string]any{"workspace": map[string]any{"classRef": map[string]any{"name": class.Name}, "reusePolicy": "none", "onDetach": "Delete"}},
		"agentRuntime": map[string]any{"maxTurns": int64(1), "allowedTools": []any{"Glob", "Grep", "Read"}, "allowBash": false}})
	if err := p.c.Create(ctx, task); err != nil {
		return nil, fmt.Errorf("Task admission: %w", err)
	}
	fmt.Printf("ACCEPTED_TASK namespace=%s name=%s uid=%s\n", p.namespace, task.GetName(), task.GetUID())
	p.pass("fail-closed class-use, provenance and authority admission accepts the proof ServiceAccount Task")
	var workspace *api.ExecutionWorkspace
	var startup *api.StartupEvidence
	var actor *pb.Actor
	var worker *corev1.Pod
	var pool *unstructured.Unstructured
	var policies []string
	servingInstance, servingProfile, servingBoot := "", "", ""
	if err := wait(ctx, func() (bool, error) {
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
			return false, err
		}
		name := str(task, "status", "executionWorkspace", "workspaceRef", "name")
		if name == "" {
			name = task.GetLabels()["acp.workspace.orka.ai/execution-workspace"]
		}
		if name != "" && startup == nil {
			current := &api.ExecutionWorkspace{}
			if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: name}, current); err == nil {
				workspace = current
				if current.Status.Allocation != nil && current.Status.Allocation.Startup != nil {
					var err error
					actor, worker, pool, policies, err = p.observeStartup(ctx, current)
					if err != nil {
						return false, err
					}
					startup = current.Status.Allocation.Startup.DeepCopy()
				}
			} else if !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		if name := str(task, "status", "execution", "runtimePoolName"); name != "" {
			runtimePool := p.object("RuntimePool", name, nil)
			if err := p.c.Get(ctx, client.ObjectKeyFromObject(runtimePool), runtimePool); err == nil && str(runtimePool, "status", "lifecycle") == "Serving" {
				servingInstance = str(runtimePool, "status", "activeInstance", "runtimeInstanceID")
				servingProfile = str(runtimePool, "status", "activeInstance", "profileDigest")
				servingBoot = str(runtimePool, "status", "activeInstance", "bootID")
			}
		}
		phase := str(task, "status", "phase")
		if phase == "Failed" || phase == "Cancelled" {
			return false, fmt.Errorf("Task became %s: %s %s", phase, str(task, "status", "execution", "reason"), str(task, "status", "execution", "message"))
		}
		return phase == "Succeeded", nil
	}); err != nil {
		return nil, fmt.Errorf("Task execution: %w; last phase=%s state=%s", err, str(task, "status", "phase"), str(task, "status", "execution", "state"))
	}
	identity := map[string]string{}
	for _, field := range []string{"runtimeSessionUID", "runtimeInstanceID", "runtimeSessionSupervisorBootID", "promptID"} {
		identity[field] = str(task, "status", "execution", field)
		if identity[field] == "" {
			return nil, fmt.Errorf("successful Task omitted %s", field)
		}
	}
	available, _, _ := unstructured.NestedBool(task.Object, "status", "resultRef", "available")
	if !available || startup == nil || workspace == nil || servingInstance != identity["runtimeInstanceID"] || servingBoot != identity["runtimeSessionSupervisorBootID"] || servingProfile == "" {
		return nil, fmt.Errorf("successful Task lacks independently observed startup, authenticated Serving or persisted result evidence")
	}
	p.pass("provider process evidence matches exact Actor UID/version/worker; full-memory remains gated")
	if p.challengeRead {
		p.pass("independent public challenge read binds the native process and provider hash before closure")
	}
	p.pass("actual supervisor reaches authenticated Serving on native port 80 and completes an ACP RuntimeSession prompt with persisted result")
	if err := wait(ctx, func() (bool, error) {
		current := &api.ExecutionWorkspace{}
		err := p.c.Get(ctx, client.ObjectKeyFromObject(workspace), current)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if current.UID != workspace.UID {
			return false, fmt.Errorf("terminal workspace was replaced")
		}
		if current.Spec.DesiredState != api.ExecutionWorkspaceDesiredDeleted || current.Status.State != api.ExecutionWorkspaceStateDeleted || current.Status.ObservedGeneration != current.Generation {
			return false, nil
		}
		if current.Status.Allocation != nil && (current.Status.Allocation.Key != workspace.Status.Allocation.Key || current.Status.Allocation.Sequence != workspace.Status.Allocation.Sequence || current.Status.Allocation.Identity != workspace.Status.Allocation.Identity || current.Status.Allocation.State != api.AllocationDeleted) {
			return false, fmt.Errorf("terminal workspace changed the saved allocation fence")
		}
		return sdk.ValidateInteractiveDeletedDisposition(current.Status.Disposition, current.Spec.Lifecycle.DeletionPolicy) == nil, nil
	}); err != nil {
		return nil, fmt.Errorf("Core terminal retirement: %w", err)
	}
	if err := wait(ctx, func() (bool, error) {
		_, nativeErr := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: &pb.ObjectRef{Atespace: startup.Process.Namespace, Name: startup.Process.Name}})
		if nativeErr != nil && status.Code(nativeErr) != codes.NotFound {
			return false, nativeErr
		}
		poolErr := p.c.Get(ctx, client.ObjectKeyFromObject(pool), &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ate.dev/v1alpha1", "kind": "WorkerPool"}})
		workerErr := p.c.Get(ctx, client.ObjectKeyFromObject(worker), &corev1.Pod{})
		return status.Code(nativeErr) == codes.NotFound && apierrors.IsNotFound(poolErr) && apierrors.IsNotFound(workerErr), nil
	}); err != nil {
		return nil, fmt.Errorf("exact native Actor/private pool/worker retirement: %w", err)
	}
	corePoolName, corePoolUID := str(task, "status", "execution", "runtimePoolName"), str(task, "status", "execution", "runtimePoolUID")
	if corePoolName == "" || corePoolUID == "" {
		return nil, fmt.Errorf("successful Task omitted its exact Core RuntimePool fence")
	}
	if err := wait(ctx, func() (bool, error) {
		currentPool := p.object("RuntimePool", corePoolName, nil)
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(currentPool), currentPool); !apierrors.IsNotFound(err) {
			if err != nil {
				return false, err
			}
			if string(currentPool.GetUID()) != corePoolUID {
				return false, fmt.Errorf("terminal Core RuntimePool was replaced")
			}
			return false, nil
		}
		var tokens corev1.SecretList
		if err := p.c.List(ctx, &tokens, client.InNamespace(p.namespace)); err != nil {
			return false, err
		}
		for _, token := range tokens.Items {
			if token.Labels["workspace.orka.ai/attachment-for"] == string(workspace.UID) || token.Labels["orka.ai/runtime-pool-uid"] == corePoolUID {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return nil, fmt.Errorf("Core RuntimePool and credential retirement: %w", err)
	}
	p.pass("terminal Core settlement deletes its exact RuntimePool and credentials, attachment Secrets, native Actor, private WorkerPool and worker Pod")
	return map[string]any{"namespace": p.namespace, "task": task.GetName(), "taskUID": task.GetUID(), "phase": "Succeeded", "workspace": workspace.Name, "workspaceUID": workspace.UID,
		"process": startup.Process, "endpoint": startup.Endpoint, "nativeActorUID": actor.GetMetadata().GetUid(), "privateWorkerPool": map[string]any{"name": pool.GetName(), "uid": pool.GetUID()},
		"runtimeSession": identity, "resultAvailable": available, "coreAdmission": workspace.Spec.CoreAdmission, "providerObservedSequence": workspace.Status.Allocation.Sequence, "workloadRevision": workspace.Spec.Workload.Revision, "runtimeProfileDigest": servingProfile, "selectedNetworkPolicies": policies, "attachmentSecretsRemaining": 0, "coreRuntimePool": map[string]any{"name": corePoolName, "uid": corePoolUID, "deleted": true}, "runtimeCredentialSecretsRemaining": 0, "checks": p.checks,
		"nativeCommit": provider.UpstreamCommit, "runtimeImage": p.image, "publicChallengeObservedBeforeClose": p.challengeRead, "networkPolicyEnforcement": "objects and pre-worker ordering verified; kind default CNI does not prove packet enforcement"}, nil
}

func (p *proof) verifyRejectedRetirement(ctx context.Context) (map[string]any, error) {
	w := &api.ExecutionWorkspace{}
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: os.Getenv("REJECTED_WORKSPACE_NAME")}, w); err != nil {
		return nil, err
	}
	if string(w.UID) != os.Getenv("REJECTED_WORKSPACE_UID") || w.Spec.Workload == nil || w.Spec.Workload.Revision != os.Getenv("REJECTED_WORKLOAD_REVISION") || w.Spec.DesiredState != api.ExecutionWorkspaceDesiredDeleted || w.Status.State != api.ExecutionWorkspaceStateDeleted || w.Status.ObservedGeneration != w.Generation || w.Status.Allocation != nil || w.Status.Disposition == nil || w.Status.Disposition.Compute != api.DispositionDeleted {
		return nil, fmt.Errorf("rejected workload lacks exact unchanged request and completed no-compute retirement")
	}
	report, err := p.verifyRetiredInventory(ctx, string(w.UID), "")
	if err != nil {
		return nil, err
	}
	report["workspace"] = w.Name
	report["workspaceUID"] = w.UID
	report["workloadRevisionUnchanged"] = w.Spec.Workload.Revision
	report["state"] = w.Status.State
	report["disposition"] = w.Status.Disposition
	return report, nil
}

func (p *proof) verifyRetiredTask(ctx context.Context) (map[string]any, error) {
	task := p.object("Task", "native-core", nil)
	if err := p.c.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
		return nil, err
	}
	workspaceUID, poolUID := os.Getenv("RETIRED_WORKSPACE_UID"), os.Getenv("RETIRED_RUNTIME_POOL_UID")
	phase := str(task, "status", "phase")
	if string(task.GetUID()) != os.Getenv("RETIRED_TASK_UID") || task.GetGeneration() != 1 || (phase != "Cancelled" && phase != "Failed") ||
		workspaceUID == "" || task.GetAnnotations()["acp.workspace.orka.ai/execution-workspace-uid"] != workspaceUID ||
		poolUID == "" || str(task, "status", "execution", "runtimePoolUID") != poolUID {
		return nil, fmt.Errorf("retired Task identity, immutable generation or terminal outcome differs")
	}
	w := &api.ExecutionWorkspace{}
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: task.GetLabels()["acp.workspace.orka.ai/execution-workspace"]}, w); !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("retired public workspace is not absent: %v", err)
	}
	pool := p.object("RuntimePool", str(task, "status", "execution", "runtimePoolName"), nil)
	if err := p.c.Get(ctx, client.ObjectKeyFromObject(pool), pool); !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("retired Core RuntimePool is not absent: %v", err)
	}
	report, err := p.verifyRetiredInventory(ctx, workspaceUID, poolUID)
	if err != nil {
		return nil, err
	}
	report["taskUID"] = task.GetUID()
	report["taskPhase"] = phase
	report["taskGenerationUnchanged"] = task.GetGeneration()
	report["workspaceUID"] = workspaceUID
	report["workspaceAbsent"] = true
	report["coreRuntimePoolUID"] = poolUID
	report["coreRuntimePoolAbsent"] = true
	return report, nil
}

func (p *proof) verifyRetiredInventory(ctx context.Context, workspaceUID, poolUID string) (map[string]any, error) {
	actors, err := p.native.ListActors(ctx, &pb.ListActorsRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil || len(actors.GetActors()) != 0 || actors.GetNextPageToken() != "" {
		return nil, fmt.Errorf("rejected native namespace retains Actors: %v", err)
	}
	tags, err := p.native.ListTags(ctx, &pb.ListTagsRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil || len(tags.GetTags()) != 0 || tags.GetNextPageToken() != "" {
		return nil, fmt.Errorf("rejected native namespace retains Tags: %v", err)
	}
	templates, err := p.native.ListActorTemplates(ctx, &pb.ListActorTemplatesRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil || len(templates.GetActorTemplates()) != 1 || templates.GetActorTemplates()[0].GetMetadata().GetName() != "infrastructure" || templates.GetNextPageToken() != "" {
		return nil, fmt.Errorf("rejected native namespace has a derived runtime template: %v", err)
	}
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1alpha1")
	pools.SetKind("WorkerPoolList")
	if err := p.c.List(ctx, pools, client.InNamespace(p.namespace)); err != nil {
		return nil, err
	}
	if len(pools.Items) != 1 || pools.Items[0].GetName() != "native-core" {
		return nil, fmt.Errorf("rejected native namespace retains a private worker allocation")
	}
	if expected := os.Getenv("RETIRED_SOURCE_POOL_UID"); expected != "" && string(pools.Items[0].GetUID()) != expected {
		return nil, fmt.Errorf("retained operator pool identity differs")
	}
	var pods corev1.PodList
	if err := p.c.List(ctx, &pods, client.InNamespace(p.namespace)); err != nil {
		return nil, err
	}
	for _, pod := range pods.Items {
		if pod.Labels["substrate.workspace.orka.ai/allocation-id"] != "" {
			return nil, fmt.Errorf("retired native namespace retains a private worker Pod")
		}
	}
	var secrets corev1.SecretList
	if err := p.c.List(ctx, &secrets, client.InNamespace(p.namespace)); err != nil {
		return nil, err
	}
	for _, secret := range secrets.Items {
		if secret.Labels["workspace.orka.ai/attachment-for"] == workspaceUID || (poolUID != "" && secret.Labels["orka.ai/runtime-pool-uid"] == poolUID) {
			return nil, fmt.Errorf("retired workspace retains attachment or runtime credential Secrets")
		}
	}
	report := map[string]any{"namespace": p.namespace, "nativeActors": 0, "nativeTags": 0, "derivedRuntimeTemplates": 0, "privateWorkerPools": 0, "privateWorkerPods": 0, "attachmentSecrets": 0,
		"operatorWorkerPoolUID": pools.Items[0].GetUID(), "operatorTemplateUID": templates.GetActorTemplates()[0].GetMetadata().GetUid()}
	if poolUID != "" {
		report["runtimeCredentialSecrets"] = 0
	}
	return report, nil
}

func main() {
	namespace, image := os.Getenv("POD_NAMESPACE"), os.Getenv("RUNTIME_IMAGE")
	if namespace == "" || image == "" {
		fmt.Fprintln(os.Stderr, "POD_NAMESPACE and RUNTIME_IMAGE are required")
		os.Exit(2)
	}
	config, err := rest.InClusterConfig()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)
	_ = api.AddToScheme(scheme)
	_ = profile.AddToScheme(scheme)
	var c client.Client
	if err == nil {
		c, err = client.New(config, client.Options{Scheme: scheme})
	}
	controlConfig := provider.Config{APIEndpoint: "api.ate-system.svc:443", CAFile: "/native/server/trust-bundle.pem", CertFile: "/native/client/credential-bundle.pem", KeyFile: "/native/client/credential-bundle.pem", ActorDNSSuffix: "actors.resources.substrate.ate.dev", DirectEgressEnabled: true}
	var report map[string]any
	if err == nil {
		conn, dialErr := provider.Dial(controlConfig)
		err = dialErr
		if err == nil {
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
			defer cancel()
			p := &proof{c: c, native: pb.NewControlClient(conn), namespace: namespace, image: image}
			if os.Getenv("RETIRED_TASK_UID") != "" {
				report, err = p.verifyRetiredTask(ctx)
			} else if os.Getenv("REJECTED_WORKSPACE_UID") != "" {
				report, err = p.verifyRejectedRetirement(ctx)
			} else {
				report, err = p.run(ctx)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "native Core Task proof failed:", err)
		os.Exit(1)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("NATIVE_CORE_PROOF", string(encoded))
}
