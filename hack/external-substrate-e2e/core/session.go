// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Fixture contract shared with hack/external-workspace-e2e/acp-agent and
// core/providerproxy (separate modules, so the values are repeated).
const (
	// fixtureThreadID is the Codex rollout thread the agent uses as its ACP
	// session ID when it runs with a CODEX_HOME.
	fixtureThreadID     = "01a10020-1222-76e3-977d-d5165792ae72"
	providerProbeMarker = "orka-fixture:provider-probe"
	holdMarkerPrefix    = "orka-fixture:hold-seconds="
	// providerTokenKey is the key of Core's private provider Secret.
	providerTokenKey = "token"
	// Core RuntimePool eviction surface (Orka ADR 0046).
	evictionAnnotation = "orka.ai/external-runtime-eviction"
	sessionName        = "native-core-session"
)

// providerObservations is the fixture provider proxy's count of requests per
// Authorization class, plus the agent's session reports on injected requests.
type providerObservations struct {
	Injected    int      `json:"injected"`
	Placeholder int      `json:"placeholder"`
	Missing     int      `json:"missing"`
	Other       int      `json:"other"`
	Sessions    []string `json:"sessions"`
}

// observationsClient is shared by every poll of the fixture proxy, so its
// connection is reused instead of leaving one idle transport per poll.
var observationsClient = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}

func (p *proof) proxyHost() string {
	return "fixture-provider-proxy." + p.namespace + ".svc.cluster.local"
}

func (p *proof) providerObservations(ctx context.Context) (providerObservations, error) {
	var seen providerObservations
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+p.proxyHost()+":8081/observations", nil)
	if err != nil {
		return seen, err
	}
	response, err := observationsClient.Do(request)
	if err != nil {
		return seen, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return seen, fmt.Errorf("fixture provider proxy observations HTTP %d", response.StatusCode)
	}
	return seen, json.NewDecoder(response.Body).Decode(&seen)
}

// waitProviderRequests waits until the fixture proxy has seen want injected
// requests and requires that no request arrived without Core's token.
func (p *proof) waitProviderRequests(ctx context.Context, want int) (providerObservations, error) {
	var seen providerObservations
	err := wait(ctx, func() (bool, error) {
		var err error
		if seen, err = p.providerObservations(ctx); err != nil {
			return false, nil
		}
		if seen.Placeholder != 0 || seen.Missing != 0 || seen.Other != 0 {
			return false, fmt.Errorf("provider proxy received a request without the injected token: %+v", seen)
		}
		if seen.Injected > want {
			return false, fmt.Errorf("provider proxy received more injected requests than prompts: %+v", seen)
		}
		return seen.Injected == want && len(seen.Sessions) == want, nil
	})
	if err != nil {
		return seen, fmt.Errorf("injected provider request %d: %w (last %+v)", want, err, seen)
	}
	return seen, nil
}

// requireFixtureInjection binds the gateway credential injection that
// observeStartup verified (requireCredentialInjection) to this fixture: Core's
// provider Secret token is the bearer for the provider-proxy origin's port
// 8080, and that Secret is Core's labeled, RuntimePool-owned credential.
// Secret values are never read: only the Secret's identity labels and owner.
// It runs at startup observation, before settlement retires the Secret.
func (p *proof) requireFixtureInjection(ctx context.Context) error {
	in := p.injection
	ports, _ := in["ports"].([]int32)
	name, _ := in["secret"].(string)
	if in["host"] != p.proxyHost() || !slices.Equal(ports, []int32{8080}) || in["header"] != "Authorization" || in["prefix"] != "Bearer " || name == "" ||
		in["credentialURI"] != fmt.Sprintf("ate-secret://k8s.io/default/%s/%s/%s", p.namespace, name, providerTokenKey) {
		return fmt.Errorf("Core credential replacement is not its provider Secret token as the bearer for the provider-proxy origin: %v", in)
	}
	secret := &corev1.Secret{}
	if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: name}, secret); err != nil {
		return err
	}
	owner := metav1.GetControllerOf(secret)
	if secret.Labels["orka.ai/runtime-pool-provider-credential"] != "true" || owner == nil || owner.Kind != "RuntimePool" {
		return fmt.Errorf("injected provider Secret is not Core's labeled, RuntimePool-owned credential")
	}
	in["secretOwner"] = owner.Name
	return nil
}

func conditionOf(u *unstructured.Unstructured, kind string) (string, string) {
	conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, item := range conditions {
		if c, ok := item.(map[string]any); ok && c["type"] == kind {
			return fmt.Sprint(c["status"]), fmt.Sprint(c["reason"])
		}
	}
	return "", ""
}

// telemetryReady is the last observed TelemetryReady condition of a Core
// RuntimePool. Core sets it when it publishes a native workload with
// telemetry enabled: True/TelemetryProjected when every supervisor signal fits
// the backend's 32-entry environment limit, else False/NativeEnvironmentLimit.
type telemetryReady struct {
	RuntimePool string `json:"runtimePool"`
	Status      string `json:"status"`
	Reason      string `json:"reason"`
	Message     string `json:"message"`
}

// observeTelemetryReady records pool's TelemetryReady condition and fails as
// soon as Core reports that it left supervisor telemetry out.
func observeTelemetryReady(pool *unstructured.Unstructured, seen *telemetryReady) error {
	conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
	for _, item := range conditions {
		c, ok := item.(map[string]any)
		if !ok || c["type"] != "TelemetryReady" {
			continue
		}
		*seen = telemetryReady{RuntimePool: pool.GetName(), Status: fmt.Sprint(c["status"]), Reason: fmt.Sprint(c["reason"]), Message: fmt.Sprint(c["message"])}
		if seen.Status != "True" || seen.Reason != "TelemetryProjected" {
			return fmt.Errorf("RuntimePool %s reports TelemetryReady=%s/%s: %s", seen.RuntimePool, seen.Status, seen.Reason, seen.Message)
		}
	}
	return nil
}

// requireTelemetryReady requires that a RuntimePool reported every supervisor
// telemetry signal projected.
func requireTelemetryReady(seen telemetryReady, what string) error {
	if seen.Status != "True" || seen.Reason != "TelemetryProjected" {
		return fmt.Errorf("%s RuntimePool never reported TelemetryReady=True/TelemetryProjected (last observed %+v)", what, seen)
	}
	return nil
}

// sessionInstance is one observed runtime instance of the session workspace.
type sessionInstance struct {
	workspace *api.ExecutionWorkspace
	startup   *api.StartupEvidence
	actor     *pb.Actor
	worker    *corev1.Pod
	pool      *unstructured.Unstructured
	task      *unstructured.Unstructured
	telemetry telemetryReady
}

func (i *sessionInstance) report() map[string]any {
	return map[string]any{"task": i.task.GetName(), "taskUID": i.task.GetUID(), "telemetryReady": i.telemetry, "workloadSequence": i.workspace.Spec.Workload.Sequence, "workloadRevision": i.workspace.Spec.Workload.Revision,
		"identity": i.workspace.Status.Allocation.Identity, "nativeActorUID": i.actor.GetMetadata().GetUid(), "worker": map[string]any{"name": i.worker.Name, "uid": i.worker.UID},
		"privateWorkerPool": map[string]any{"name": i.pool.GetName(), "uid": i.pool.GetUID()}, "runtimePool": str(i.task, "status", "execution", "runtimePoolName"),
		"runtimeSessionUID": str(i.task, "status", "execution", "runtimeSessionUID"), "runtimeInstanceID": str(i.task, "status", "execution", "runtimeInstanceID"),
		"supervisorBootID": str(i.task, "status", "execution", "runtimeSessionSupervisorBootID"), "promptID": str(i.task, "status", "execution", "promptID")}
}

// sessionTask creates one turn of the proof Session and waits until its
// workspace's new instance is observed, then for until to accept the Task.
func (p *proof) sessionTask(ctx context.Context, name, class, onDetach, prompt string, previous *sessionInstance, until func(*sessionInstance) (bool, error)) (*sessionInstance, error) {
	sessionRef := map[string]any{"name": sessionName}
	if previous == nil {
		sessionRef["create"] = true
	}
	task := p.object("Task", name, map[string]any{"type": "agent", "agentRef": map[string]any{"name": "native-core"}, "sessionRef": sessionRef,
		"prompt": prompt, "timeout": "10m",
		"execution":    map[string]any{"workspace": map[string]any{"classRef": map[string]any{"name": class}, "reusePolicy": "session", "onDetach": onDetach}},
		"agentRuntime": map[string]any{"maxTurns": int64(1), "allowedTools": []any{"Glob", "Grep", "Read"}, "allowBash": false}})
	if err := p.c.Create(ctx, task); err != nil {
		return nil, fmt.Errorf("%s admission: %w", name, err)
	}
	fmt.Printf("ACCEPTED_TASK namespace=%s name=%s uid=%s\n", p.namespace, task.GetName(), task.GetUID())
	instance := &sessionInstance{task: task}
	err := wait(ctx, func() (bool, error) {
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
			return false, err
		}
		if phase := str(task, "status", "phase"); phase == "Failed" || phase == "Cancelled" {
			return false, fmt.Errorf("%s became %s: %s %s", name, phase, str(task, "status", "execution", "reason"), str(task, "status", "execution", "message"))
		}
		if poolName := str(task, "status", "execution", "runtimePoolName"); poolName != "" {
			pool := p.object("RuntimePool", poolName, nil)
			if err := p.c.Get(ctx, client.ObjectKeyFromObject(pool), pool); err == nil {
				if err := observeTelemetryReady(pool, &instance.telemetry); err != nil {
					return false, err
				}
			} else if !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		if instance.startup == nil {
			workspaceName := task.GetLabels()["acp.workspace.orka.ai/execution-workspace"]
			if workspaceName == "" {
				return false, nil
			}
			current := &api.ExecutionWorkspace{}
			if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: workspaceName}, current); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			if previous != nil && current.UID != previous.workspace.UID {
				return false, fmt.Errorf("the continuation attached a different workspace incarnation")
			}
			a := current.Status.Allocation
			if current.Spec.Workload == nil || a == nil || a.Startup == nil || a.State != api.AllocationReady || (previous != nil && a.Sequence <= previous.workspace.Status.Allocation.Sequence) {
				return false, nil
			}
			actor, worker, pool, _, err := p.observeStartup(ctx, current)
			if err != nil {
				return false, err
			}
			instance.workspace, instance.startup, instance.actor, instance.worker, instance.pool = current, a.Startup.DeepCopy(), actor, worker, pool
		}
		return until(instance)
	})
	if err != nil {
		return instance, fmt.Errorf("%s: %w; last phase=%s state=%s", name, err, str(task, "status", "phase"), str(task, "status", "execution", "state"))
	}
	return instance, nil
}

func (p *proof) taskSucceeded(i *sessionInstance) (bool, error) {
	if str(i.task, "status", "phase") != "Succeeded" {
		return false, nil
	}
	available, _, _ := unstructured.NestedBool(i.task.Object, "status", "resultRef", "available")
	if !available || i.startup == nil || str(i.task, "status", "execution", "promptID") == "" || str(i.task, "status", "execution", "runtimeSessionUID") == "" ||
		str(i.task, "status", "execution", "runtimeSessionSupervisorBootID") == "" {
		return false, fmt.Errorf("%s succeeded without startup, prompt, supervisor boot and persisted result evidence", i.task.GetName())
	}
	return true, nil
}

// runSession proves a two-turn Session across a Substrate worker eviction: the
// first turn's prompt is held while its exact worker Pod is deleted, Core
// drains and suspends the workspace data-only, and the second turn cold-resumes
// on a new Actor and worker through session/resume before full retirement.
func (p *proof) runSession(ctx context.Context) (map[string]any, error) {
	parameters := &profile.SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace, Name: "native-session"},
		Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Namespace: p.namespace, Name: "infrastructure"},
			Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly}}}
	if err := p.c.Create(ctx, parameters); err != nil {
		return nil, err
	}
	class := &api.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: p.namespace, Name: "native-session"},
		Spec: api.ExecutionWorkspaceClassSpec{ProviderRef: &api.ClusterObjectReference{Name: "substrate"},
			ParametersRef: &api.TypedObjectReference{Group: profile.GroupVersion.Group, Kind: "SubstrateWorkspaceProfile", Name: parameters.Name},
			Mode:          api.ExecutionWorkspaceModeInteractive, RequiredFeatures: []api.ExecutionWorkspaceFeature{api.WorkspaceFeatureACPRuntime},
			AllowedReuseScopes: []api.WorkspaceReuseScope{api.WorkspaceReuseScopeSession}, Lifecycle: api.ExecutionWorkspaceLifecycle{
				DefaultOnDetach: api.WorkspaceOnDetachSuspend, AllowedOnDetach: []api.WorkspaceOnDetach{api.WorkspaceOnDetachSuspend, api.WorkspaceOnDetachDelete},
				DetachTimeout: metav1.Duration{Duration: 2 * time.Minute}, MaxLifetime: &metav1.Duration{Duration: time.Hour},
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
		return nil, fmt.Errorf("suspendable native class readiness: %w; conditions=%v", err, class.Status.Conditions)
	}
	p.pass("actual Core admits a data-only suspendable Session class on the external native provider")

	// Turn 1: wait for the held prompt's injected provider request, then
	// delete the exact attested worker Pod while the prompt is still accepted.
	before, err := p.providerObservations(ctx)
	if err != nil {
		return nil, err
	}
	evictedAt := time.Time{}
	first, err := p.sessionTask(ctx, "native-session-1", class.Name, "Suspend",
		"Turn one of the Substrate Session proof. "+providerProbeMarker+" "+holdMarkerPrefix+"60", nil, func(i *sessionInstance) (bool, error) {
			if str(i.task, "status", "execution", "promptID") == "" || str(i.task, "status", "phase") != "Running" {
				return false, nil
			}
			seen, err := p.providerObservations(ctx)
			return err == nil && seen.Injected == before.Injected+1, nil
		})
	if err != nil {
		return nil, err
	}
	poolName := str(first.task, "status", "execution", "runtimePoolName")
	if poolName == "" {
		return nil, fmt.Errorf("running turn omitted its RuntimePool")
	}
	// Core runs this phase with telemetry: a suspendable native supervisor
	// must still receive every supervisor signal within the 32-entry limit.
	if err := requireTelemetryReady(first.telemetry, "turn 1"); err != nil {
		return nil, err
	}
	uid := first.worker.UID
	if err := p.c.Delete(ctx, first.worker.DeepCopy(), client.Preconditions{UID: &uid}); err != nil {
		return nil, fmt.Errorf("evict exact worker Pod: %w", err)
	}
	evictedAt = time.Now().UTC()
	fmt.Printf("EVICTED_WORKER namespace=%s name=%s uid=%s\n", first.worker.Namespace, first.worker.Name, uid)

	// Observe the RuntimePool eviction surface until the data-only suspension
	// is reported and turn 1 settles.
	var record map[string]any
	seen := map[string]bool{}
	var suspended *api.ExecutionWorkspace
	if err := wait(ctx, func() (bool, error) {
		pool := p.object("RuntimePool", poolName, nil)
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(pool), pool); err != nil {
			return false, err
		}
		if err := observeTelemetryReady(pool, &first.telemetry); err != nil {
			return false, err
		}
		if raw := pool.GetAnnotations()[evictionAnnotation]; raw != "" {
			parsed := map[string]any{}
			if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
				return false, fmt.Errorf("eviction annotation is not JSON")
			}
			identity, _ := parsed["identity"].(map[string]any)
			if parsed["workspaceUID"] != string(first.workspace.UID) || identity["allocationID"] != first.workspace.Status.Allocation.Identity.AllocationID ||
				identity["instanceID"] != first.workspace.Status.Allocation.Identity.InstanceID || parsed["suspendable"] != true {
				return false, fmt.Errorf("eviction record does not bind the exact suspendable instance: %s", raw)
			}
			record = parsed
		}
		evictionStatus, evictionReason := conditionOf(pool, "WorkerEviction")
		admissionStatus, admissionReason := conditionOf(pool, "AdmissionReady")
		if evictionStatus == "True" && evictionReason == "EvictionDraining" {
			seen["EvictionDraining"] = true
			if admissionStatus == "False" && admissionReason == "AdmissionClosed" {
				seen["AdmissionClosed"] = true
			}
			if str(pool, "status", "lifecycle") == "Draining" {
				seen["Draining"] = true
			}
		}
		if evictionStatus == "False" && (evictionReason == "EvictionDeadlineExceeded" || evictionReason == "EvictionNotSuspendable") {
			return false, fmt.Errorf("eviction ended with %s", evictionReason)
		}
		if evictionStatus == "False" && evictionReason == "EvictionSuspended" {
			seen["EvictionSuspended"] = true
		}
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(first.task), first.task); err != nil {
			return false, err
		}
		if phase := str(first.task, "status", "phase"); phase == "Failed" || phase == "Cancelled" {
			return false, fmt.Errorf("evicted turn became %s: %s %s", phase, str(first.task, "status", "execution", "reason"), str(first.task, "status", "execution", "message"))
		}
		current := &api.ExecutionWorkspace{}
		if err := p.c.Get(ctx, client.ObjectKeyFromObject(first.workspace), current); err != nil {
			return false, err
		}
		a := current.Status.Allocation
		if current.UID != first.workspace.UID || a == nil || a.Identity != first.workspace.Status.Allocation.Identity {
			return false, fmt.Errorf("evicted workspace changed its exact instance before suspension")
		}
		if current.Status.State == api.ExecutionWorkspaceStateSuspended && a.State == api.AllocationStopped && a.RetainedData != nil {
			suspended = current
		}
		done, err := p.taskSucceeded(first)
		return done && suspended != nil && seen["EvictionSuspended"] && record != nil, err
	}); err != nil {
		return nil, fmt.Errorf("worker eviction drain and data-only suspension: %w; observed=%v", err, seen)
	}
	for _, state := range []string{"EvictionDraining", "AdmissionClosed", "Draining"} {
		if !seen[state] {
			return nil, fmt.Errorf("worker eviction never reported %s while draining; observed=%v", state, seen)
		}
	}
	p.pass("deleting the attested worker Pod records an exact-instance eviction, closes admission and drains (EvictionDraining, AdmissionClosed, Draining)")
	if err := wait(ctx, func() (bool, error) {
		_, nativeErr := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: &pb.ObjectRef{Atespace: first.startup.Process.Namespace, Name: first.startup.Process.Name}})
		if nativeErr != nil && status.Code(nativeErr) != codes.NotFound {
			return false, nativeErr
		}
		workerErr := p.c.Get(ctx, client.ObjectKeyFromObject(first.worker), &corev1.Pod{})
		poolErr := p.c.Get(ctx, client.ObjectKeyFromObject(first.pool), &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ate.dev/v1", "kind": "WorkerPool"}})
		return status.Code(nativeErr) == codes.NotFound && apierrors.IsNotFound(workerErr) && apierrors.IsNotFound(poolErr), nil
	}); err != nil {
		return nil, fmt.Errorf("evicted Actor/private pool/worker retirement: %w", err)
	}
	p.pass("the held prompt settles, the turn succeeds and the workspace suspends data-only (EvictionSuspended) with retained VOLUMES data; the evicted Actor, worker and private pool are gone")

	// Turn 2: cold resume of the same workspace on a new Actor and worker.
	second, err := p.sessionTask(ctx, "native-session-2", class.Name, "Delete",
		"Turn two of the Substrate Session proof. "+providerProbeMarker, first, p.taskSucceeded)
	if err != nil {
		return nil, err
	}
	if err := requireTelemetryReady(second.telemetry, "turn 2"); err != nil {
		return nil, err
	}
	p.pass("both Session turns run with supervisor telemetry: each suspendable native RuntimePool reports TelemetryReady=True/TelemetryProjected")
	w := second.workspace
	if w.Spec.Workload.PreviousInstance == nil || *w.Spec.Workload.PreviousInstance != first.workspace.Status.Allocation.Identity || w.Spec.Workload.RetainedData == nil ||
		suspended.Status.Allocation.RetainedData == nil || *w.Spec.Workload.RetainedData != *suspended.Status.Allocation.RetainedData {
		return nil, fmt.Errorf("continuation workload does not fence the evicted instance and resume its retained data")
	}
	if second.actor.GetMetadata().GetUid() == first.actor.GetMetadata().GetUid() || second.worker.UID == first.worker.UID || second.pool.GetUID() == first.pool.GetUID() ||
		w.Status.Allocation.Identity == first.workspace.Status.Allocation.Identity {
		return nil, fmt.Errorf("continuation did not cold-boot a new Actor, worker and private pool")
	}
	if str(second.task, "status", "execution", "runtimeSessionSupervisorBootID") == str(first.task, "status", "execution", "runtimeSessionSupervisorBootID") {
		return nil, fmt.Errorf("continuation reused the evicted supervisor boot")
	}
	requests, err := p.waitProviderRequests(ctx, before.Injected+2)
	if err != nil {
		return nil, err
	}
	turns := requests.Sessions[len(requests.Sessions)-2:]
	if turns[0] != "mode=new;session="+fixtureThreadID || turns[1] != "mode=resume;session="+fixtureThreadID {
		return nil, fmt.Errorf("the turns did not continue one native session through session/resume: %v", turns)
	}
	p.pass("the continuation cold-resumes the retained data on a new Actor UID, worker Pod and supervisor boot, and the agent continues its native session through session/resume")
	if err := p.waitRetirement(ctx, w, second.startup, second.pool, second.worker, str(second.task, "status", "execution", "runtimePoolName"), str(second.task, "status", "execution", "runtimePoolUID")); err != nil {
		return nil, err
	}
	p.pass("the Delete detach of the final turn retires the session workspace, native Actor, private pool, worker, Core RuntimePool and credentials")
	return map[string]any{"class": class.Name, "profile": parameters.Name, "session": sessionName, "workspace": w.Name, "workspaceUID": w.UID,
		"turn1": first.report(), "turn2": second.report(), "evictedWorker": map[string]any{"name": first.worker.Name, "uid": first.worker.UID, "deletedAt": evictedAt},
		"evictionRecord": record, "observedEviction": seen, "retainedData": w.Spec.Workload.RetainedData, "providerSessionReports": turns,
		"suspendedState": suspended.Status.State, "memoryRestored": false}, nil
}
