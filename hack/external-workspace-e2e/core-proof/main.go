// Copyright (c) 2026. MIT License - see LICENSE file for details.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	workspaceapi "github.com/orka-agents/orka-workspace/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const namespace = "orka-system"

func object(kind, name string, spec map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "core.orka.ai/v1alpha1", "kind": kind,
		"metadata": map[string]any{"name": name, "namespace": namespace}}}
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
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func run(ctx context.Context, c client.Client, runID string) (map[string]any, error) {
	params := object("FakePoolParameters", "core-"+runID, map[string]any{})
	params.SetAPIVersion("fake.workspace.orka.ai/v1alpha1")
	if err := c.Create(ctx, params); err != nil {
		return nil, err
	}
	lifecycle := workspaceapi.ExecutionWorkspaceLifecycle{DefaultOnDetach: workspaceapi.WorkspaceOnDetachDelete,
		AllowedOnDetach: []workspaceapi.WorkspaceOnDetach{workspaceapi.WorkspaceOnDetachDelete},
		DetachTimeout:   metav1.Duration{Duration: time.Minute},
		DeletionPolicy: workspaceapi.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspaceapi.WorkspaceDeletionActionDelete,
			PersistentVolumes: workspaceapi.WorkspaceDeletionActionDelete, Checkpoints: workspaceapi.WorkspaceDeletionActionDelete}}
	class := &workspaceapi.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "core-" + runID},
		Spec: workspaceapi.ExecutionWorkspaceClassSpec{ProviderRef: &workspaceapi.ClusterObjectReference{Name: "fake"},
			ParametersRef: &workspaceapi.TypedObjectReference{Group: "fake.workspace.orka.ai", Kind: "FakePoolParameters", Name: params.GetName()},
			Mode:          workspaceapi.ExecutionWorkspaceModeInteractive, RequiredFeatures: []workspaceapi.ExecutionWorkspaceFeature{workspaceapi.WorkspaceFeatureACPRuntime},
			AllowedReuseScopes: []workspaceapi.WorkspaceReuseScope{workspaceapi.WorkspaceReuseScopeNone}, Lifecycle: lifecycle}}
	if err := c.Create(ctx, class); err != nil {
		return nil, err
	}
	if err := wait(ctx, func() (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(class), class); err != nil {
			return false, err
		}
		for _, condition := range class.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == class.Generation {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return nil, fmt.Errorf("external class readiness: %w; conditions=%v", err, class.Status.Conditions)
	}
	fmt.Println("PASS Orka resolves current external provider and opaque fake profile")
	agent := object("Agent", "core-"+runID, map[string]any{
		"runtime": map[string]any{"contractVersion": "orka.harness.v2", "type": "codex", "defaultMaxTurns": int64(1),
			"defaultAllowedTools": []any{"Glob", "Grep", "Read"}, "defaultAllowBash": false},
		"model": map[string]any{"name": "gpt-5.4"}})
	if err := c.Create(ctx, agent); err != nil {
		return nil, fmt.Errorf("Agent admission: %w", err)
	}
	task := object("Task", "core-"+runID, map[string]any{
		"type": "agent", "agentRef": map[string]any{"name": agent.GetName()},
		"prompt": "Run the deterministic external workspace fixture.", "timeout": "5m",
		"execution":    map[string]any{"workspace": map[string]any{"classRef": map[string]any{"name": class.Name}, "reusePolicy": "none", "onDetach": "Delete"}},
		"agentRuntime": map[string]any{"maxTurns": int64(1), "allowedTools": []any{"Glob", "Grep", "Read"}, "allowBash": false}})
	if err := c.Create(ctx, task); err != nil {
		return nil, fmt.Errorf("Task admission: %w", err)
	}
	fmt.Println("PASS fail-closed Task class/provenance/authority admission accepts authorized request")
	workspaceName := ""
	var startup *workspaceapi.StartupEvidence
	var workspace *workspaceapi.ExecutionWorkspace
	var servingInstance string
	var servingProfileDigest string
	var servingBoot string
	verifiedPod := false
	policyNames := []string{}
	if err := wait(ctx, func() (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
			return false, err
		}
		if name := str(task, "status", "executionWorkspace", "workspaceRef", "name"); name != "" {
			workspaceName = name
		} else if name := task.GetLabels()["acp.workspace.orka.ai/execution-workspace"]; name != "" {
			// The settlement link exists before attachment acknowledgement;
			// observe provisioning while the public status catches up.
			workspaceName = name
		}
		if workspaceName != "" {
			current := &workspaceapi.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspaceName}, current); err == nil {
				workspace = current
				if current.Status.Allocation != nil && current.Status.Allocation.Startup != nil {
					startup = current.Status.Allocation.Startup.DeepCopy()
					if startup.Pod == nil {
						return false, fmt.Errorf("fake provider startup evidence omitted exact Pod identity")
					}
					pod := &corev1.Pod{}
					if err := c.Get(ctx, client.ObjectKey{Namespace: startup.Pod.Namespace, Name: startup.Pod.Name}, pod); err == nil {
						if pod.UID != startup.Pod.UID || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
							return false, fmt.Errorf("actual provider Pod failed exact UID or token automount verification")
						}
						verifiedPod = true
						var policies networkingv1.NetworkPolicyList
						if err := c.List(ctx, &policies, client.InNamespace(pod.Namespace)); err != nil {
							return false, err
						}
						policyNames = nil
						for _, policy := range policies.Items {
							selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
							if err == nil && selector.Matches(labels.Set(pod.Labels)) {
								policyNames = append(policyNames, policy.Name)
							}
						}
						if len(policyNames) == 0 {
							return false, fmt.Errorf("no realized NetworkPolicy selects the exact provider Pod")
						}
					} else if !apierrors.IsNotFound(err) {
						return false, err
					}
				}
			} else if !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		if poolName := str(task, "status", "execution", "runtimePoolName"); poolName != "" {
			pool := object("RuntimePool", poolName, nil)
			if err := c.Get(ctx, client.ObjectKeyFromObject(pool), pool); err == nil && str(pool, "status", "lifecycle") == "Serving" {
				servingInstance = str(pool, "status", "activeInstance", "runtimeInstanceID")
				servingProfileDigest = str(pool, "status", "activeInstance", "profileDigest")
				servingBoot = str(pool, "status", "activeInstance", "bootID")
			}
		}
		phase := str(task, "status", "phase")
		if phase == "Failed" || phase == "Cancelled" {
			return false, fmt.Errorf("Task became %s; execution state=%s, reason=%s: %s", phase,
				str(task, "status", "execution", "state"), str(task, "status", "execution", "reason"), str(task, "status", "execution", "message"))
		}
		return phase == "Succeeded", nil
	}); err != nil {
		return nil, fmt.Errorf("Task execution: %w; last phase=%s, execution=%s, workspace=%s", err,
			str(task, "status", "phase"), str(task, "status", "execution", "state"), workspaceName)
	}
	identity := map[string]string{}
	if available, _, _ := unstructured.NestedBool(task.Object, "status", "resultRef", "available"); !available {
		return nil, fmt.Errorf("successful Task omitted persisted result availability")
	}
	for _, field := range []string{"runtimeSessionUID", "runtimeInstanceID", "runtimeSessionSupervisorBootID", "promptID"} {
		identity[field] = str(task, "status", "execution", field)
		if identity[field] == "" {
			return nil, fmt.Errorf("successful Task omitted %s session evidence", field)
		}
	}
	fmt.Println("PASS actual supervisor bootstraps and executes a deterministic ACP RuntimeSession prompt")
	if workspaceName == "" || workspace == nil || startup == nil || !verifiedPod || servingInstance != identity["runtimeInstanceID"] || servingProfileDigest == "" || servingBoot != identity["runtimeSessionSupervisorBootID"] {
		return nil, fmt.Errorf("could not independently observe the provider-created startup Pod")
	}
	if startup.Pod.UID == "" || startup.Pod.Namespace != "orka-runtimes" {
		return nil, fmt.Errorf("startup evidence did not name an exact runtime namespace Pod")
	}
	if err := wait(ctx, func() (bool, error) {
		err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspaceName}, workspace)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return workspace.Status.State == workspaceapi.ExecutionWorkspaceStateDeleted && workspace.Status.Disposition != nil, nil
	}); err != nil {
		return nil, fmt.Errorf("core exact-instance retirement: %w", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: startup.Pod.Namespace, Name: startup.Pod.Name}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("retired runtime Pod remains: %v", err)
	}
	var tokens corev1.SecretList
	if err := c.List(ctx, &tokens, client.InNamespace(namespace), client.MatchingLabels{"workspace.orka.ai/attachment-for": string(workspace.UID)}); err != nil {
		return nil, err
	}
	if len(tokens.Items) != 0 {
		return nil, fmt.Errorf("retired workspace retains %d attachment token Secrets", len(tokens.Items))
	}
	fmt.Println("PASS terminal Task revokes attachment credentials and retires the exact runtime Pod")
	return map[string]any{"task": namespace + "/" + task.GetName(), "phase": "Succeeded", "workspace": namespace + "/" + workspaceName,
		"startupPod": startup.Pod, "runtimeSession": identity, "runtimeProfileDigest": servingProfileDigest, "selectedNetworkPolicies": policyNames, "attachmentSecretsRemaining": 0,
		"networkPolicyEnforcement": "policy objects verified; kind default CNI does not prove packet enforcement"}, nil
}

func main() {
	runID := flag.String("run-id", "", "isolated resource suffix")
	report := flag.String("report", "", "public JSON proof report")
	flag.Parse()
	if *runID == "" || os.Getenv("KUBECONFIG") == "" {
		fmt.Fprintln(os.Stderr, "run-id and kindctl-scoped KUBECONFIG are required")
		os.Exit(2)
	}
	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err == nil {
		config.QPS, config.Burst = 20, 40
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)
	_ = workspaceapi.AddToScheme(scheme)
	// Unstructured Orka objects use live discovery rather than coupling the
	// independently built provider harness to Orka's implementation module.
	var c client.Client
	if err == nil {
		c, err = client.New(config, client.Options{Scheme: scheme})
	}
	var result map[string]any
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()
		result, err = run(ctx, c, *runID)
	}
	if err == nil && *report != "" {
		var data []byte
		data, err = json.MarshalIndent(result, "", "  ")
		if err == nil {
			err = os.WriteFile(*report, append(data, '\n'), 0600)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "core external workspace proof failed:", err)
		os.Exit(1)
	}
	fmt.Println("Core RuntimeSession proof passed.")
}
