// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Command fiberd-core-proof runs an actual Orka Task through deployed Core and
// the fiberd or Substrate provider, and times it. It creates only public
// objects and never calls the provider lifecycle or writes workspace status.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	workspaceapi "github.com/orka-agents/orka-workspace/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const namespace = "orka-system"

type challenge struct {
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
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type observation struct {
	startup          *workspaceapi.StartupEvidence
	service          *corev1.Service
	worker           *corev1.Pod
	challengeChecked bool
	firstStartup     time.Duration
}

// observe checks the provider's native evidence against independent reads.
// fiberd: the shared home Pod, the per-instance Service and the supervisor's
// own challenge. Substrate: the instance's private worker Pod.
func observe(ctx context.Context, c client.Client, provider string, w *workspaceapi.ExecutionWorkspace, o *observation) error {
	startup := w.Status.Allocation.Startup
	process := startup.Process
	if process == nil || startup.Pod != nil {
		return fmt.Errorf("%s startup omitted native process identity", provider)
	}
	endpoint, err := url.Parse(startup.Endpoint)
	if err != nil || endpoint.Scheme != "http" || (endpoint.Port() != "80" && endpoint.Port() != "") {
		return fmt.Errorf("%s startup endpoint %q does not serve the bootstrap port", provider, startup.Endpoint)
	}
	worker := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: process.Worker.Namespace, Name: process.Worker.Name}, worker); err != nil {
		return err
	}
	if worker.UID != process.Worker.UID || worker.DeletionTimestamp != nil {
		return fmt.Errorf("native worker %s/%s is not the recorded live Pod", process.Worker.Namespace, process.Worker.Name)
	}
	o.startup, o.worker = startup.DeepCopy(), worker
	if provider != "fiberd" {
		return nil
	}
	home := worker
	if home.Labels["fiberd.io/grant"] == "" {
		return fmt.Errorf("native worker is not the fiberd home Pod")
	}
	service := &corev1.Service{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: home.Namespace, Name: w.Status.Allocation.Identity.InstanceID}, service); err != nil {
		return err
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 80 || service.Labels["fiberd.workspace.orka.ai/instance"] != w.Status.Allocation.Identity.InstanceID {
		return fmt.Errorf("per-instance Service does not map the bootstrap port")
	}
	o.service = service
	if o.challengeChecked {
		return nil
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+home.Status.PodIP+":"+strconv.Itoa(service.Spec.Ports[0].TargetPort.IntValue())+"/v2/credential-bootstrap", nil)
	response, err := (&http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}).Do(request)
	if err != nil {
		return nil // the proof host may not route to Pod IPs; Core's own bootstrap still checks it
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil // Core already consumed the one-time challenge
	}
	var ch challenge
	if err := json.NewDecoder(io.LimitReader(response.Body, 32<<10)).Decode(&ch); err != nil {
		return err
	}
	canonical, _ := json.Marshal(ch)
	sum := sha256.Sum256(canonical)
	if "sha256:"+hex.EncodeToString(sum[:]) != process.ChallengeSHA256 || ch.Actor.Atespace != process.Namespace || ch.Actor.Name != process.Name || ch.Actor.UID != process.UID {
		return fmt.Errorf("supervisor challenge does not match the provider's native evidence")
	}
	o.challengeChecked = true
	return nil
}

// createAdmitted retries only while Core's admission webhook is not yet
// listening; any admission decision is final.
func createAdmitted(ctx context.Context, c client.Client, obj client.Object) error {
	deadline := time.Now().Add(90 * time.Second)
	for {
		err := c.Create(ctx, obj)
		if err == nil || !strings.Contains(err.Error(), "failed calling webhook") || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Second)
	}
}

// profiles are each provider's opaque class parameters.
var profiles = map[string]struct {
	group, kind string
	spec        map[string]any
}{
	"fiberd": {"fiberd.workspace.orka.ai", "FiberdPoolParameters", map[string]any{}},
	// The ActorTemplate comes from Orka's workspace-substrate-cluster.sh.
	"substrate": {"substrate.workspace.orka.ai", "SubstrateWorkspaceProfile",
		map[string]any{"templateRef": map[string]any{"namespace": "orka-system", "name": "orka-acp-infra"}}},
}

func run(ctx context.Context, c client.Client, provider, runID string) (map[string]any, error) {
	profile, ok := profiles[provider]
	if !ok {
		return nil, fmt.Errorf("unknown provider %q", provider)
	}
	params := object(profile.kind, "core-"+runID, profile.spec)
	params.SetAPIVersion(profile.group + "/v1alpha1")
	if err := c.Create(ctx, params); err != nil {
		return nil, err
	}
	lifecycle := workspaceapi.ExecutionWorkspaceLifecycle{DefaultOnDetach: workspaceapi.WorkspaceOnDetachDelete,
		AllowedOnDetach: []workspaceapi.WorkspaceOnDetach{workspaceapi.WorkspaceOnDetachDelete},
		DetachTimeout:   metav1.Duration{Duration: time.Minute},
		DeletionPolicy: workspaceapi.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspaceapi.WorkspaceDeletionActionDelete,
			PersistentVolumes: workspaceapi.WorkspaceDeletionActionDelete, Checkpoints: workspaceapi.WorkspaceDeletionActionDelete}}
	class := &workspaceapi.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "core-" + runID},
		Spec: workspaceapi.ExecutionWorkspaceClassSpec{ProviderRef: &workspaceapi.ClusterObjectReference{Name: provider},
			ParametersRef: &workspaceapi.TypedObjectReference{Group: profile.group, Kind: profile.kind, Name: params.GetName()},
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
		return nil, fmt.Errorf("%s class readiness: %w; conditions=%v", provider, err, class.Status.Conditions)
	}
	fmt.Printf("PASS Orka resolves the %s provider and its opaque profile\n", provider)
	agent := object("Agent", "core-"+runID, map[string]any{
		"runtime": map[string]any{"contractVersion": "orka.harness.v2", "type": "codex", "defaultMaxTurns": int64(1),
			"defaultAllowedTools": []any{"Glob", "Grep", "Read"}, "defaultAllowBash": false},
		"model": map[string]any{"name": "gpt-5.4"}})
	if err := createAdmitted(ctx, c, agent); err != nil {
		return nil, fmt.Errorf("Agent admission: %w", err)
	}
	task := object("Task", "core-"+runID, map[string]any{
		"type": "agent", "agentRef": map[string]any{"name": agent.GetName()},
		"prompt": "Run the deterministic workspace fixture.", "timeout": "5m",
		"execution":    map[string]any{"workspace": map[string]any{"classRef": map[string]any{"name": class.Name}, "reusePolicy": "none", "onDetach": "Delete"}},
		"agentRuntime": map[string]any{"maxTurns": int64(1), "allowedTools": []any{"Glob", "Grep", "Read"}, "allowBash": false}})
	created := time.Now()
	if err := createAdmitted(ctx, c, task); err != nil {
		return nil, fmt.Errorf("Task admission: %w", err)
	}
	fmt.Println("PASS fail-closed Task class/provenance/authority admission accepts the request")
	workspaceName := ""
	var workspace *workspaceapi.ExecutionWorkspace
	o := &observation{}
	var serving time.Duration
	var servingInstance, servingBoot string
	if err := wait(ctx, func() (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
			return false, err
		}
		if name := str(task, "status", "executionWorkspace", "workspaceRef", "name"); name != "" {
			workspaceName = name
		} else if name := task.GetLabels()["acp.workspace.orka.ai/execution-workspace"]; name != "" {
			workspaceName = name
		}
		if workspaceName != "" {
			current := &workspaceapi.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspaceName}, current); err == nil {
				workspace = current
				if current.Status.Allocation != nil && current.Status.Allocation.Startup != nil {
					if o.firstStartup == 0 {
						o.firstStartup = time.Since(created)
					}
					if err := observe(ctx, c, provider, current, o); err != nil {
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
				if serving == 0 {
					serving = time.Since(created)
				}
				servingInstance = str(pool, "status", "activeInstance", "runtimeInstanceID")
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
	succeeded := time.Since(created)
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
	if o.startup == nil || servingInstance != identity["runtimeInstanceID"] || servingBoot != identity["runtimeSessionSupervisorBootID"] {
		return nil, fmt.Errorf("could not tie the Serving runtime to the %s startup evidence", provider)
	}
	fmt.Println("PASS the native supervisor bootstraps, serves and executes a deterministic ACP RuntimeSession prompt")
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
	retired := time.Since(created)
	// fiberd's home outlives its fibers; the per-instance Service must go.
	// Substrate's private worker Pod must go with its instance.
	var gone client.Object = &corev1.Pod{}
	key := client.ObjectKeyFromObject(o.worker)
	if o.service != nil {
		gone, key = &corev1.Service{}, client.ObjectKeyFromObject(o.service)
	}
	if err := wait(ctx, func() (bool, error) {
		err := c.Get(ctx, key, gone)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return err == nil && gone.GetUID() != o.worker.UID && o.service == nil, err
	}); err != nil {
		return nil, fmt.Errorf("retired instance resource %s remains: %w", key, err)
	}
	released := time.Since(created)
	var tokens corev1.SecretList
	if err := c.List(ctx, &tokens, client.InNamespace(namespace), client.MatchingLabels{"workspace.orka.ai/attachment-for": string(workspace.UID)}); err != nil {
		return nil, err
	}
	if len(tokens.Items) != 0 {
		return nil, fmt.Errorf("retired workspace retains %d attachment token Secrets", len(tokens.Items))
	}
	fmt.Println("PASS terminal Task revokes attachment credentials and releases its native instance")
	ms := func(d time.Duration) int64 { return d.Milliseconds() }
	result := map[string]any{"provider": provider, "task": namespace + "/" + task.GetName(), "phase": "Succeeded", "workspace": namespace + "/" + workspaceName,
		"startup": o.startup, "worker": map[string]any{"namespace": o.worker.Namespace, "name": o.worker.Name, "uid": o.worker.UID},
		"challengeReadBeforeCore": o.challengeChecked, "runtimeSession": identity,
		"timingsMS": map[string]int64{"taskToStartupEvidence": ms(o.firstStartup), "taskToServing": ms(serving),
			"taskToSucceeded": ms(succeeded), "taskToWorkspaceRetired": ms(retired), "taskToInstanceReleased": ms(released)},
		"attachmentSecretsRemaining": 0}
	if o.service != nil {
		result["service"] = o.service.Name
	}
	return result, nil
}

func main() {
	runID := flag.String("run-id", "", "isolated resource suffix")
	provider := flag.String("provider", "fiberd", "workspace provider: fiberd or substrate")
	report := flag.String("report", "", "public JSON proof report")
	flag.Parse()
	if *runID == "" || os.Getenv("KUBECONFIG") == "" {
		fmt.Fprintln(os.Stderr, "run-id and a scoped KUBECONFIG are required")
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
	var c client.Client
	if err == nil {
		c, err = client.New(config, client.Options{Scheme: scheme})
	}
	var result map[string]any
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()
		result, err = run(ctx, c, *provider, *runID)
	}
	if result != nil && *report != "" {
		data, _ := json.MarshalIndent(result, "", "  ")
		_ = os.WriteFile(*report, append(data, '\n'), 0o600)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s core proof failed: %v\n", *provider, err)
		os.Exit(1)
	}
	fmt.Printf("%s Core RuntimeSession proof passed.\n", *provider)
}
