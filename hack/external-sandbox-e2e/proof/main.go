// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	osexec "os/exec"
	"slices"
	"strings"
	"time"

	workspace "github.com/orka-agents/orka-workspace/api/v1alpha1"
	provider "github.com/orka-agents/orka-workspace/sdk"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const namespace = "external-sandbox-proof"
const nonceEnv = "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE"

type proof struct {
	client       client.Client
	core         client.Client
	config       *rest.Config
	w            *workspace.ExecutionWorkspace
	checks       []string
	image, runID string
}

func main() {
	image := flag.String("image", "", "digest-pinned persistence listener image")
	runID := flag.String("run-id", "", "isolated resource suffix")
	report := flag.String("report", "", "JSON proof report")
	flag.Parse()
	if *image == "" || *runID == "" || os.Getenv("KUBECONFIG") == "" {
		fmt.Fprintln(os.Stderr, "image, run-id and kindctl-scoped KUBECONFIG are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	p := &proof{image: *image, runID: *runID}
	var err error
	p.config, err = clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	s := runtime.NewScheme()
	if err == nil {
		err = core.AddToScheme(s)
	}
	if err == nil {
		err = workspace.AddToScheme(s)
	}
	if err == nil {
		p.client, err = client.New(p.config, client.Options{Scheme: s})
	}
	if err == nil {
		coreConfig := rest.CopyConfig(p.config)
		coreConfig.Impersonate.UserName = "system:serviceaccount:" + namespace + ":sandbox-proof-core"
		coreConfig.Impersonate.Groups = []string{"system:authenticated", "system:serviceaccounts"}
		p.core, err = client.New(coreConfig, client.Options{Scheme: s})
	}
	var result map[string]any
	if err == nil {
		result, err = p.run(ctx)
	}
	if err != nil {
		if p.w != nil {
			data, _ := json.Marshal(p.w.Status)
			fmt.Fprintln(os.Stderr, "last public workspace status:", string(data))
		}
		fmt.Fprintln(os.Stderr, "Sandbox persistence proof failed:", err)
		os.Exit(1)
	}
	result["checks"] = p.checks
	result["scope"] = "standalone installed-backend persistence; Orka RuntimeSession proof is separate"
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	if *report != "" {
		if err := os.WriteFile(*report, append(data, '\n'), 0600); err != nil {
			panic(err)
		}
	}
	fmt.Printf("Sandbox persistence proof passed (%d checks).\n", len(p.checks))
}

func until(ctx context.Context, check func() (bool, error)) error {
	for {
		ready, err := check()
		if err != nil || ready {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (p *proof) passed(check string) {
	p.checks = append(p.checks, check)
	fmt.Println("PASS", check)
}

func randomNonce() string {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return hex.EncodeToString(data)
}

func (p *proof) update(ctx context.Context, change func(*workspace.ExecutionWorkspace)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &workspace.ExecutionWorkspace{}
		if err := p.client.Get(ctx, client.ObjectKeyFromObject(p.w), current); err != nil {
			return err
		}
		before := current.DeepCopy()
		change(current)
		current.Spec.CoreAdmission.AdmittedGeneration = current.Generation + 1
		if err := p.core.Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		p.w = current
		return p.admitStatus(ctx)
	})
}

func (p *proof) admitStatus(ctx context.Context) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &workspace.ExecutionWorkspace{}
		if err := p.client.Get(ctx, client.ObjectKeyFromObject(p.w), current); err != nil {
			return err
		}
		before := current.DeepCopy()
		provider.SetCondition(&current.Status.Conditions, metav1.Condition{Type: string(workspace.ConditionWorkspaceAdmitted), Status: metav1.ConditionTrue, Reason: string(workspace.ReasonReady), ObservedGeneration: current.Generation})
		if err := p.core.Status().Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		p.w = current
		return nil
	})
}

func (p *proof) waitAllocation(ctx context.Context, state workspace.AllocationState) error {
	return until(ctx, func() (bool, error) {
		if err := p.client.Get(ctx, client.ObjectKeyFromObject(p.w), p.w); err != nil {
			return false, err
		}
		a := p.w.Status.Allocation
		return a != nil && a.Sequence == p.w.Spec.Workload.Sequence && a.State == state && p.w.Status.ObservedGeneration == p.w.Generation, nil
	})
}

func (p *proof) exec(ctx context.Context, pod *core.Pod, command ...string) (string, error) {
	kindctl := os.Getenv("KINDCTL_BIN")
	if kindctl == "" {
		kindctl = "kindctl"
	}
	repo := os.Getenv("ORKA_SANDBOX_E2E_REPO_ROOT")
	if repo == "" {
		return "", errors.New("original repo path is required for scoped kindctl exec")
	}
	args := []string{"kubectl", "--tag", "external-workspace", "exec", "-n", pod.Namespace, pod.Name, "-c", "supervisor", "--"}
	process := osexec.CommandContext(ctx, kindctl, append(args, command...)...)
	process.Dir = repo
	var stdout, stderr bytes.Buffer
	process.Stdout, process.Stderr = &stdout, &stderr
	err := process.Run()
	if err != nil {
		return "", fmt.Errorf("runtime exec failed: %w: %s", err, stderr.String())
	}
	return stdout.String(), nil
}

func (p *proof) readyPod(ctx context.Context) (*core.Pod, error) {
	if err := provider.ValidateStartup(*p.w.Spec.Workload, *p.w.Status.Allocation); err != nil {
		return nil, err
	}
	ref := p.w.Status.Allocation.Startup.Pod
	if ref == nil {
		return nil, errors.New("Sandbox startup omitted its exact Pod reference")
	}
	pod := &core.Pod{}
	if err := until(ctx, func() (bool, error) {
		if err := p.client.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, pod); err != nil {
			return false, err
		}
		if pod.UID != ref.UID || pod.DeletionTimestamp != nil {
			return false, errors.New("startup Pod identity changed")
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == "supervisor" {
				return status.Ready && status.State.Running != nil && status.RestartCount == 0, nil
			}
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	return pod, nil
}

func (p *proof) run(ctx context.Context) (map[string]any, error) {
	registration := &workspace.ExecutionWorkspaceProvider{}
	if err := until(ctx, func() (bool, error) {
		if err := p.client.Get(ctx, client.ObjectKey{Name: "sandbox"}, registration); err != nil {
			return false, err
		}
		return registration.Status.ObservedGeneration == registration.Generation && registration.Status.PinnedParametersUID != "" && registration.Status.LastHeartbeat != nil &&
			slices.Contains(registration.Status.SupportedContracts, workspace.LifecycleContractV1) &&
			slices.Contains(registration.Status.SupportedFeatures, workspace.WorkspaceFeatureACPRuntime) &&
			slices.Contains(registration.Status.SupportedFeatures, workspace.WorkspaceFeatureSuspend), nil
	}); err != nil {
		return nil, fmt.Errorf("provider registration readiness: %w", err)
	}
	p.passed("separate Sandbox provider advertises current installed lifecycle contract")
	name := "persistence-" + p.runID
	profile := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"suspend": map[string]any{"mode": "DataOnly", "volume": map[string]any{"storageClassName": "standard", "accessModes": []any{"ReadWriteOnce"}, "capacity": "16Mi"}}}}}
	profile.SetGroupVersionKind(schema.GroupVersionKind{Group: "sandbox.workspace.orka.ai", Version: "v1alpha1", Kind: "SandboxWorkspaceProfile"})
	profile.SetNamespace(namespace)
	profile.SetName(name)
	if err := p.client.Create(ctx, profile); err != nil {
		return nil, err
	}
	if err := p.client.Get(ctx, client.ObjectKeyFromObject(profile), profile); err != nil {
		return nil, err
	}
	profileHash, err := provider.ParametersProfileHash(profile)
	if err != nil {
		return nil, err
	}
	lifecycle := workspace.ExecutionWorkspaceLifecycle{DefaultOnDetach: workspace.WorkspaceOnDetachDelete, AllowedOnDetach: []workspace.WorkspaceOnDetach{workspace.WorkspaceOnDetachDelete, workspace.WorkspaceOnDetachSuspend},
		DetachTimeout: metav1.Duration{Duration: time.Minute}, MaxLifetime: &metav1.Duration{Duration: time.Hour},
		DeletionPolicy: workspace.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspace.WorkspaceDeletionActionDelete, PersistentVolumes: workspace.WorkspaceDeletionActionDelete, Checkpoints: workspace.WorkspaceDeletionActionDelete}}
	class := &workspace.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: workspace.ExecutionWorkspaceClassSpec{
		ProviderRef: &workspace.ClusterObjectReference{Name: registration.Name}, ParametersRef: &workspace.TypedObjectReference{Group: "sandbox.workspace.orka.ai", Kind: "SandboxWorkspaceProfile", Name: name},
		Mode: workspace.ExecutionWorkspaceModeInteractive, RequiredFeatures: []workspace.ExecutionWorkspaceFeature{workspace.WorkspaceFeatureACPRuntime},
		AllowedReuseScopes: []workspace.WorkspaceReuseScope{workspace.WorkspaceReuseScopeNone, workspace.WorkspaceReuseScopeSession}, Lifecycle: lifecycle}}
	if err := p.client.Create(ctx, class); err != nil {
		return nil, err
	}
	classHash, err := provider.ClassProfileHash(class.Spec)
	if err != nil {
		return nil, err
	}
	before := class.DeepCopy()
	class.Status.ObservedGeneration = class.Generation
	class.Status.ProviderRef = class.Spec.ProviderRef.DeepCopy()
	class.Status.ProfileHash = classHash
	provider.SetCondition(&class.Status.Conditions, metav1.Condition{Type: string(workspace.ConditionClassReady), Status: metav1.ConditionTrue, Reason: string(workspace.ReasonReady), ObservedGeneration: class.Generation})
	if err := p.core.Status().Patch(ctx, class, client.MergeFrom(before)); err != nil {
		return nil, err
	}
	p.passed("separate namespace-scoped fixture identity supplies exact immutable class and provider admission bindings")
	classBinding := workspace.ImmutableObjectBinding{Name: class.Name, UID: class.UID, Generation: class.Generation, ProfileHash: class.Status.ProfileHash}
	providerBinding := workspace.ImmutableObjectBinding{Name: registration.Name, UID: registration.UID, Generation: registration.Generation}
	p.w = &workspace.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: map[string]string{workspace.ProviderControllerLabel: registration.Spec.ControllerName}}, Spec: workspace.ExecutionWorkspaceSpec{
		Mode: workspace.ExecutionWorkspaceModeInteractive, ClassBinding: classBinding, ProviderBinding: providerBinding,
		SessionRef: &workspace.ObjectIdentityReference{Name: "proof-session-" + p.runID, UID: types.UID("proof-session-" + p.runID)},
		Slot:       "default", DesiredState: workspace.ExecutionWorkspaceDesiredReady, Lifecycle: lifecycle,
		CoreAdmission: &workspace.ExecutionWorkspaceCoreAdmission{ClassBinding: classBinding, ProviderBinding: providerBinding, AdmittedGeneration: 1}}}
	if err := p.core.Create(ctx, p.w); err != nil {
		return nil, err
	}
	if err := p.admitStatus(ctx); err != nil {
		return nil, err
	}
	nonce := randomNonce()
	automount := false
	request := workspace.WorkloadRequest{Sequence: 1, Key: workspace.AllocationKey{Namespace: namespace, Name: name, WorkspaceUID: p.w.UID, ProviderUID: registration.UID}, Image: p.image,
		ParametersRef: class.Spec.ParametersRef.DeepCopy(), ParametersBinding: &workspace.ImmutableObjectBinding{Name: name, UID: profile.GetUID(), Generation: profile.GetGeneration(), ProfileHash: profileHash},
		Runtime: &workspace.RuntimeWorkload{BootstrapPort: 8080, Protocol: "orka.harness.v2", ContainerName: "supervisor", ClassBinding: classBinding,
			PoolBinding: workspace.ImmutableObjectBinding{Name: name, UID: types.UID("pool-" + p.runID), Generation: 1, ProfileHash: classBinding.ProfileHash}, RequiredFeatures: []workspace.ExecutionWorkspaceFeature{workspace.WorkspaceFeatureACPRuntime, workspace.WorkspaceFeatureSuspend},
			Template: core.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Labels: map[string]string{"external-sandbox-proof": p.runID}}, Spec: core.PodSpec{
				RestartPolicy: core.RestartPolicyNever, AutomountServiceAccountToken: &automount,
				SecurityContext: &core.PodSecurityContext{RunAsNonRoot: new(true), RunAsUser: new(int64(65532)), RunAsGroup: new(int64(65532)), FSGroup: new(int64(65532))},
				Containers: []core.Container{{Name: "supervisor", Image: p.image, ImagePullPolicy: core.PullNever, Ports: []core.ContainerPort{{ContainerPort: 8080, Protocol: core.ProtocolTCP}},
					SecurityContext: &core.SecurityContext{AllowPrivilegeEscalation: new(false), Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}}},
					VolumeMounts:    []core.VolumeMount{{Name: "orka-workspace", MountPath: "/durable/orka-workspace"}}, Env: []core.EnvVar{{Name: "ORKA_ACP_DURABLE_WORKSPACE_DIR", Value: "/durable/orka-workspace"}, {Name: nonceEnv, Value: nonce}}}},
			}}}}
	request.Revision, err = provider.WorkloadRevision(request)
	if err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := p.update(ctx, func(w *workspace.ExecutionWorkspace) { w.Spec.Workload = &request }); err != nil {
		return nil, err
	}
	if err := p.waitAllocation(ctx, workspace.AllocationReady); err != nil {
		return nil, fmt.Errorf("initial materialization: %w", err)
	}
	first, err := p.readyPod(ctx)
	if err != nil {
		return nil, err
	}
	if len(p.w.Status.Allocation.Startup.PersistentVolumes) != 1 {
		return nil, errors.New("startup must attest one exact durable PVC and PV")
	}
	storage := p.w.Status.Allocation.Startup.PersistentVolumes[0]
	sentinel := "sandbox-persistence-" + p.runID + "-" + randomNonce()
	if _, err := p.exec(ctx, first, "/bin/sh", "-c", "printf '%s' '"+sentinel+"' > /durable/orka-workspace/persistence-proof"); err != nil {
		return nil, err
	}
	p.passed("real runtime Pod writes sentinel data to dynamically provisioned durable PVC")
	oldIdentity := p.w.Status.Allocation.Identity
	if err := p.update(ctx, func(w *workspace.ExecutionWorkspace) {
		w.Spec.DesiredState = workspace.ExecutionWorkspaceDesiredSuspended
		w.Spec.Retirement = &workspace.WorkloadRetirement{Sequence: request.Sequence, Identity: oldIdentity, Action: workspace.WorkloadRetirementSuspend}
	}); err != nil {
		return nil, err
	}
	if err := p.waitAllocation(ctx, workspace.AllocationStopped); err != nil {
		return nil, fmt.Errorf("DataOnly suspension: %w", err)
	}
	if err := p.client.Get(ctx, client.ObjectKeyFromObject(first), &core.Pod{}); !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("suspension did not remove exact Pod: %v", err)
	}
	retained := p.w.Status.Allocation.RetainedData
	if retained == nil || !retained.Valid() || retained.SourceInstance != oldIdentity {
		return nil, errors.New("suspension lost exact retained data provenance")
	}
	p.passed("exact first Pod is absent before suspension reports retained Data lineage")
	request = *request.DeepCopy()
	request.Sequence++
	request.PreviousInstance = &oldIdentity
	request.RetainedData = retained.DeepCopy()
	nextNonce := randomNonce()
	request.Runtime.Template.Spec.Containers[0].Env[1].Value = nextNonce
	request.Revision, err = provider.WorkloadRevision(request)
	if err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := p.update(ctx, func(w *workspace.ExecutionWorkspace) {
		w.Spec.DesiredState = workspace.ExecutionWorkspaceDesiredReady
		w.Spec.Workload = &request
		w.Spec.Retirement = nil
	}); err != nil {
		return nil, err
	}
	if err := p.waitAllocation(ctx, workspace.AllocationReady); err != nil {
		return nil, fmt.Errorf("cold resume: %w", err)
	}
	second, err := p.readyPod(ctx)
	if err != nil {
		return nil, err
	}
	if second.UID == first.UID {
		return nil, errors.New("cold resume reused the old Pod UID")
	}
	actualNonce := ""
	for _, env := range second.Spec.Containers[0].Env {
		if env.Name == nonceEnv {
			actualNonce = env.Value
		}
	}
	if actualNonce != nextNonce || actualNonce == nonce {
		return nil, errors.New("replacement did not carry rotated public bootstrap nonce")
	}
	if len(p.w.Status.Allocation.Startup.PersistentVolumes) != 1 || p.w.Status.Allocation.Startup.PersistentVolumes[0] != storage {
		return nil, errors.New("cold resume changed the attested PVC/PV identities")
	}
	data, err := p.exec(ctx, second, "/bin/cat", "/durable/orka-workspace/persistence-proof")
	if err != nil {
		return nil, fmt.Errorf("cold resume lost persisted data: %w", err)
	}
	if strings.TrimSpace(data) != sentinel {
		return nil, errors.New("cold resume changed the persisted sentinel contents")
	}
	p.passed("fresh Pod UID and rotated public nonce preserve data on the same attested PVC/PV")
	secondIdentity := p.w.Status.Allocation.Identity
	if err := p.update(ctx, func(w *workspace.ExecutionWorkspace) {
		w.Spec.DesiredState = workspace.ExecutionWorkspaceDesiredDeleted
		w.Spec.Retirement = &workspace.WorkloadRetirement{Sequence: request.Sequence, Identity: secondIdentity, Action: workspace.WorkloadRetirementDelete}
	}); err != nil {
		return nil, err
	}
	if err := p.waitAllocation(ctx, workspace.AllocationDeleted); err != nil {
		return nil, fmt.Errorf("provider deletion: %w", err)
	}
	for _, target := range []client.Object{second, &core.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: storage.Claim.Namespace, Name: storage.Claim.Name}}, &core.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: storage.Volume.Name}}} {
		if err := p.client.Get(ctx, client.ObjectKeyFromObject(target), target); !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("native cleanup retained %T: %v", target, err)
		}
	}
	if err := provider.ValidateDeletedDisposition(p.w.Status.Allocation.Disposition, lifecycle.DeletionPolicy); err != nil {
		return nil, err
	}
	p.passed("exact replacement Pod, PVC and backing PV are absent before deletion completes")
	return map[string]any{"workspace": namespace + "/" + name, "image": p.image, "upstreamVersion": "v1.0.3", "firstPodUID": first.UID, "secondPodUID": second.UID, "claimUID": storage.Claim.UID, "volumeUID": storage.Volume.UID, "retainedData": retained}, nil
}
