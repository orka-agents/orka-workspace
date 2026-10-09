// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
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

const namespace = "external-workspace-proof"

type proof struct {
	admin, core, untrusted, provider client.Client
	checks                           []string
	workspace                        *workspacev1alpha1.ExecutionWorkspace
	image, runID                     string
}

func main() {
	image := flag.String("image", "", "digest-pinned credential-free listener image")
	runID := flag.String("run-id", "", "isolated resource suffix")
	report := flag.String("report", "", "JSON result file")
	flag.Parse()
	if *image == "" || *runID == "" || os.Getenv("KUBECONFIG") == "" {
		fmt.Fprintln(os.Stderr, "image, run-id, and kindctl-scoped KUBECONFIG are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	p, err := newProof(*image, *runID)
	if err == nil {
		err = p.run(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "external workspace proof failed:", err)
		os.Exit(1)
	}
	result := map[string]any{"checks": p.checks, "workspace": namespace + "/" + p.workspace.Name, "allocation": p.workspace.Status.Allocation, "runtimeSessionProof": "separate core phase"}
	if *report != "" {
		data, marshalErr := json.MarshalIndent(result, "", "  ")
		if marshalErr != nil || os.WriteFile(*report, append(data, '\n'), 0600) != nil {
			fmt.Fprintln(os.Stderr, "could not save proof report")
			os.Exit(1)
		}
	}
	fmt.Printf("Standalone provider proof passed (%d checks). RuntimeSession execution is checked in the separate core phase.\n", len(p.checks))
}

func newProof(image, runID string) (*proof, error) {
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, coordinationv1.AddToScheme, admissionv1.AddToScheme, workspacev1alpha1.AddToScheme} {
		if err := register(scheme); err != nil {
			return nil, err
		}
	}
	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return nil, err
	}
	makeClient := func(user string) (client.Client, error) {
		copy := rest.CopyConfig(config)
		if user != "" {
			copy.Impersonate.UserName = user
			copy.Impersonate.Groups = []string{"system:authenticated", "system:serviceaccounts"}
		}
		return client.New(copy, client.Options{Scheme: scheme})
	}
	p := &proof{image: image, runID: runID}
	for _, entry := range []struct {
		target *client.Client
		user   string
	}{
		{&p.admin, ""},
		{&p.core, "system:serviceaccount:" + namespace + ":proof-core"},
		{&p.untrusted, "system:serviceaccount:" + namespace + ":proof-untrusted"},
		{&p.provider, "system:serviceaccount:orka-workspace-system:orka-workspace-fake"},
	} {
		*entry.target, err = makeClient(entry.user)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *proof) passed(check string) {
	p.checks = append(p.checks, check)
	fmt.Println("PASS", check)
}

func until(ctx context.Context, check func() (bool, error)) error {
	for {
		done, err := check()
		if err != nil || done {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func denied(err error) error {
	// CEL validations with reason Forbidden report 403; validations that use
	// Kubernetes' default reason Invalid report 422. Both must name this policy.
	if err == nil || !(apierrors.IsForbidden(err) || apierrors.IsInvalid(err)) || !strings.Contains(err.Error(), "workspace-field-ownership.orka.ai") {
		return fmt.Errorf("expected ownership-policy rejection, got %v", err)
	}
	return nil
}

func (p *proof) run(ctx context.Context) error {
	policy := &admissionv1.ValidatingAdmissionPolicy{}
	if err := p.admin.Get(ctx, client.ObjectKey{Name: "workspace-field-ownership.orka.ai"}, policy); err != nil {
		return err
	}
	if policy.Status.TypeChecking != nil && len(policy.Status.TypeChecking.ExpressionWarnings) != 0 {
		return fmt.Errorf("ownership policy has %d expression warnings", len(policy.Status.TypeChecking.ExpressionWarnings))
	}
	p.passed("ownership policy compiles without expression warnings")

	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := until(ctx, func() (bool, error) {
		if err := p.admin.Get(ctx, client.ObjectKey{Name: "fake"}, provider); err != nil {
			return false, err
		}
		return provider.Status.ObservedGeneration == provider.Generation && provider.Status.LastHeartbeat != nil && len(provider.Status.SupportedContracts) > 1, nil
	}); err != nil {
		return fmt.Errorf("provider advertisement: %w", err)
	}
	p.passed("separate provider ServiceAccount publishes current contract advertisement")
	beforeProvider := provider.DeepCopy()
	provider.Status.ObservedGeneration += 100
	if err := denied(p.untrusted.Status().Patch(ctx, provider, client.MergeFrom(beforeProvider))); err != nil {
		return err
	}
	p.passed("ServiceAccount with HTTP status-write permission but no virtual verb is denied")

	foreign := &workspacev1alpha1.ExecutionWorkspaceProvider{ObjectMeta: metav1.ObjectMeta{Name: "proof-foreign-" + p.runID}, Spec: provider.Spec}
	foreign.Spec.ControllerName = "foreign.workspace.orka.ai"
	if err := p.core.Create(ctx, foreign); err != nil {
		return err
	}
	beforeForeign := foreign.DeepCopy()
	foreign.Status.ObservedGeneration = 1
	if err := denied(p.provider.Status().Patch(ctx, foreign, client.MergeFrom(beforeForeign))); err != nil {
		return err
	}
	p.passed("provider cannot publish status for another registration")

	lease := &coordinationv1.Lease{}
	if err := until(ctx, func() (bool, error) {
		if err := p.admin.Get(ctx, client.ObjectKey{Namespace: "orka-workspace-system", Name: "orka-workspace-fake.workspace.orka.ai"}, lease); err != nil {
			return false, err
		}
		var pods corev1.PodList
		if err := p.admin.List(ctx, &pods, client.InNamespace("orka-workspace-system"), client.MatchingLabels{"app": "orka-workspace-fake"}); err != nil {
			return false, err
		}
		ready := 0
		for _, pod := range pods.Items {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue && pod.DeletionTimestamp == nil {
					ready++
				}
			}
		}
		return ready == 2 && lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "", nil
	}); err != nil {
		return err
	}
	p.passed("two healthy provider replicas share one leader-election Lease holder")

	params := &unstructured.Unstructured{}
	params.SetGroupVersionKind(schema.GroupVersionKind{Group: "fake.workspace.orka.ai", Version: "v1alpha1", Kind: "FakePoolParameters"})
	params.SetNamespace(namespace)
	params.SetName("proof-" + p.runID)
	params.Object["spec"] = map[string]any{}
	if err := p.core.Create(ctx, params); err != nil {
		return err
	}
	lifecycle := workspacev1alpha1.ExecutionWorkspaceLifecycle{DefaultOnDetach: workspacev1alpha1.WorkspaceOnDetachDelete, AllowedOnDetach: []workspacev1alpha1.WorkspaceOnDetach{workspacev1alpha1.WorkspaceOnDetachDelete}, DetachTimeout: metav1.Duration{Duration: time.Minute}, DeletionPolicy: workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}}
	class := &workspacev1alpha1.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "proof-" + p.runID}, Spec: workspacev1alpha1.ExecutionWorkspaceClassSpec{ProviderRef: &workspacev1alpha1.ClusterObjectReference{Name: "fake"}, ParametersRef: &workspacev1alpha1.TypedObjectReference{Group: "fake.workspace.orka.ai", Kind: "FakePoolParameters", Name: params.GetName()}, Mode: workspacev1alpha1.ExecutionWorkspaceModeInteractive, RequiredFeatures: []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime}, AllowedReuseScopes: []workspacev1alpha1.WorkspaceReuseScope{workspacev1alpha1.WorkspaceReuseScopeNone}, Lifecycle: lifecycle}}
	if err := p.core.Create(ctx, class); err != nil {
		return err
	}
	hash, err := workspaceprovider.ClassProfileHash(class.Spec)
	if err != nil {
		return err
	}
	classBinding := workspacev1alpha1.ImmutableObjectBinding{Name: class.Name, UID: class.UID, Generation: class.Generation, ProfileHash: hash}
	providerBinding := workspacev1alpha1.ImmutableObjectBinding{Name: "fake", UID: provider.UID, Generation: provider.Generation}
	p.workspace = &workspacev1alpha1.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "proof-" + p.runID, Labels: map[string]string{workspacev1alpha1.ProviderControllerLabel: "fake.workspace.orka.ai"}, Finalizers: []string{"workspace.orka.ai/finalizer"}}, Spec: workspacev1alpha1.ExecutionWorkspaceSpec{Mode: workspacev1alpha1.ExecutionWorkspaceModeInteractive, ClassBinding: classBinding, ProviderBinding: providerBinding, DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady, Slot: "default", Lifecycle: lifecycle, CoreAdmission: &workspacev1alpha1.ExecutionWorkspaceCoreAdmission{ClassBinding: classBinding, ProviderBinding: providerBinding, AdmittedGeneration: 1}, AttachmentEpoch: 1, Attachment: &workspacev1alpha1.ExecutionWorkspaceAttachment{TaskRef: workspacev1alpha1.ObjectIdentityReference{Name: "proof-task", UID: "proof-task"}, Epoch: 1, TokenSHA256: "sha256:" + strings.Repeat("0", 64), TokenSecretRef: workspacev1alpha1.SecretReference{Name: "unissued-proof-token"}, ExpiresAt: metav1.NewTime(time.Now().Add(time.Hour))}}}
	if err := p.core.Create(ctx, p.workspace); err != nil {
		return err
	}
	if err := p.admitStatus(ctx); err != nil {
		return err
	}
	if err := until(ctx, func() (bool, error) {
		if err := p.admin.Get(ctx, client.ObjectKeyFromObject(p.workspace), p.workspace); err != nil {
			return false, err
		}
		return p.workspace.Status.AttachedEpoch == 1 && p.workspace.Status.Allocation == nil, nil
	}); err != nil {
		return err
	}
	p.passed("attachment acknowledged before workload creation or runtime bootstrap")
	if err := p.ownershipChecks(ctx); err != nil {
		return err
	}

	automount := false
	request := workspacev1alpha1.WorkloadRequest{Sequence: 1, Key: workspacev1alpha1.AllocationKey{Namespace: namespace, Name: p.workspace.Name, WorkspaceUID: p.workspace.UID, ProviderUID: provider.UID}, Image: p.image, Runtime: &workspacev1alpha1.RuntimeWorkload{BootstrapPort: 8080, Protocol: "orka.harness.v2", ContainerName: "supervisor", ClassBinding: classBinding, PoolBinding: workspacev1alpha1.ImmutableObjectBinding{Name: "proof-runtime-pool", UID: types.UID("runtime-" + string(p.workspace.UID)), Generation: 1, ProfileHash: hash}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"external-workspace-proof": p.runID}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &automount, Containers: []corev1.Container{{Name: "supervisor", Image: p.image, ImagePullPolicy: corev1.PullNever, Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}}}}}}}}
	request.Revision, err = workspaceprovider.WorkloadRevision(request)
	if err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if err := p.updateSpec(ctx, func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Workload = &request
		w.Spec.CoreAdmission.AdmittedGeneration = w.Generation + 1
	}); err != nil {
		return err
	}
	if err := p.admitStatus(ctx); err != nil {
		return err
	}
	if err := until(ctx, func() (bool, error) {
		if err := p.admin.Get(ctx, client.ObjectKeyFromObject(p.workspace), p.workspace); err != nil {
			return false, err
		}
		observed := p.workspace.Status.Allocation
		return observed != nil && observed.State == workspaceprovider.AllocationReady && observed.Startup != nil, nil
	}); err != nil {
		return fmt.Errorf("physical Pod readiness: %w", err)
	}
	observed := *p.workspace.Status.Allocation
	if err := workspaceprovider.ValidateStartup(request, observed); err != nil {
		return err
	}
	pod := &corev1.Pod{}
	if err := p.admin.Get(ctx, client.ObjectKey{Namespace: observed.Startup.Pod.Namespace, Name: observed.Startup.Pod.Name}, pod); err != nil {
		return err
	}
	if pod.UID != observed.Startup.Pod.UID || pod.Spec.Containers[0].Image != p.image || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		return errors.New("reported Pod did not match its frozen request or exact UID")
	}
	if err := until(ctx, func() (bool, error) {
		if err := p.admin.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
			return false, err
		}
		if pod.UID != observed.Startup.Pod.UID {
			return false, errors.New("provider Pod was replaced before the listener started")
		}
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == "supervisor" && container.Ready && container.State.Running != nil && container.RestartCount == 0 {
				return pod.Status.Phase == corev1.PodRunning, nil
			}
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("actual credential-free listener process: %w", err)
	}
	p.passed("provider creates a running credential-free listener Pod and reports its exact UID and request revision")
	stale := observed
	stale.Identity.InstanceID += "-stale"
	if !errors.Is(workspaceprovider.ValidateStartup(request, stale), workspaceprovider.ErrStaleIdentity) {
		return errors.New("stale startup identity was accepted")
	}
	p.passed("consumer startup validation rejects mismatched instance identity")

	if err := p.updateSpec(ctx, func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
		w.Spec.Attachment = nil
	}); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	if err := p.admin.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil || pod.DeletionTimestamp != nil {
		return errors.New("desired deletion terminated the Pod without exact retirement authorization")
	}
	p.passed("desired deletion alone cannot terminate the physical instance")
	if err := p.admin.Get(ctx, client.ObjectKeyFromObject(p.workspace), p.workspace); err != nil {
		return err
	}
	if err := p.updateSpec(ctx, func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: observed.Identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
	}); err != nil {
		return err
	}
	if err := until(ctx, func() (bool, error) {
		if err := p.admin.Get(ctx, client.ObjectKeyFromObject(p.workspace), p.workspace); err != nil {
			return false, err
		}
		allocation := p.workspace.Status.Allocation
		return allocation != nil && allocation.State == workspaceprovider.AllocationDeleted && p.workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateDeleted && allocation.Disposition != nil, nil
	}); err != nil {
		return fmt.Errorf("authorized exact cleanup: %w", err)
	}
	if err := p.admin.Get(ctx, client.ObjectKeyFromObject(pod), pod); !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleted instance still has a Pod: %v", err)
	}
	if err := workspaceprovider.ValidateDeletedDisposition(p.workspace.Status.Disposition, lifecycle.DeletionPolicy); err != nil {
		return err
	}
	p.passed("matching core retirement stops the exact Pod and publishes verified deletion disposition")
	return nil
}

// The provider writes status concurrently. Re-read the exact workspace on a
// bounded conflict retry so the fixture does not mistake a status race for a
// lifecycle failure or overwrite another writer's conditions.
func (p *proof) updateWorkspace(ctx context.Context, status bool, mutate func(*workspacev1alpha1.ExecutionWorkspace)) error {
	key, uid := client.ObjectKeyFromObject(p.workspace), p.workspace.UID
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &workspacev1alpha1.ExecutionWorkspace{}
		if err := p.core.Get(ctx, key, current); err != nil {
			return err
		}
		if current.UID != uid {
			return errors.New("proof workspace UID changed")
		}
		before := current.DeepCopy()
		mutate(current)
		patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
		var err error
		if status {
			err = p.core.Status().Patch(ctx, current, patch)
		} else {
			err = p.core.Patch(ctx, current, patch)
		}
		if err == nil {
			p.workspace = current
		}
		return err
	})
}

func (p *proof) updateSpec(ctx context.Context, mutate func(*workspacev1alpha1.ExecutionWorkspace)) error {
	return p.updateWorkspace(ctx, false, mutate)
}

func (p *proof) admitStatus(ctx context.Context) error {
	return p.updateWorkspace(ctx, true, func(w *workspacev1alpha1.ExecutionWorkspace) {
		workspaceprovider.SetCondition(&w.Status.Conditions, metav1.Condition{Type: string(workspacev1alpha1.ConditionWorkspaceAdmitted), Status: metav1.ConditionTrue, Reason: string(workspacev1alpha1.ReasonReady), ObservedGeneration: w.Generation})
	})
}

func (p *proof) ownershipChecks(ctx context.Context) error {
	for _, check := range []struct {
		name   string
		client client.Client
		status bool
		mutate func(*workspacev1alpha1.ExecutionWorkspace)
	}{
		{"untrusted identity cannot forge core admission", p.untrusted, false, func(w *workspacev1alpha1.ExecutionWorkspace) { w.Spec.CoreAdmission.AdmittedGeneration++ }},
		{"untrusted identity cannot alter attachment intent", p.untrusted, false, func(w *workspacev1alpha1.ExecutionWorkspace) { w.Spec.Attachment = nil }},
		{"untrusted identity cannot remove core finalizers", p.untrusted, false, func(w *workspacev1alpha1.ExecutionWorkspace) { w.Finalizers = nil }},
		{"untrusted identity cannot overwrite core conditions", p.untrusted, true, func(w *workspacev1alpha1.ExecutionWorkspace) { w.Status.Conditions[0].Status = metav1.ConditionFalse }},
		{"provider identity cannot overwrite core conditions", p.provider, true, func(w *workspacev1alpha1.ExecutionWorkspace) { w.Status.Conditions[0].Status = metav1.ConditionFalse }},
		{"untrusted identity cannot swap controller routing", p.untrusted, false, func(w *workspacev1alpha1.ExecutionWorkspace) {
			w.Labels[workspacev1alpha1.ProviderControllerLabel] = "foreign.workspace.orka.ai"
		}},
	} {
		before := p.workspace.DeepCopy()
		current := before.DeepCopy()
		check.mutate(current)
		var err error
		if check.status {
			err = check.client.Status().Patch(ctx, current, client.MergeFrom(before))
		} else {
			err = check.client.Patch(ctx, current, client.MergeFrom(before))
		}
		if err := denied(err); err != nil {
			return fmt.Errorf("%s: %w", check.name, err)
		}
		p.passed(check.name)
	}
	return nil
}
