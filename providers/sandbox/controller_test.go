package sandbox

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/sandbox/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestProviderAdvertisementPinsConfigAndSupportedContracts(t *testing.T) {
	c, _ := fixture(t, false)
	config := &profilev1alpha1.SandboxProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "sandbox", UID: "config"}}
	if err := c.Create(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "sandbox"}, provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.ParametersRef = workspacev1alpha1.TypedObjectReference{Group: profilev1alpha1.GroupVersion.Group, Kind: "SandboxProviderConfig", Name: config.Name}
	if err := c.Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	r := &ProviderReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(provider), provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.PinnedParametersUID != "config" || !slices.Contains(provider.Status.SupportedContracts, workspaceprovider.LifecycleContractV1) || !slices.Contains(provider.Status.SupportedFeatures, workspacev1alpha1.WorkspaceFeatureSuspend) {
		t.Fatal("provider did not publish its implemented lifecycle contract")
	}
	config.UID = "replacement"
	if err := c.Update(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(provider), provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.PinnedParametersUID != "config" || len(provider.Status.SupportedContracts) != 0 || provider.Status.LastHeartbeat != nil {
		t.Fatal("same-name replacement config was repinned or advertised")
	}
}

func servedBackend() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{
		{GroupVersion: "agents.x-k8s.io/v1beta1", APIResources: []metav1.APIResource{{Name: "sandboxes"}}},
		{GroupVersion: "extensions.agents.x-k8s.io/v1beta1", APIResources: []metav1.APIResource{{Name: "sandboxclaims"}, {Name: "sandboxtemplates"}, {Name: "sandboxwarmpools"}}},
	}
}

func TestProviderAdvertisementRequiresServedBackendAPIs(t *testing.T) {
	c, _ := fixture(t, false)
	config := &profilev1alpha1.SandboxProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "sandbox", UID: "config"}}
	if err := c.Create(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "sandbox"}, provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.ParametersRef = workspacev1alpha1.TypedObjectReference{Group: profilev1alpha1.GroupVersion.Group, Kind: "SandboxProviderConfig", Name: config.Name}
	if err := c.Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	backend := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: servedBackend()}}
	r := &ProviderReconciler{Client: c, Discovery: backend}
	advertised := func(t *testing.T) bool {
		t.Helper()
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(provider), provider); err != nil {
			t.Fatal(err)
		}
		return provider.Status.LastHeartbeat != nil && len(provider.Status.SupportedFeatures) != 0
	}
	reconcile := func(t *testing.T) {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t)
	if !advertised(t) {
		t.Fatal("served backend was not advertised")
	}
	without := func(name string) []*metav1.APIResourceList {
		lists := servedBackend()
		for _, list := range lists {
			list.APIResources = slices.DeleteFunc(list.APIResources, func(resource metav1.APIResource) bool { return resource.Name == name })
		}
		return lists
	}
	cases := map[string][]*metav1.APIResourceList{"missing group version": servedBackend()[:1]}
	for _, group := range backendResources {
		for _, name := range group {
			cases["missing "+name] = without(name)
		}
	}
	for name, resources := range cases {
		backend.Resources = resources
		reconcile(t)
		if advertised(t) || provider.Status.Backend != nil {
			t.Fatalf("%s: provider advertised an unserved backend", name)
		}
		backend.Resources = servedBackend()
		reconcile(t)
		if !advertised(t) {
			t.Fatalf("%s: restored backend was not advertised", name)
		}
	}
	heartbeat := provider.Status.LastHeartbeat.DeepCopy()
	backend.PrependReactor("*", "*", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("discovery unavailable")
	})
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err == nil {
		t.Fatal("transient discovery failure was hidden")
	}
	if !advertised(t) || !provider.Status.LastHeartbeat.Equal(heartbeat) {
		t.Fatal("transient discovery failure withdrew the advertisement")
	}
	// A definitively invalid configuration is withdrawn even while discovery fails.
	if err := c.Delete(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	reconcile(t)
	if advertised(t) {
		t.Fatal("transient discovery failure preserved an invalid configuration's advertisement")
	}
}

// groupDiscovery answers each group version independently.
type groupDiscovery struct {
	discovery.ServerResourcesInterface
	errors map[string]error
}

func (d groupDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	if err := d.errors[groupVersion]; err != nil {
		return nil, err
	}
	for _, list := range servedBackend() {
		if list.GroupVersion == groupVersion {
			return list, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{}, groupVersion)
}

func TestBackendAbsenceWinsOverTransientDiscoveryError(t *testing.T) {
	for _, missing := range []string{"agents.x-k8s.io/v1beta1", "extensions.agents.x-k8s.io/v1beta1"} {
		errs := map[string]error{missing: apierrors.NewNotFound(schema.GroupResource{}, missing)}
		for groupVersion := range backendResources {
			if groupVersion != missing {
				errs[groupVersion] = errors.New("discovery unavailable")
			}
		}
		r := &ProviderReconciler{Discovery: groupDiscovery{errors: errs}}
		// Map iteration order varies; the definitive absence must win every time.
		for range 20 {
			if served, err := r.backendServed(); served || err != nil {
				t.Fatalf("missing %s with a transient sibling error: served=%v err=%v", missing, served, err)
			}
		}
	}
	errs := map[string]error{"agents.x-k8s.io/v1beta1": errors.New("discovery unavailable")}
	if served, err := (&ProviderReconciler{Discovery: groupDiscovery{errors: errs}}).backendServed(); served || err == nil {
		t.Fatalf("transient discovery error without absence was not reported: served=%v err=%v", served, err)
	}
}

func TestRetirementWaitsForExactCoreAuthorization(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	r := &ExecutionWorkspaceReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := New(c).sandbox(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	if sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning || record.Operation != "ensure" {
		t.Fatal("desiredState alone suspended the runtime before core drain")
	}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: first.Identity, Action: workspacev1alpha1.WorkloadRetirementSuspend}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	sb, err = New(c).sandbox(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	if sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
		t.Fatal("matching core retirement did not suspend")
	}
}

func TestAttachmentAcknowledgementDoesNotWaitForRuntime(t *testing.T) {
	c, request := fixture(t, false)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{Epoch: 7}
	workspace.Spec.AttachmentEpoch = 7
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := (&ExecutionWorkspaceReconciler{Client: c}).Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	if workspace.Status.AttachedEpoch != 7 || workspace.Status.Allocation == nil || workspace.Status.Allocation.State != workspaceprovider.AllocationPending {
		t.Fatal("attachment acknowledgement waited for a runtime Pod")
	}
}

func TestExplicitStopDuringReadyDoesNotRequireReadmission(t *testing.T) {
	c, request := fixture(t, false)
	first := ready(t, c, request)
	key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: 1, Identity: first.Identity, Action: workspacev1alpha1.WorkloadRetirementStop}
	workspace.Generation++
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := (&ExecutionWorkspaceReconciler{Client: c}).Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Operation != "stop" {
		t.Fatal("ready-state retirement waited for admission or started another instance")
	}
}

func TestExplicitSuspendDuringReadyPreservesDataForRollover(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: 1, Identity: first.Identity, Action: workspacev1alpha1.WorkloadRetirementSuspend}
	workspace.Generation++
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := (&ExecutionWorkspaceReconciler{Client: c}).Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Operation != "suspend" {
		t.Fatal("ready-state suspend did not preserve data for the next sequence")
	}
}

func TestReadySuspendAuthorizationObservedAfterDeadlineStops(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	notAfter := metav1.NewTime(time.Now().Add(-time.Hour))
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: 1, Identity: first.Identity, Action: workspacev1alpha1.WorkloadRetirementSuspend, NotAfter: &notAfter}
	workspace.Generation++
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	r := &ExecutionWorkspaceReconciler{Client: c}
	for range 12 {
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if err := nativeTick(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), key, workspace); err != nil {
			t.Fatal(err)
		}
		if workspace.Status.Allocation != nil && workspace.Status.Allocation.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil || record.Operation != "stop" {
		t.Fatalf("late authorization started a suspension: %v", err)
	}
	allocation := workspace.Status.Allocation
	condition := workspaceprovider.FindCondition(workspace.Status.Conditions, string(workspacev1alpha1.ConditionWorkspaceDataPlaneReady))
	if allocation == nil || allocation.State != workspaceprovider.AllocationStopped || allocation.RetainedData != nil || allocation.Identity != first.Identity ||
		condition == nil || condition.Reason != string(workspacev1alpha1.ReasonSuspendDeadlineExceeded) || workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspended {
		t.Fatalf("late authorization was not stopped without data and reported: %+v %+v state=%s", allocation, condition, workspace.Status.State)
	}
}

// lostSandboxSuspend fails the first Sandbox update, as if the provider
// stopped after journaling a suspension but before issuing it.
type lostSandboxSuspend struct {
	client.Client
	failed bool
}

func (c *lostSandboxSuspend) Update(ctx context.Context, object client.Object, options ...client.UpdateOption) error {
	if _, ok := object.(*sandboxv1beta1.Sandbox); ok && !c.failed {
		c.failed = true
		return errors.New("provider stopped before issuing the suspension")
	}
	return c.Client.Update(ctx, object, options...)
}

func TestJournaledSuspensionIsNotIssuedAfterDeadline(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	window := metav1.NewTime(time.Now().Add(time.Hour))
	if _, err := New(&lostSandboxSuspend{Client: c}).SuspendInstance(t.Context(), request.Key, first.Identity, &window); err == nil {
		t.Fatal("lost Sandbox update succeeded")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil || record.Operation != "suspend" {
		t.Fatalf("suspension intent was not journaled: %v", err)
	}
	window = metav1.NewTime(time.Now().Add(-time.Hour))
	if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity, &window); !errors.Is(err, workspaceprovider.ErrSuspendDeadlineExceeded) {
		t.Fatalf("journaled suspension was issued after its deadline: %v", err)
	}
	sb, err := New(c).sandbox(t.Context(), record)
	if err != nil || sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning {
		t.Fatalf("late suspension changed the Sandbox: %v", err)
	}
	var stopped workspaceprovider.AllocationObservation
	for range 12 {
		stopped, err = (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	if stopped.State != workspaceprovider.AllocationStopped || stopped.Identity != first.Identity || stopped.RetainedData != nil {
		t.Fatalf("abandoned suspension blocked exact Stop or claimed retained data: %+v", stopped)
	}
	if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity, &window); !errors.Is(err, workspaceprovider.ErrSuspendDeadlineExceeded) {
		t.Fatalf("stopped instance accepted a late suspension: %v", err)
	}
}
