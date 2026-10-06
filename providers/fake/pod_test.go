package fake

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type podClient struct {
	client.Client
	t           *testing.T
	creates     int
	loseCreate  bool
	unavailable bool
}

func (c *podClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	pod, ok := object.(*corev1.Pod)
	if !ok {
		return c.Client.Create(ctx, object, options...)
	}
	c.creates++
	// Assert the public behavior needed for restart safety at the backend seam.
	journals := &corev1.ConfigMapList{}
	if err := c.Client.List(ctx, journals); err != nil {
		return err
	}
	found := false
	for _, cm := range journals.Items {
		var record journalRecord
		if json.Unmarshal([]byte(cm.Data[journalDataKey]), &record) == nil && record.Pod != nil && record.Pod.Name == pod.Name && record.CreateIssued {
			found = true
		}
	}
	if !found {
		c.t.Fatal("Pod creation preceded durable intent")
	}
	pod.UID = types.UID("uid-" + pod.Name)
	if err := c.Client.Create(ctx, pod, options...); err != nil {
		return err
	}
	pod.Status.PodIP = "10.0.0.8"
	pod.Status.Phase = corev1.PodPending
	// Deliberately no PodReady condition: it cannot be a bootstrap prerequisite.
	if err := c.Client.Status().Update(ctx, pod); err != nil {
		return err
	}
	if c.loseCreate {
		c.loseCreate = false
		return errors.New("lost Pod creation response")
	}
	return nil
}
func (c *podClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*corev1.Pod); ok && c.unavailable {
		return errors.New("Pod API unavailable")
	}
	return c.Client.Get(ctx, key, object, options...)
}
func runtimeFixture(t *testing.T) (*podClient, workspaceprovider.WorkloadRequest) {
	t.Helper()
	base, request := fixture(t)
	c := &podClient{Client: base, t: t}
	workspace := workspaceFor(t, c, request.Key)
	disabled := false
	request.Command = []string{"/fixture-supervisor"}
	request.Runtime = &workspaceprovider.RuntimeWorkload{
		PoolBinding:  workspacev1alpha1.ImmutableObjectBinding{Name: "pool", UID: "pool-uid", Generation: 1, ProfileHash: request.Image[len("fixture.invalid/status-only@"):]},
		ClassBinding: workspace.Spec.ClassBinding,
		Protocol:     "orka.harness.v2", ContainerName: "supervisor", BootstrapPort: 8080,
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Labels: map[string]string{"fixture": "supervisor"}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &disabled, Containers: []corev1.Container{{Name: "supervisor", Image: request.Image, Command: request.Command, Args: request.Args, Resources: request.Resources, Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}}},
	}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	return c, request
}
func publishRequest(ctx context.Context, c client.Client, request workspaceprovider.WorkloadRequest) error {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}, workspace); err != nil {
		return err
	}
	workspace.Spec.Workload = &request
	workspace.Generation++
	workspace.Spec.CoreAdmission.AdmittedGeneration = workspace.Generation
	conditions := workspace.Status.Conditions
	for i := range conditions {
		if conditions[i].Type == string(workspacev1alpha1.ConditionWorkspaceAdmitted) {
			conditions[i].ObservedGeneration = workspace.Generation
		}
	}
	if err := c.Update(ctx, workspace); err != nil {
		return err
	}
	workspace.Status.Conditions = conditions
	return c.Status().Update(ctx, workspace)
}
func TestRealPodConformanceAndBootstrapReadiness(t *testing.T) {
	c, request := runtimeFixture(t)
	if err := conformance.Check(t.Context(), func() workspaceprovider.Lifecycle { return New(c) }, request); err != nil {
		t.Fatal(err)
	}
	if c.creates != 1 {
		t.Fatalf("created %d Pods", c.creates)
	}
}
func TestPodCreateLostResponseRecoversExactUID(t *testing.T) {
	c, request := runtimeFixture(t)
	c.loseCreate = true
	if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("expected lost response")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	identity := record.Observation.Identity
	observed, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Identity != identity || observed.Startup == nil || observed.Startup.Pod == nil || observed.Startup.Pod.UID == "" || c.creates != 1 {
		t.Fatal("retry replaced the Pod or its exact identity")
	}
}

func TestPodCreateLostResponseThenDisappearanceRefusesReplay(t *testing.T) {
	c, request := runtimeFixture(t)
	c.loseCreate = true
	if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("expected lost response")
	}
	_, original, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if !original.CreateIssued || original.Pod == nil || original.Pod.UID != "" {
		t.Fatal("lost response did not leave a durable unresolved Pod creation")
	}
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: original.Pod.Namespace, Name: original.Pod.Name}
	if err := c.Get(t.Context(), key, pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := New(c).EnsureAllocation(t.Context(), request); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
			t.Fatalf("unresolved creation was replayed: %v", err)
		}
	}
	if c.creates != 1 {
		t.Fatalf("replayed unresolved creation %d times", c.creates-1)
	}
	_, current, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if current.Observation.Identity != original.Observation.Identity || !current.CreateIssued || current.Pod.UID != "" {
		t.Fatal("retry changed the unresolved instance fence")
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, original.Observation.Identity); err == nil {
		t.Fatal("unresolved creation was reported terminated")
	}
}
func TestStopRecoversUIDAfterLostCreateResponse(t *testing.T) {
	c, request := runtimeFixture(t)
	c.loseCreate = true
	if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("expected lost response")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, record.Observation.Identity); err != nil {
		t.Fatal(err)
	}
	stopped, err := New(c).StopInstance(t.Context(), request.Key, record.Observation.Identity)
	if err != nil || stopped.State != workspaceprovider.AllocationStopped {
		t.Fatalf("lost create cleanup = %q, %v", stopped.State, err)
	}
}
func TestPodDisappearanceDoesNotRecreateOrClaimTermination(t *testing.T) {
	c, request := runtimeFixture(t)
	ready, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: ready.Startup.Pod.Namespace, Name: ready.Startup.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	for _, read := range []func() (workspaceprovider.AllocationObservation, error){func() (workspaceprovider.AllocationObservation, error) {
		return New(c).Observe(t.Context(), request.Key)
	}, func() (workspaceprovider.AllocationObservation, error) {
		return New(c).EnsureAllocation(t.Context(), request)
	}} {
		observed, err := read()
		if err != nil {
			t.Fatal(err)
		}
		if observed.State != workspaceprovider.AllocationPending || observed.Startup != nil || c.creates != 1 {
			t.Fatal("missing Pod recreated or misreported as ready/stopped")
		}
	}
}
func TestPodMutationAndUIDReplacementAreRejected(t *testing.T) {
	for _, mutation := range []string{"image", "uid", "restart"} {
		t.Run(mutation, func(t *testing.T) {
			c, request := runtimeFixture(t)
			ready, err := New(c).EnsureAllocation(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: ready.Startup.Pod.Namespace, Name: ready.Startup.Pod.Name}, pod); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "image":
				pod.Spec.Containers[0].Image = "foreign"
				err = c.Update(t.Context(), pod)
			case "uid":
				pod.UID = "foreign"
				err = c.Update(t.Context(), pod)
			case "restart":
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "supervisor", RestartCount: 1}}
				err = c.Status().Update(t.Context(), pod)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := New(c).Observe(t.Context(), request.Key); err == nil {
				t.Fatal("mutated Pod was accepted")
			}
			if mutation == "uid" {
				if _, err := New(c).StopInstance(t.Context(), request.Key, ready.Identity); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
					t.Fatalf("foreign Pod stop = %v", err)
				}
			}
		})
	}
}

type priorityPodClient struct {
	*podClient
	admit func(*corev1.PodSpec)
}

func (c *priorityPodClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if pod, ok := object.(*corev1.Pod); ok {
		c.admit(&pod.Spec)
	}
	return c.podClient.Create(ctx, object, options...)
}

func TestPodPriorityAdmissionPreservesExplicitTemplateFields(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*corev1.PodSpec)
		admit     func(*corev1.PodSpec)
		valid     bool
	}{
		{
			name: "global default priority class", valid: true,
			configure: func(*corev1.PodSpec) {},
			admit: func(spec *corev1.PodSpec) {
				spec.PriorityClassName = "global-default"
				spec.Priority = new(int32(700))
				spec.PreemptionPolicy = new(corev1.PreemptNever)
			},
		},
		{
			name: "named priority class", valid: true,
			configure: func(spec *corev1.PodSpec) { spec.PriorityClassName = "workspace-priority" },
			admit: func(spec *corev1.PodSpec) {
				spec.Priority = new(int32(700))
				spec.PreemptionPolicy = new(corev1.PreemptNever)
			},
		},
		{
			name: "explicit priority and preemption", valid: true,
			configure: func(spec *corev1.PodSpec) {
				spec.PriorityClassName = "workspace-priority"
				spec.Priority = new(int32(700))
				spec.PreemptionPolicy = new(corev1.PreemptNever)
			},
			admit: func(*corev1.PodSpec) {},
		},
		{
			name:      "changed explicit priority",
			configure: func(spec *corev1.PodSpec) { spec.Priority = new(int32(700)) },
			admit:     func(spec *corev1.PodSpec) { spec.Priority = new(int32(701)) },
		},
		{
			name:      "changed explicit preemption policy",
			configure: func(spec *corev1.PodSpec) { spec.PreemptionPolicy = new(corev1.PreemptNever) },
			admit:     func(spec *corev1.PodSpec) { spec.PreemptionPolicy = new(corev1.PreemptLowerPriority) },
		},
		{
			name:      "changed explicit priority class",
			configure: func(spec *corev1.PodSpec) { spec.PriorityClassName = "workspace-priority" },
			admit:     func(spec *corev1.PodSpec) { spec.PriorityClassName = "foreign-priority" },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, request := runtimeFixture(t)
			tc.configure(&request.Runtime.Template.Spec)
			var err error
			request.Revision, err = workspaceprovider.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := publishRequest(t.Context(), base, request); err != nil {
				t.Fatal(err)
			}
			c := &priorityPodClient{podClient: base, admit: tc.admit}
			ready, err := New(c).EnsureAllocation(t.Context(), request)
			if !tc.valid {
				if err == nil {
					t.Fatal("changed explicitly frozen scheduling field was accepted")
				}
				return
			}
			if err != nil || ready.State != workspaceprovider.AllocationReady || ready.Startup == nil || ready.Startup.Pod == nil {
				t.Fatalf("admission-derived scheduling fields rejected: %v", err)
			}
			observed, err := New(c).Observe(t.Context(), request.Key)
			if err != nil || observed.Identity != ready.Identity || observed.Startup == nil || *observed.Startup.Pod != *ready.Startup.Pod || c.creates != 1 {
				t.Fatalf("scheduling normalization changed exact runtime identity: %v", err)
			}
			revision, err := workspaceprovider.WorkloadRevision(request)
			if err != nil || revision != request.Revision {
				t.Fatalf("scheduling normalization changed admitted request: %v", err)
			}
		})
	}
}

func TestProviderOutageCannotProveTermination(t *testing.T) {
	c, request := runtimeFixture(t)
	ready, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	c.unavailable = true
	if _, err := New(c).Observe(t.Context(), request.Key); err == nil {
		t.Fatal("outage produced observation")
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, ready.Identity); err == nil {
		t.Fatal("outage proved termination")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Observation.State != workspaceprovider.AllocationPending || record.Observation.Startup != nil {
		t.Fatal("retirement outage retained readiness")
	}
	if _, err := New(c).DeleteAllocation(t.Context(), request.Key, ready.Identity, deletionPolicy()); !errors.Is(err, workspaceprovider.ErrInstanceRunning) {
		t.Fatalf("deleted on outage: %v", err)
	}
}
func TestAttachmentAcknowledgementDoesNotWaitForPodCreation(t *testing.T) {
	c, request := runtimeFixture(t)
	workspace := workspaceFor(t, c, request.Key)
	class := &workspacev1alpha1.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: workspace.Namespace, Name: workspace.Spec.ClassBinding.Name, UID: workspace.Spec.ClassBinding.UID}}
	if err := c.Create(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{Epoch: 5}
	workspace.Spec.AttachmentEpoch = 5
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	c.unavailable = true
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err == nil {
		t.Fatal("expected Pod API outage")
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateAttached || workspace.Status.AttachedEpoch != 5 || !workspaceprovider.ConditionIsTrue(workspace.Status.Conditions, string(workspacev1alpha1.ConditionWorkspaceAttached)) {
		t.Fatal("runtime outage prevented attachment acknowledgement")
	}
	if workspaceprovider.ConditionIsTrue(workspace.Status.Conditions, string(workspacev1alpha1.ConditionWorkspaceDataPlaneReady)) {
		t.Fatal("runtime outage advertised readiness")
	}
}

func TestDeletionCanCleanAnExactPodAfterTemplateMutation(t *testing.T) {
	c, request := runtimeFixture(t)
	ready, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: ready.Startup.Pod.Namespace, Name: ready.Startup.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Containers[0].Image = "foreign"
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	workspace := workspaceFor(t, c, request.Key)
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: ready.Identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
	workspace.Spec.Lifecycle.DeletionPolicy = deletionPolicy()
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
			t.Fatal(err)
		}
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateDeleted || workspace.Status.Allocation.State != workspaceprovider.AllocationDeleted {
		t.Fatal("Pod verification failure stranded exact cleanup")
	}
}

func TestDesiredDeletionWaitsForExactCoreRetirementAuthorization(t *testing.T) {
	c, request := runtimeFixture(t)
	ready, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	workspace := workspaceFor(t, c, request.Key)
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Lifecycle.DeletionPolicy = deletionPolicy()
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	reconcile := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}
	for _, identity := range []*workspaceprovider.InstanceIdentity{nil, {AllocationID: ready.Identity.AllocationID, InstanceID: "foreign", RequestRevision: ready.Identity.RequestRevision}} {
		workspace = workspaceFor(t, c, request.Key)
		if identity != nil {
			workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: *identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
		}
		if err := c.Update(t.Context(), workspace); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(t.Context(), reconcile); err != nil {
			t.Fatal(err)
		}
		pod := &corev1.Pod{}
		if err := c.Get(t.Context(), types.NamespacedName{Namespace: ready.Startup.Pod.Namespace, Name: ready.Startup.Pod.Name}, pod); err != nil {
			t.Fatalf("retired before core drain authorization: %v", err)
		}
		workspace = workspaceFor(t, c, request.Key)
		if workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateDeleted {
			t.Fatal("unproven retirement released cleanup")
		}
	}
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: ready.Identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), reconcile); err != nil {
			t.Fatal(err)
		}
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateDeleted {
		t.Fatal("authorized exact retirement did not complete")
	}
}

func TestMissingJournalBeforeStatusCannotRecreateOrClaimDeletion(t *testing.T) {
	c, request := runtimeFixture(t)
	ready, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	workspace := workspaceFor(t, c, request.Key)
	if workspace.Status.Allocation != nil {
		t.Fatal("fixture unexpectedly published provider status")
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("lost journal recreated an allocation")
	}
	if _, err := New(c).Observe(t.Context(), request.Key); err == nil || err == workspaceprovider.ErrNotFound {
		t.Fatal("lost journal reported no allocation")
	}
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: ready.Identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err == nil {
		t.Fatal("lost journal did not block cleanup")
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateDeleted || workspace.Status.Disposition != nil {
		t.Fatal("lost journal manufactured termination")
	}
}

func TestPodAdmissionInjectedImagePullSecretsAreRejected(t *testing.T) {
	base, request := runtimeFixture(t)
	c := &priorityPodClient{podClient: base, admit: func(spec *corev1.PodSpec) {
		spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "admission-injected-credential"}}
	}}
	observed, err := New(c).EnsureAllocation(t.Context(), request)
	if err == nil || observed.Startup != nil {
		t.Fatalf("credential-bearing Pod was accepted: %v", err)
	}
	if c.creates != 1 {
		t.Fatalf("Pod create attempts = %d", c.creates)
	}
}
