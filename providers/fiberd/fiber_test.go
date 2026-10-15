// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fiberd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var testClient client.Client

func TestRuntimeConformanceOnMemoryFibers(t *testing.T) {
	for name, check := range map[string]func(context.Context, func() workspaceprovider.Lifecycle, workspaceprovider.WorkloadRequest) error{
		"lifecycle": conformance.Check,
		"replacement": func(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest) error {
			return conformance.CheckReplacement(ctx, factory, request, func(ctx context.Context, next workspaceprovider.WorkloadRequest) error {
				return PublishRequest(ctx, testClient, next)
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, workspacev1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}).Build()
			testClient = c
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			request, err := RuntimeConformanceFixture(ctx, c, ConformanceImage, "conformance-nonce")
			if err != nil {
				t.Fatal(err)
			}
			fibers := NewMemoryFibers(ConformanceImage)
			if err := check(ctx, func() workspaceprovider.Lifecycle { return New(c, fibers) }, request); err != nil {
				t.Fatal(err)
			}
			if fibers.Releases == 0 {
				t.Fatal("no fiber was released")
			}
		})
	}
}

func newTestClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, workspacev1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}).Build()
}

func TestRetirementReleasesFiberWhoseCloneResponseWasLost(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := RuntimeConformanceFixture(ctx, c, ConformanceImage, "n")
	if err != nil {
		t.Fatal(err)
	}
	fibers := NewMemoryFibers(ConformanceImage)
	fibers.LoseClones = 1
	driver := New(c, fibers)
	clock := time.Now()
	driver.now = func() time.Time { return clock }
	pending, err := driver.EnsureAllocation(ctx, request)
	if err == nil {
		t.Fatal("lost clone response was not reported")
	}
	if running, _ := fibers.Running(ctx, pending.Identity.InstanceID); running != 1 {
		t.Fatalf("lost clone left %d fibers running, want 1", running)
	}
	clock = clock.Add(cloneBudget + cloneGrace)
	stopped, err := driver.StopInstance(ctx, request.Key, pending.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != workspaceprovider.AllocationStopped || fibers.Releases != 1 {
		t.Fatalf("retirement left state %s after %d releases", stopped.State, fibers.Releases)
	}
	if running, _ := fibers.Running(ctx, pending.Identity.InstanceID); running != 0 {
		t.Fatalf("retirement left %d fibers running", running)
	}
	if fibers.seq != 1 {
		t.Fatalf("retirement created %d fibers, want 1", fibers.seq)
	}
}

// A Clone already sent when retirement begins may still start a fiber.
// Retirement waits out its journaled deadline, then releases that fiber.
func TestRetirementWaitsOutAnInFlightClone(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := RuntimeConformanceFixture(ctx, c, ConformanceImage, "n")
	if err != nil {
		t.Fatal(err)
	}
	fibers := NewMemoryFibers(ConformanceImage)
	fibers.CloneGate = make(chan struct{})
	driver := New(c, fibers)
	var mu sync.Mutex
	clock := time.Now()
	driver.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	ensured := make(chan error, 1)
	go func() {
		_, err := driver.EnsureAllocation(ctx, request)
		ensured <- err
	}()
	var identity workspaceprovider.InstanceIdentity
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		_, record, err := driver.read(ctx, request.Key)
		if err != nil || record.Fiber == nil || record.Fiber.CloneDeadline.IsZero() {
			return false, nil
		}
		identity = record.Observation.Identity
		return true, nil
	}); err != nil {
		t.Fatalf("clone was never issued: %v", err)
	}
	stopping, err := driver.StopInstance(ctx, request.Key, identity)
	if err != nil {
		t.Fatal(err)
	}
	if stopping.State == workspaceprovider.AllocationStopped {
		t.Fatal("retirement reported Stopped while a sent Clone could still start a fiber")
	}
	close(fibers.CloneGate)
	if err := <-ensured; err != nil {
		t.Fatalf("ensure after retirement began: %v", err)
	}
	if running, _ := fibers.Running(ctx, identity.InstanceID); running != 1 {
		t.Fatalf("the in-flight clone left %d fibers, want 1", running)
	}
	mu.Lock()
	clock = clock.Add(cloneBudget + cloneGrace)
	mu.Unlock()
	stopped, err := driver.StopInstance(ctx, request.Key, identity)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != workspaceprovider.AllocationStopped || fibers.Releases != 1 || fibers.seq != 1 {
		t.Fatalf("retirement left state %s after %d releases and %d clones", stopped.State, fibers.Releases, fibers.seq)
	}
	if running, _ := fibers.Running(ctx, identity.InstanceID); running != 0 {
		t.Fatalf("retirement left %d fibers running", running)
	}
}

// Retirement can finish between Ensure recording its fiber and creating the
// fiber's Service. That Service must not outlive the instance.
func TestRetirementRemovesAServiceCreatedAfterIt(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := RuntimeConformanceFixture(ctx, c, ConformanceImage, "n")
	if err != nil {
		t.Fatal(err)
	}
	fibers := NewMemoryFibers(ConformanceImage)
	driver := New(c, fibers)
	reached, hold := make(chan struct{}), make(chan struct{})
	driver.beforeServiceCreate = func() {
		close(reached)
		<-hold
	}
	ensured := make(chan error, 1)
	go func() {
		_, err := driver.EnsureAllocation(ctx, request)
		ensured <- err
	}()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("Ensure never reached Service creation")
	}
	_, record, err := driver.read(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := driver.StopInstance(ctx, request.Key, record.Observation.Identity)
	if err != nil || stopped.State != workspaceprovider.AllocationStopped {
		t.Fatalf("retirement before the Service existed: %v %v", stopped.State, err)
	}
	driver.beforeServiceCreate = nil
	close(hold)
	if err := <-ensured; err != nil {
		t.Fatalf("ensure after retirement: %v", err)
	}
	services := &corev1.ServiceList{}
	if err := c.List(ctx, services); err != nil {
		t.Fatal(err)
	}
	if len(services.Items) != 0 {
		t.Fatalf("a Service created after retirement remains: %s", services.Items[0].Name)
	}
}

func TestObserveWithdrawsEvidenceWhenServiceIsGone(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := RuntimeConformanceFixture(ctx, c, ConformanceImage, "n")
	if err != nil {
		t.Fatal(err)
	}
	fibers := NewMemoryFibers(ConformanceImage)
	driver := New(c, fibers)
	ready, err := driver.EnsureAllocation(ctx, request)
	if err != nil || ready.State != workspaceprovider.AllocationReady {
		t.Fatalf("allocation not ready: %v %v", ready.State, err)
	}
	services := &corev1.ServiceList{}
	if err := c.List(ctx, services); err != nil || len(services.Items) != 1 {
		t.Fatalf("want one fiber Service: %v", err)
	}
	if err := c.Delete(ctx, &services.Items[0]); err != nil {
		t.Fatal(err)
	}
	if observed, err := driver.Observe(ctx, request.Key); !errors.Is(err, workspaceprovider.ErrStaleIdentity) || observed.Startup != nil {
		t.Fatalf("observation without its Service: %v %v", observed.Startup, err)
	}
}

func TestFiberRequestRejectsUnservedImageAndPodOnlyShapes(t *testing.T) {
	c := newTestClient(t)
	request, err := RuntimeConformanceFixture(context.Background(), c, ConformanceImage, "n")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFiberRequest(request, []string{"other@sha256:" + ConformanceImage[len(ConformanceImage)-64:]}, nil); err == nil {
		t.Fatal("unserved image was admitted")
	}
	pod := *request.DeepCopy()
	pod.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime}
	if err := validateFiberRequest(pod, []string{ConformanceImage}, nil); err == nil {
		t.Fatal("Pod-materialized request was admitted")
	}
	downward := *request.DeepCopy()
	downward.Runtime.Template.Spec.Containers[0].Env = append(downward.Runtime.Template.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "X", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}})
	if err := validateFiberRequest(downward, []string{ConformanceImage}, nil); err == nil {
		t.Fatal("downward environment was admitted")
	}
	privileged := *request.DeepCopy()
	privileged.Runtime.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"SETUID", "SYS_ADMIN"}}}
	if err := validateFiberRequest(privileged, []string{ConformanceImage}, []string{"CAP_SETUID"}); err == nil {
		t.Fatal("capability outside the home's set was admitted")
	}
	privileged.Runtime.Template.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"SETUID"}
	if err := validateFiberRequest(privileged, []string{ConformanceImage}, []string{"CAP_SETUID"}); err != nil {
		t.Fatalf("capability inside the home's set was refused: %v", err)
	}
	coreCaps := []string{"CAP_CHOWN", "CAP_KILL", "CAP_SETGID", "CAP_SETUID"}
	dropped := *request.DeepCopy()
	dropped.Runtime.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
		Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN", "KILL", "SETGID", "SETUID"}}}
	if err := validateFiberRequest(dropped, []string{ConformanceImage}, coreCaps); err != nil {
		t.Fatalf("Core's drop-ALL-then-add shape was refused: %v", err)
	}
	dropped.Runtime.Template.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"SETUID"}
	if err := validateFiberRequest(dropped, []string{ConformanceImage}, coreCaps); err == nil {
		t.Fatal("dropping capabilities every sandbox holds was admitted")
	}
	dropped.Runtime.Template.Spec.Containers[0].SecurityContext.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"KILL"}}
	if err := validateFiberRequest(dropped, []string{ConformanceImage}, coreCaps); err == nil {
		t.Fatal("a named drop of a held capability was admitted")
	}
	nonRoot := *request.DeepCopy()
	nonRoot.Runtime.Template.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: new(true)}
	nonRoot.Runtime.Template.Spec.Containers[0].SecurityContext = nil
	if err := validateFiberRequest(nonRoot, []string{ConformanceImage}, nil); err == nil {
		t.Fatal("runAsNonRoot was admitted")
	}
	nonRoot.Runtime.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsNonRoot: new(false)}
	if err := validateFiberRequest(nonRoot, []string{ConformanceImage}, nil); err != nil {
		t.Fatalf("container override of runAsNonRoot was refused: %v", err)
	}
	workdir := *request.DeepCopy()
	workdir.Runtime.Template.Spec.Containers[0].WorkingDir = "/workspace"
	if err := validateFiberRequest(workdir, []string{ConformanceImage}, nil); err == nil {
		t.Fatal("a working directory the template cannot apply was admitted")
	}
	scratch := *request.DeepCopy()
	scratch.Runtime.Template.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	scratch.Runtime.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}
	if err := validateFiberRequest(scratch, []string{ConformanceImage}, nil); err == nil {
		t.Fatal("emptyDir mount the template cannot provide was admitted")
	}
}
