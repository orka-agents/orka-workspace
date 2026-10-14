// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fiberd

import (
	"context"
	"testing"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

func TestFiberRequestRejectsUnservedImageAndPodOnlyShapes(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = workspacev1alpha1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}).Build()
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
}
