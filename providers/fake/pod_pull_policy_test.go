// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"context"
	"reflect"
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type pullPolicyAdmissionClient struct {
	client.Client
	policy corev1.PullPolicy
}

func (c *pullPolicyAdmissionClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if pod, ok := object.(*corev1.Pod); ok {
		// The test supplies the independently verified Kubernetes default for
		// each image. Admission preserves explicitly requested policies.
		for _, containers := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
			for i := range containers {
				if containers[i].ImagePullPolicy == "" {
					containers[i].ImagePullPolicy = c.policy
				}
			}
		}
	}
	return c.Client.Create(ctx, object, options...)
}

func TestFakeImagePullPolicyDefaultsPreserveFrozenIntent(t *testing.T) {
	for _, test := range []struct {
		name, repository    string
		requested, realized corev1.PullPolicy
	}{
		{"digest only", "example.invalid/runtime", "", corev1.PullIfNotPresent},
		{"version tag and digest", "example.invalid/runtime:v1", "", corev1.PullIfNotPresent},
		{"latest tag and digest", "example.invalid/runtime:latest", "", corev1.PullAlways},
		{"registry port and latest path", "registry.example:5000/team/latest", "", corev1.PullIfNotPresent},
		{"registry port and latest tag", "registry.example:5000/team/runtime:latest", "", corev1.PullAlways},
		{"explicit IfNotPresent", "example.invalid/runtime:latest", corev1.PullIfNotPresent, corev1.PullIfNotPresent},
		{"explicit Always", "example.invalid/runtime", corev1.PullAlways, corev1.PullAlways},
		{"explicit Never", "example.invalid/runtime:latest", corev1.PullNever, corev1.PullNever},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, request := runtimeFixture(t)
			c := &pullPolicyAdmissionClient{Client: base, policy: test.realized}
			request.Image = test.repository + "@sha256:" + strings.Repeat("2", 64)
			request.Runtime.Template.Spec.Containers[0].Image = request.Image
			request.Runtime.Template.Spec.Containers[0].ImagePullPolicy = test.requested
			request.Runtime.Template.Spec.InitContainers = []corev1.Container{{Name: "setup", Image: request.Image, ImagePullPolicy: test.requested}}
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			frozen := request.Runtime.Template.DeepCopy()
			observed, err := New(c).EnsureAllocation(t.Context(), request)
			if err != nil || observed.State != sdk.AllocationReady || observed.Startup == nil || observed.Startup.Pod == nil {
				t.Fatalf("realized pull policy blocked startup: %+v, %v", observed, err)
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: observed.Startup.Pod.Namespace, Name: observed.Startup.Pod.Name}, pod); err != nil {
				t.Fatal(err)
			}
			if pod.Spec.Containers[0].ImagePullPolicy != test.realized || pod.Spec.InitContainers[0].ImagePullPolicy != test.realized {
				t.Fatal("fixture did not realize the API pull policy")
			}
			if after, err := New(c).Observe(t.Context(), request.Key); err != nil || after.Startup == nil || *after.Startup.Pod != *observed.Startup.Pod {
				t.Fatalf("realized pull policy lost exact startup: %+v, %v", after, err)
			}
			if !reflect.DeepEqual(*frozen, request.Runtime.Template) {
				t.Fatal("pull-policy comparison mutated the frozen template")
			}
			// Both regular and init containers retain policy drift detection.
			for _, containers := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
				containers[0].ImagePullPolicy = corev1.PullNever
				if test.realized == corev1.PullNever {
					containers[0].ImagePullPolicy = corev1.PullAlways
				}
				if err := c.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				if after, err := New(c).EnsureAllocation(t.Context(), request); err == nil || after.Startup != nil {
					t.Fatalf("changed pull policy published startup: %+v, %v", after, err)
				}
				if after, err := New(c).Observe(t.Context(), request.Key); err == nil || after.Startup != nil {
					t.Fatalf("changed pull policy remained observable: %+v, %v", after, err)
				}
				containers[0].ImagePullPolicy = test.realized
				if err := c.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
