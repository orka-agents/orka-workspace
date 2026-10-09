// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"reflect"
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
)

func TestSandboxImagePullPolicyDefaultsPreserveFrozenIntent(t *testing.T) {
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
			c, request := fixture(t, false)
			request.Image = test.repository + "@sha256:" + strings.Repeat("2", 64)
			request.Runtime.Template.Spec.Containers[0].Image = request.Image
			request.Runtime.Template.Spec.Containers[0].ImagePullPolicy = test.requested
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(t, c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			frozen := request.Runtime.Template.DeepCopy()
			pod := materializedPod(t, c, request)
			// Model the independently verified API default without calling the
			// provider normalizer. Explicit policies retain their requested value.
			pod.Spec.Containers[0].ImagePullPolicy = test.realized
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			observed := ready(t, c, request)
			if observed.Startup == nil || observed.Startup.Pod == nil || observed.Startup.Pod.UID != pod.UID {
				t.Fatal("realized pull policy lost exact startup")
			}
			if after, err := New(c).Observe(t.Context(), request.Key); err != nil || after.State != sdk.AllocationReady || after.Startup == nil || *after.Startup.Pod != *observed.Startup.Pod {
				t.Fatalf("realized pull policy was not observable: %+v, %v", after, err)
			}
			if !reflect.DeepEqual(*frozen, request.Runtime.Template) {
				t.Fatal("pull-policy comparison mutated the frozen template")
			}
			pod.Spec.Containers[0].ImagePullPolicy = corev1.PullNever
			if test.realized == corev1.PullNever {
				pod.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
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
		})
	}
}
