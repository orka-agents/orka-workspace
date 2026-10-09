// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"slices"
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNativeRejectsActiveDeadlineBeforeAllocationEffects(t *testing.T) {
	for _, test := range []struct {
		name    string
		seconds int64
	}{{"zero", 0}, {"one second", 1}, {"positive lifetime", 3600}} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			request.Runtime.Template.Spec.ActiveDeadlineSeconds = &test.seconds
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, "active deadline is unsupported")
		})
	}
}

func TestNativeRejectsUnusableBootstrapNonceBeforeAllocationEffects(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []corev1.EnvVar
	}{
		{name: "missing"},
		{name: "empty", env: []corev1.EnvVar{{Name: bootstrapNonceEnv}}},
		{name: "downward field", env: []corev1.EnvVar{{Name: bootstrapNonceEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}}}},
		{name: "literal and downward field", env: []corev1.EnvVar{{Name: bootstrapNonceEnv, Value: "literal", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}}}},
		{name: "variable expansion", env: []corev1.EnvVar{{Name: bootstrapNonceEnv, Value: "$(EARLIER)"}}},
		{name: "escape expansion", env: []corev1.EnvVar{{Name: bootstrapNonceEnv, Value: "$$literal"}}},
		{name: "duplicate", env: []corev1.EnvVar{{Name: bootstrapNonceEnv, Value: "first"}, {Name: bootstrapNonceEnv, Value: "second"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			container := &request.Runtime.Template.Spec.Containers[0]
			container.Env = slices.DeleteFunc(container.Env, func(env corev1.EnvVar) bool { return env.Name == bootstrapNonceEnv })
			container.Env = append(container.Env, test.env...)
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, "bootstrap nonce must be a unique nonempty literal")
		})
	}
}

func TestNativePreservesLiteralBootstrapNonce(t *testing.T) {
	c, native, request := fixture(t, false)
	const nonce = "public-nonce-$literal"
	for i := range request.Runtime.Template.Spec.Containers[0].Env {
		env := &request.Runtime.Template.Spec.Containers[0].Env[i]
		if env.Name == bootstrapNonceEnv {
			env.Value = nonce
		}
	}
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	observed := ready(t, c, native, request)
	if observed.Startup == nil || observed.Startup.Process == nil || observed.Startup.Process.ChallengeSHA256 == "" {
		t.Fatal("literal bootstrap nonce did not produce sealed process evidence")
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range record.TemplateSpec.Containers[0].Env {
		if env.Name == bootstrapNonceEnv {
			if env.Value != nonce {
				t.Fatal("native compiler changed the admitted literal bootstrap nonce")
			}
			return
		}
	}
	t.Fatal("native compiler lost the admitted bootstrap nonce")
}

func assertNativeRequestRejectedBeforeEffects(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest, expected string) {
	t.Helper()
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("fixture does not reach the native compiler: %v", err)
	}
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("unsupported native intent was not rejected: %v", err)
	}
	if native.boots != 0 || len(native.actors) != 0 || len(native.workers) != 0 || len(native.tags) != 0 || len(native.templates) != 1 {
		t.Fatal("rejected intent created native resources")
	}
	configMaps, pods, policies := &corev1.ConfigMapList{}, &corev1.PodList{}, &networkingv1.NetworkPolicyList{}
	for _, list := range []client.ObjectList{configMaps, pods, policies} {
		if err := c.List(t.Context(), list); err != nil {
			t.Fatal(err)
		}
	}
	if len(configMaps.Items) != 0 || len(pods.Items) != 0 || len(policies.Items) != 0 {
		t.Fatal("rejected intent created a journal, anchor, worker, or network policy")
	}
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1alpha1")
	pools.SetKind("WorkerPoolList")
	if err := c.List(t.Context(), pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 1 || pools.Items[0].GetUID() != "worker-pool-uid" {
		t.Fatal("rejected intent created or replaced a native runtime pool")
	}
}
