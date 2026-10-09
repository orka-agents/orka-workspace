// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"context"
	"reflect"
	"testing"

	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type serviceAccountDefaultClient struct{ client.Client }

func (c *serviceAccountDefaultClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if pod, ok := object.(*corev1.Pod); ok {
		// Kubernetes v1.37 core Pod defaulting, followed by ServiceAccount
		// admission. Embedded CRD PodSpecs do not receive these passes.
		if pod.Spec.ServiceAccountName == "" {
			pod.Spec.ServiceAccountName = pod.Spec.DeprecatedServiceAccount
		}
		pod.Spec.DeprecatedServiceAccount = pod.Spec.ServiceAccountName
		if pod.Spec.ServiceAccountName == "" {
			pod.Spec.ServiceAccountName, pod.Spec.DeprecatedServiceAccount = "default", "default"
		}
	}
	return c.Client.Create(ctx, object, options...)
}

func TestServiceAccountPodAdmissionPreservesExactFakeStartup(t *testing.T) {
	for _, tc := range []struct {
		name, canonical, alias, effective string
	}{
		{"default", "", "", "default"},
		{"alias only", "", "selected", "selected"},
		{"canonical only", "selected", "", "selected"},
		{"canonical wins", "selected", "default", "selected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, request := runtimeFixture(t)
			c := &serviceAccountDefaultClient{Client: base}
			if tc.effective != "default" {
				if err := c.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: request.Runtime.Template.Namespace, Name: tc.effective}}); err != nil {
					t.Fatal(err)
				}
			}
			request.Runtime.Template.Spec.ServiceAccountName = tc.canonical
			request.Runtime.Template.Spec.DeprecatedServiceAccount = tc.alias
			request.Revision, _ = workspaceprovider.WorkloadRevision(request)
			before := request.DeepCopy()
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			created, err := New(c).EnsureAllocation(t.Context(), request)
			if err != nil || created.State != workspaceprovider.AllocationReady || created.Startup == nil || created.Startup.Pod == nil {
				t.Fatalf("API-normalized ServiceAccount rejected: %+v, %v", created, err)
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: created.Startup.Pod.Namespace, Name: created.Startup.Pod.Name}, pod); err != nil {
				t.Fatal(err)
			}
			if pod.Spec.ServiceAccountName != tc.effective || pod.Spec.DeprecatedServiceAccount != tc.effective {
				t.Fatalf("fixture did not realize the effective ServiceAccount: %+v", pod.Spec)
			}
			observed, err := New(c).Observe(t.Context(), request.Key)
			if err != nil || observed.State != workspaceprovider.AllocationReady || observed.Startup == nil || observed.Startup.Pod == nil || *observed.Startup.Pod != *created.Startup.Pod || observed.Identity != created.Identity {
				t.Fatalf("API-normalized ServiceAccount lost its exact fence: %+v, %v", observed, err)
			}
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil || !reflect.DeepEqual(&request, before) || !reflect.DeepEqual(&record.Request, before) {
				t.Fatalf("comparison rewrote frozen ServiceAccount intent: %v", err)
			}
			pod.Spec.ServiceAccountName, pod.Spec.DeprecatedServiceAccount = "different", "different"
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
				t.Fatalf("effective ServiceAccount drift retained startup: %+v, %v", observed, err)
			}
			if base.creates != 1 {
				t.Fatal("normalization replaced the exact Pod lifetime")
			}
		})
	}
}
