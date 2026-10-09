// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"reflect"
	"strings"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSandboxRejectsUnsupportedTemplateMetadataBeforeAllocation(t *testing.T) {
	for name, mutate := range map[string]func(*metav1.ObjectMeta){
		"name":                  func(m *metav1.ObjectMeta) { m.Name = "requested-pod" },
		"generate name":         func(m *metav1.ObjectMeta) { m.GenerateName = "requested-" },
		"UID":                   func(m *metav1.ObjectMeta) { m.UID = "requested-uid" },
		"resource version":      func(m *metav1.ObjectMeta) { m.ResourceVersion = "123" },
		"generation":            func(m *metav1.ObjectMeta) { m.Generation = 2 },
		"self link":             func(m *metav1.ObjectMeta) { m.SelfLink = "/requested-pod" },
		"creation timestamp":    func(m *metav1.ObjectMeta) { m.CreationTimestamp = metav1.NewTime(time.Unix(1, 0)) },
		"deletion timestamp":    func(m *metav1.ObjectMeta) { m.DeletionTimestamp = new(metav1.NewTime(time.Unix(1, 0))) },
		"deletion grace period": func(m *metav1.ObjectMeta) { m.DeletionGracePeriodSeconds = new(int64(30)) },
		"owner references": func(m *metav1.ObjectMeta) {
			m.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "external-owner", UID: "external-owner"}}
		},
		"finalizers": func(m *metav1.ObjectMeta) { m.Finalizers = []string{"example.test/cleanup"} },
		"managed fields": func(m *metav1.ObjectMeta) {
			m.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "external-manager", Operation: metav1.ManagedFieldsOperationApply, APIVersion: "v1"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, request := fixture(t, true)
			mutate(&request.Runtime.Template.ObjectMeta)
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(t, c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			before := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: request.Key.Namespace, Name: request.Key.Name}, before); err != nil {
				t.Fatal(err)
			}
			frozen := request.Runtime.Template.DeepCopy()
			for range 2 {
				if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "metadata supports only") || observed.Startup != nil {
					t.Fatalf("unsupported metadata reached allocation: %+v, %v", observed, err)
				}
			}
			requireNoSandboxAllocation(t, c, request)
			after := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(before), after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(*frozen, request.Runtime.Template) {
				t.Fatal("metadata rejection changed the workspace or frozen template")
			}
		})
	}
}

func TestSandboxSupportedTemplateMetadataPreservesStartup(t *testing.T) {
	c, request := fixture(t, false)
	request.Runtime.Template.Labels["example.test/runtime"] = "admitted"
	request.Runtime.Template.Annotations = map[string]string{"example.test/revision": "one"}
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	frozen := request.Runtime.Template.DeepCopy()
	observed := ready(t, c, request)
	if observed.Startup == nil || observed.Startup.Pod == nil {
		t.Fatal("supported metadata lost startup evidence")
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: observed.Startup.Pod.Namespace, Name: observed.Startup.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Namespace != frozen.Namespace || pod.Labels["example.test/runtime"] != "admitted" || pod.Annotations["example.test/revision"] != "one" {
		t.Fatal("supported metadata was not carried to the exact runtime Pod")
	}
	if after, err := New(c).Observe(t.Context(), request.Key); err != nil || after.State != sdk.AllocationReady || after.Startup == nil || *after.Startup.Pod != *observed.Startup.Pod {
		t.Fatalf("supported metadata did not preserve exact startup: %+v, %v", after, err)
	}
	if !reflect.DeepEqual(*frozen, request.Runtime.Template) {
		t.Fatal("metadata validation changed the frozen template")
	}
}

func TestLegacySandboxUnsupportedMetadataWithdrawsStartupButAllowsExactCleanup(t *testing.T) {
	c, request := fixture(t, false)
	ready(t, c, request)
	d := New(c)
	cm, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	// A pre-fix journal could freeze a finalizer that the native Pod never
	// received. Keep that historical allocation identity for exact cleanup.
	record.Request.Runtime.Template.Finalizers = []string{"example.test/cleanup"}
	record.Request.Revision, err = sdk.WorkloadRevision(record.Request)
	if err != nil {
		t.Fatal(err)
	}
	record.Observation.Identity.RequestRevision = record.Request.Revision
	record.Observation.Startup.Identity = record.Observation.Identity
	if err := admit(t, c)(t.Context(), record.Request); err != nil {
		t.Fatal(err)
	}
	if err := d.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	before := cm.DeepCopy()
	if observed, err := d.Observe(t.Context(), request.Key); err == nil || !strings.Contains(err.Error(), "metadata supports only") || observed.Startup != nil {
		t.Fatalf("legacy dropped metadata retained startup: %+v, %v", observed, err)
	}
	if observed, err := d.EnsureAllocation(t.Context(), record.Request); err == nil || observed.Startup != nil {
		t.Fatalf("legacy dropped metadata was re-admitted: %+v, %v", observed, err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(cm), cm); err != nil || !reflect.DeepEqual(before, cm) {
		t.Fatalf("rejected legacy attestation mutated the journal: %v", err)
	}
	identity := record.Observation.Identity
	stopped := false
	for range 5 {
		observed, err := (&simulatedLifecycle{d}).StopInstance(t.Context(), request.Key, identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationStopped && observed.Identity == identity {
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("legacy metadata blocked exact stop")
	}
	for range 8 {
		observed, err := (&simulatedLifecycle{d}).DeleteAllocation(t.Context(), request.Key, identity, cleanupPolicy())
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationDeleted && observed.Identity == identity {
			if err := sdk.ValidateDeletedDisposition(observed.Disposition, cleanupPolicy()); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("legacy metadata blocked exact deletion")
}
