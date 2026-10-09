package fake

import (
	"testing"

	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodTolerationAdmissionPreservesDeclaredNoExecute(t *testing.T) {
	defaultToleration := func(key string) corev1.Toleration {
		return corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(300))}
	}
	declared := corev1.Toleration{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(60))}
	for _, tc := range []struct {
		name      string
		declared  []corev1.Toleration
		admitted  []corev1.Toleration
		wantReady bool
	}{
		{name: "default injection without declared tolerations", admitted: []corev1.Toleration{defaultToleration(corev1.TaintNodeNotReady), defaultToleration(corev1.TaintNodeUnreachable)}, wantReady: true},
		{name: "default injection for an undeclared taint", declared: []corev1.Toleration{declared}, admitted: []corev1.Toleration{defaultToleration(corev1.TaintNodeUnreachable)}, wantReady: true},
		{name: "webhook broadens a declared taint", declared: []corev1.Toleration{declared}, admitted: []corev1.Toleration{defaultToleration(corev1.TaintNodeNotReady)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, request := runtimeFixture(t)
			request.Runtime.Template.Spec.Tolerations = tc.declared
			var err error
			request.Revision, err = workspaceprovider.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := publishRequest(t.Context(), base, request); err != nil {
				t.Fatal(err)
			}
			c := &priorityPodClient{podClient: base, admit: func(spec *corev1.PodSpec) {
				spec.Tolerations = append(spec.Tolerations, tc.admitted...)
			}}
			ready, err := New(c).EnsureAllocation(t.Context(), request)
			if !tc.wantReady {
				if err == nil {
					t.Fatal("broadened NoExecute toleration was accepted")
				}
				return
			}
			if err != nil || ready.State != workspaceprovider.AllocationReady {
				t.Fatalf("default toleration injection rejected: state=%q err=%v", ready.State, err)
			}
		})
	}
}

func TestPodTemplateRejectsUnsupportedMetadata(t *testing.T) {
	for name, mutate := range map[string]func(*metav1.ObjectMeta){
		"finalizers": func(meta *metav1.ObjectMeta) { meta.Finalizers = []string{"example.com/hold"} },
		"ownerReferences": func(meta *metav1.ObjectMeta) {
			meta.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "owner", UID: "owner-uid"}}
		},
		"name":         func(meta *metav1.ObjectMeta) { meta.Name = "requested" },
		"generateName": func(meta *metav1.ObjectMeta) { meta.GenerateName = "requested-" },
	} {
		t.Run(name, func(t *testing.T) {
			c, request := runtimeFixture(t)
			mutate(&request.Runtime.Template.ObjectMeta)
			var err error
			request.Revision, err = workspaceprovider.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("unsupported template metadata was accepted")
			}
			if c.creates != 0 {
				t.Fatal("unsupported template metadata created a Pod")
			}
			journals := &corev1.ConfigMapList{}
			if err := c.List(t.Context(), journals); err != nil {
				t.Fatal(err)
			}
			if len(journals.Items) != 0 {
				t.Fatal("unsupported template metadata created a journal")
			}
		})
	}
}
