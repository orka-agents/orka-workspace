package sandbox

import (
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
)

func TestTolerationAdmissionPreservesFrozenIntent(t *testing.T) {
	for _, key := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		for _, test := range []struct {
			name                         string
			frozen, realized             bool
			frozenSeconds, actualSeconds *int64
			allowed                      bool
		}{
			{"explicit 10 matching", true, true, new(int64(10)), new(int64(10)), true},
			{"explicit 10 changed to 300", true, true, new(int64(10)), new(int64(300)), false},
			{"explicit 300 matching", true, true, new(int64(300)), new(int64(300)), true},
			{"explicit 300 missing", true, false, new(int64(300)), nil, false},
			{"default 300 injected", false, true, nil, new(int64(300)), true},
			{"nondefault 10 injected", false, true, nil, new(int64(10)), false},
			{"indefinite injected", false, true, nil, nil, false},
		} {
			t.Run(key+"/"+test.name, func(t *testing.T) {
				c, request := fixture(t, false)
				if test.frozen {
					request.Runtime.Template.Spec.Tolerations = []corev1.Toleration{{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: test.frozenSeconds}}
				}
				var err error
				request.Revision, err = sdk.WorkloadRevision(request)
				if err != nil {
					t.Fatal(err)
				}
				if err := admit(t, c)(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				pod := materializedPod(t, c, request)
				pod.Spec.Tolerations = nil
				if test.realized {
					pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: test.actualSeconds})
				}
				otherKey := corev1.TaintNodeUnreachable
				if key == otherKey {
					otherKey = corev1.TaintNodeNotReady
				}
				pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Key: otherKey, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(300))})
				if err := c.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				if test.allowed {
					observed := ready(t, c, request)
					if observed.Startup == nil || observed.Startup.Pod.UID != pod.UID {
						t.Fatal("admitted tolerations lost the exact startup identity")
					}
					return
				}
				if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
					t.Fatalf("changed toleration published startup: observation=%+v error=%v", observed, err)
				}
				if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
					t.Fatalf("changed toleration remained observable: observation=%+v error=%v", observed, err)
				}
			})
		}
	}
}

func TestPriorityAdmissionPreservesFrozenSandboxIntent(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*corev1.PodSpec)
		realize   func(*corev1.PodSpec)
		allowed   bool
	}{
		{"omitted priority admission", func(*corev1.PodSpec) {}, func(spec *corev1.PodSpec) {
			spec.PriorityClassName = "global-default"
			spec.Priority = new(int32(700))
			spec.PreemptionPolicy = new(corev1.PreemptNever)
		}, true},
		{"matching explicit priority", func(spec *corev1.PodSpec) { spec.Priority = new(int32(700)) }, func(*corev1.PodSpec) {}, true},
		{"changed explicit priority", func(spec *corev1.PodSpec) { spec.Priority = new(int32(700)) }, func(spec *corev1.PodSpec) { spec.Priority = new(int32(701)) }, false},
		{"matching explicit preemption", func(spec *corev1.PodSpec) { spec.PreemptionPolicy = new(corev1.PreemptNever) }, func(*corev1.PodSpec) {}, true},
		{"changed explicit preemption", func(spec *corev1.PodSpec) { spec.PreemptionPolicy = new(corev1.PreemptNever) }, func(spec *corev1.PodSpec) { spec.PreemptionPolicy = new(corev1.PreemptLowerPriority) }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, request := fixture(t, false)
			test.configure(&request.Runtime.Template.Spec)
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(t, c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			pod := materializedPod(t, c, request)
			test.realize(&pod.Spec)
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if test.allowed {
				observed := ready(t, c, request)
				if observed.Startup == nil || observed.Startup.Pod.UID != pod.UID {
					t.Fatal("priority admission lost the exact startup identity")
				}
				return
			}
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
				t.Fatalf("changed explicit priority published startup: observation=%+v error=%v", observed, err)
			}
		})
	}
}
