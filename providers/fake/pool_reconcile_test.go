package fake

import (
	"context"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	fakev1alpha1 "github.com/orka-agents/orka-workspace/providers/fake/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func poolFixture(t *testing.T, funcs interceptor.Funcs, kind string, withParameters bool) (client.Client, *workspacev1alpha1.ExecutionWorkspacePool) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, workspacev1alpha1.AddToScheme, fakev1alpha1.AddToScheme} {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{ObjectMeta: metav1.ObjectMeta{Name: "fake", UID: "provider-uid"}, Spec: workspacev1alpha1.ExecutionWorkspaceProviderSpec{ControllerName: FakeWorkspaceControllerName}}
	pool := &workspacev1alpha1.ExecutionWorkspacePool{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "pool", UID: "pool-uid", Generation: 1}, Spec: workspacev1alpha1.ExecutionWorkspacePoolSpec{
		ProviderRef:   workspacev1alpha1.ClusterObjectReference{Name: provider.Name},
		ParametersRef: workspacev1alpha1.TypedObjectReference{Group: fakev1alpha1.GroupVersion.Group, Kind: kind, Name: "params"},
		Capacity:      workspacev1alpha1.ExecutionWorkspacePoolCapacity{MinReady: 2, MaxSize: 4},
	}}
	objects := []client.Object{provider, pool}
	if withParameters {
		objects = append(objects, &fakev1alpha1.FakePoolParameters{ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: "params", UID: "params-uid"}})
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(pool).WithInterceptorFuncs(funcs).Build()
	return c, pool
}

func reconcilePool(t *testing.T, c client.Client, pool *workspacev1alpha1.ExecutionWorkspacePool) *workspacev1alpha1.ExecutionWorkspacePool {
	t.Helper()
	if _, err := (&FakeExecutionWorkspacePoolReconciler{Client: c}).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err != nil {
		t.Fatal(err)
	}
	current := &workspacev1alpha1.ExecutionWorkspacePool{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pool), current); err != nil {
		t.Fatal(err)
	}
	return current
}

func poolConditionTrue(pool *workspacev1alpha1.ExecutionWorkspacePool, condition workspacev1alpha1.ExecutionWorkspaceConditionType) bool {
	found := workspaceprovider.FindCondition(pool.Status.Conditions, string(condition))
	return found != nil && found.Status == metav1.ConditionTrue
}

func TestPoolRequiresExactParametersBeforeReadiness(t *testing.T) {
	for _, tc := range []struct {
		name           string
		kind           string
		withParameters bool
		wantReady      bool
	}{
		{name: "missing parameters", kind: fakePoolParametersKind},
		{name: "foreign parameters kind", kind: fakeProviderConfigKind, withParameters: true},
		{name: "exact parameters", kind: fakePoolParametersKind, withParameters: true, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, pool := poolFixture(t, interceptor.Funcs{}, tc.kind, tc.withParameters)
			current := reconcilePool(t, c, pool)
			if poolConditionTrue(current, workspacev1alpha1.ConditionPoolReady) != tc.wantReady || poolConditionTrue(current, workspacev1alpha1.ConditionPoolAdmitted) != tc.wantReady {
				t.Fatalf("pool conditions = %+v, want ready=%v", current.Status.Conditions, tc.wantReady)
			}
			wantAvailable := int32(0)
			if tc.wantReady {
				wantAvailable = 2
			}
			if current.Status.Available != wantAvailable || current.Status.Total != wantAvailable {
				t.Fatalf("pool capacity = available %d total %d, want %d", current.Status.Available, current.Status.Total, wantAvailable)
			}
		})
	}
}

func TestPoolCapacityIsRecomputedAfterStatusConflict(t *testing.T) {
	downsized := false
	funcs := interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, object client.Object, patch client.Patch, options ...client.SubResourcePatchOption) error {
		if !downsized {
			downsized = true
			current := &workspacev1alpha1.ExecutionWorkspacePool{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(object), current); err != nil {
				return err
			}
			current.Spec.Capacity = workspacev1alpha1.ExecutionWorkspacePoolCapacity{MinReady: 1, MaxSize: 1}
			if err := c.Update(ctx, current); err != nil {
				return err
			}
		}
		return c.SubResource(subResource).Patch(ctx, object, patch, options...)
	}}
	c, pool := poolFixture(t, funcs, fakePoolParametersKind, true)
	current := reconcilePool(t, c, pool)
	if !downsized || current.Status.Total != 1 || current.Status.Available != 1 {
		t.Fatalf("pool status kept pre-conflict capacity: available %d total %d", current.Status.Available, current.Status.Total)
	}
}
