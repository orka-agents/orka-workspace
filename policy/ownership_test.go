package policy_test

import (
	"context"
	"os"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserveradmission "k8s.io/apiserver/pkg/admission"
	admissioncel "k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/cel/environment"
	"sigs.k8s.io/yaml"
)

const (
	coreUser          = "system:serviceaccount:workspace-system:core"
	operatorUser      = "system:serviceaccount:workspace-system:operator"
	providerUser      = "system:serviceaccount:workspace-system:alpha"
	otherProviderUser = "system:serviceaccount:workspace-system:beta"
	untrustedUser     = "system:serviceaccount:tenant:untrusted"
)

type evaluator struct {
	compiler    *admissioncel.CompositedCompiler
	validations admissioncel.ConditionEvaluator
}

// Compile the shipped YAML with the API server's compiler and lazy variables.
// The authorizer below models exact RBAC grants, not a boolean CEL replacement.
func compilePolicy(t *testing.T) evaluator {
	t.Helper()
	data, err := os.ReadFile("../config/policy/ownership.yaml")
	if err != nil {
		t.Fatal(err)
	}
	docs := strings.Split(string(data), "\n---\n")
	if len(docs) != 2 {
		t.Fatalf("got %d YAML documents, want policy and binding", len(docs))
	}
	var policy admissionregistrationv1.ValidatingAdmissionPolicy
	if err := yaml.UnmarshalStrict([]byte(docs[0]), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Spec.FailurePolicy == nil || *policy.Spec.FailurePolicy != admissionregistrationv1.Fail {
		t.Fatal("policy must fail closed")
	}
	if len(policy.Spec.MatchConstraints.ResourceRules) != 1 || len(policy.Spec.MatchConstraints.ResourceRules[0].Resources) != 10 {
		t.Fatal("all five shared resources and their status subresources must be protected")
	}
	var binding admissionregistrationv1.ValidatingAdmissionPolicyBinding
	if err := yaml.UnmarshalStrict([]byte(docs[1]), &binding); err != nil {
		t.Fatal(err)
	}
	if binding.Spec.PolicyName != policy.Name || len(binding.Spec.ValidationActions) != 1 || binding.Spec.ValidationActions[0] != admissionregistrationv1.Deny {
		t.Fatal("policy must have a matching Deny binding")
	}
	compiler, err := admissioncel.NewCompositedCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
	if err != nil {
		t.Fatal(err)
	}
	options := admissioncel.OptionalVariableDeclarations{HasAuthorizer: true}
	for _, variable := range policy.Spec.Variables {
		result := compiler.CompileAndStoreVariable(&validating.Variable{Name: variable.Name, Expression: variable.Expression}, options, environment.NewExpressions)
		if result.Error != nil {
			t.Fatalf("variable %s: %v", variable.Name, result.Error)
		}
	}
	expressions := make([]admissioncel.ExpressionAccessor, 0, len(policy.Spec.Validations))
	for _, validation := range policy.Spec.Validations {
		expressions = append(expressions, &validating.ValidationCondition{Expression: validation.Expression})
	}
	checks := compiler.CompileCondition(expressions, options, environment.NewExpressions)
	if errors := checks.CompilationErrors(); len(errors) != 0 {
		t.Fatalf("compile validations: %v", errors)
	}
	return evaluator{compiler: compiler, validations: checks}
}

func (e evaluator) allows(t *testing.T, resource, username, subresource string, current, previous *unstructured.Unstructured) bool {
	t.Helper()
	gv := schema.GroupVersion{Group: "workspace.orka.ai", Version: "v1alpha1"}
	kind := gv.WithKind(current.GetKind())
	gvr := gv.WithResource(resource)
	operation := apiserveradmission.Update
	var old runtime.Object
	if previous == nil {
		operation = apiserveradmission.Create
	} else {
		old = previous
	}
	attrs := apiserveradmission.NewAttributesRecord(current, old, kind, current.GetNamespace(), current.GetName(), gvr,
		subresource, operation, nil, false, &user.DefaultInfo{Name: username})
	versioned := &apiserveradmission.VersionedAttributes{
		Attributes: attrs, VersionedKind: kind,
		VersionedObject:    apiserveradmission.NewLazyObject(current),
		VersionedOldObject: apiserveradmission.NewLazyObject(old),
	}
	request := admissioncel.CreateAdmissionRequest(attrs, metav1.GroupVersionResource(gvr), metav1.GroupVersionKind(kind))
	grants := authorizer.AuthorizerFunc(func(_ context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
		if a.GetAPIGroup() != "workspace.orka.ai" {
			return authorizer.DecisionDeny, "wrong group", nil
		}
		allowed := a.GetUser().GetName() == coreUser && a.GetVerb() == "admit" ||
			a.GetUser().GetName() == operatorUser && a.GetVerb() == "configure"
		if a.GetVerb() == "provider-status" && a.GetResource() == "executionworkspaceproviders" && a.GetNamespace() == "" {
			allowed = a.GetUser().GetName() == providerUser && a.GetName() == "alpha" ||
				a.GetUser().GetName() == otherProviderUser && a.GetName() == "beta"
		}
		if allowed {
			return authorizer.DecisionAllow, "exact virtual-verb grant", nil
		}
		return authorizer.DecisionDeny, "no virtual-verb grant", nil
	})
	checks, _, err := e.validations.ForInput(e.compiler.CreateContext(context.Background()), versioned, request,
		admissioncel.OptionalVariableBindings{Authorizer: grants}, nil, celconfig.RuntimeCELCostBudget)
	if err != nil {
		t.Fatal(err)
	}
	for i, check := range checks {
		if check.Error != nil {
			t.Fatalf("validation %d evaluation error: %v", i, check.Error)
		}
		if check.EvalResult.Value() != true {
			return false
		}
	}
	return true
}

func fixture(resource string) *unstructured.Unstructured {
	kinds := map[string]string{
		"executionworkspaceproviders": "ExecutionWorkspaceProvider", "executionworkspaceclasses": "ExecutionWorkspaceClass",
		"executionworkspacepools": "ExecutionWorkspacePool", "executionworkspaces": "ExecutionWorkspace", "executionworkspacecheckpoints": "ExecutionWorkspaceCheckpoint",
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "workspace.orka.ai/v1alpha1", "kind": kinds[resource],
		"metadata": map[string]any{"name": "example", "namespace": "tenant", "uid": "object-uid", "resourceVersion": "1",
			"labels": map[string]any{"workspace.orka.ai/controller-name": "alpha.example.org"}},
		"spec": map[string]any{},
	}}
	switch resource {
	case "executionworkspaceproviders":
		object.SetName("alpha")
		object.SetNamespace("")
		object.Object["spec"] = map[string]any{"controllerName": "alpha.example.org/controller", "lifecycleState": "Active"}
	case "executionworkspacepools":
		object.Object["spec"] = map[string]any{"providerRef": map[string]any{"name": "alpha"}, "capacity": map[string]any{"minReady": int64(0), "maxSize": int64(1)}}
	case "executionworkspaces":
		object.Object["spec"] = map[string]any{"providerBinding": map[string]any{"name": "alpha", "uid": "provider-uid"}, "desiredState": "Ready"}
		object.SetFinalizers([]string{"workspace.orka.ai/finalizer"})
	case "executionworkspacecheckpoints":
		object.Object["spec"] = map[string]any{"workspaceRef": map[string]any{"name": "source", "uid": "source-uid"}}
		labels := object.GetLabels()
		labels["workspace.orka.ai/provider-name"] = "alpha"
		object.SetLabels(labels)
	}
	return object
}

func set(t *testing.T, object *unstructured.Unstructured, value any, fields ...string) {
	t.Helper()
	if err := unstructured.SetNestedField(object.Object, value, fields...); err != nil {
		t.Fatal(err)
	}
}

func condition(name string) map[string]any {
	return map[string]any{"type": name, "status": "True", "reason": "Ready"}
}

func TestProviderStatusRequiresExactRegistrationGrant(t *testing.T) {
	policy := compilePolicy(t)
	for _, resource := range []string{"executionworkspaceproviders", "executionworkspacepools", "executionworkspaces", "executionworkspacecheckpoints"} {
		t.Run(resource, func(t *testing.T) {
			old := fixture(resource)
			updated := old.DeepCopy()
			fields := map[string]string{"executionworkspaceproviders": "observedGeneration", "executionworkspacepools": "allocated", "executionworkspaces": "attachedEpoch", "executionworkspacecheckpoints": "phase"}
			var value any = int64(1)
			if resource == "executionworkspacecheckpoints" {
				value = "Ready"
			}
			set(t, updated, value, "status", fields[resource])
			for _, identity := range []string{providerUser, otherProviderUser, untrustedUser, coreUser, operatorUser} {
				if got := policy.allows(t, resource, identity, "status", updated, old); got != (identity == providerUser) {
					t.Errorf("%s status allowed=%t", identity, got)
				}
			}
			// Status cannot be smuggled through an ordinary object patch either.
			if policy.allows(t, resource, untrustedUser, "", updated, old) {
				t.Fatal("ordinary patch bypassed status ownership")
			}
		})
	}
}

func TestConditionOwnershipPreservesOtherWriters(t *testing.T) {
	policy := compilePolicy(t)
	for _, tc := range []struct{ resource, condition, owner string }{
		{"executionworkspaceproviders", "Ready", coreUser}, {"executionworkspaceproviders", "Compatible", coreUser},
		{"executionworkspaceproviders", "HeartbeatFresh", coreUser}, {"executionworkspaceclasses", "Ready", coreUser},
		{"executionworkspacepools", "Ready", providerUser}, {"executionworkspacepools", "Admitted", providerUser},
		{"executionworkspaces", "Admitted", coreUser}, {"executionworkspaces", "Quarantined", coreUser},
		{"executionworkspaces", "Provisioned", providerUser}, {"executionworkspaces", "DataPlaneReady", providerUser},
		{"executionworkspaces", "Attached", providerUser}, {"executionworkspaces", "Finalized", providerUser},
		{"executionworkspacecheckpoints", "Ready", providerUser},
	} {
		t.Run(tc.resource+"/"+tc.condition, func(t *testing.T) {
			old := fixture(tc.resource)
			updated := old.DeepCopy()
			set(t, updated, []any{condition(tc.condition)}, "status", "conditions")
			for _, identity := range []string{coreUser, providerUser, untrustedUser} {
				if got := policy.allows(t, tc.resource, identity, "status", updated, old); got != (identity == tc.owner) {
					t.Errorf("%s allowed=%t", identity, got)
				}
			}
		})
	}
	old := fixture("executionworkspaces")
	set(t, old, "Ready", "status", "state")
	set(t, old, []any{condition("Admitted"), condition("Attached")}, "status", "conditions")
	updated := old.DeepCopy()
	set(t, updated, []any{condition("Attached")}, "status", "conditions")
	if policy.allows(t, "executionworkspaces", providerUser, "status", updated, old) {
		t.Fatal("provider removed core condition")
	}
	updated = old.DeepCopy()
	unstructured.RemoveNestedField(updated.Object, "status")
	if policy.allows(t, "executionworkspaces", providerUser, "status", updated, old) {
		t.Fatal("provider erased core conditions with status removal")
	}
	updated = old.DeepCopy()
	set(t, updated, []any{condition("Admitted"), condition("Attached"), condition("Unassigned")}, "status", "conditions")
	if policy.allows(t, "executionworkspaces", providerUser, "status", updated, old) {
		t.Fatal("provider added unassigned condition")
	}
	updated = old.DeepCopy()
	set(t, updated, "Attached", "status", "state")
	if !policy.allows(t, "executionworkspaces", providerUser, "status", updated, old) {
		t.Fatal("provider could not preserve core conditions")
	}
}

func TestCoreIntentAndRoutingCannotBeForged(t *testing.T) {
	policy := compilePolicy(t)
	for _, tc := range []struct {
		name   string
		value  any
		fields []string
	}{
		{"admission", map[string]any{"admittedGeneration": int64(1)}, []string{"spec", "coreAdmission"}},
		{"attachment", map[string]any{"epoch": int64(1)}, []string{"spec", "attachment"}},
		{"epoch", int64(2), []string{"spec", "attachmentEpoch"}},
		{"retirement", "Deleted", []string{"spec", "desiredState"}},
		{"materialization", "invented-pool", []string{"metadata", "annotations", "workspace.orka.ai/runtime-pool"}},
		{"owner", []any{map[string]any{"uid": "foreign"}}, []string{"metadata", "ownerReferences"}},
		{"core-finalizer-removal", []any{}, []string{"metadata", "finalizers"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := fixture("executionworkspaces")
			updated := old.DeepCopy()
			set(t, updated, tc.value, tc.fields...)
			if tc.name == "attachment" {
				set(t, updated, int64(1), "spec", "attachmentEpoch")
			}
			for _, identity := range []string{coreUser, providerUser, otherProviderUser, operatorUser, untrustedUser} {
				if got := policy.allows(t, "executionworkspaces", identity, "", updated, old); got != (identity == coreUser) {
					t.Errorf("%s allowed=%t", identity, got)
				}
			}
		})
	}
	for _, resource := range []string{"executionworkspaces", "executionworkspacecheckpoints"} {
		old := fixture(resource)
		updated := old.DeepCopy()
		set(t, updated, "beta.example.org", "metadata", "labels", "workspace.orka.ai/controller-name")
		if policy.allows(t, resource, otherProviderUser, "", updated, old) {
			t.Fatal("provider changed routing to claim foreign object")
		}
		if policy.allows(t, resource, coreUser, "", updated, old) {
			t.Fatal("established routing was mutable")
		}
	}
}

func TestMetadataPartitionsAndMissingRouting(t *testing.T) {
	policy := compilePolicy(t)
	old := fixture("executionworkspaces")
	updated := old.DeepCopy()
	set(t, updated, "ready", "metadata", "annotations", "alpha.example.org/materialized")
	if !policy.allows(t, "executionworkspaces", providerUser, "", updated, old) {
		t.Fatal("selected provider cannot annotate its own namespace")
	}
	if policy.allows(t, "executionworkspaces", otherProviderUser, "", updated, old) {
		t.Fatal("foreign provider annotation accepted")
	}
	if policy.allows(t, "executionworkspaces", coreUser, "", updated, old) {
		t.Fatal("core wrote provider observation")
	}
	updated.SetFinalizers([]string{"workspace.orka.ai/finalizer", "alpha.example.org/cleanup"})
	if !policy.allows(t, "executionworkspaces", providerUser, "", updated, old) {
		t.Fatal("provider cleanup finalizer rejected")
	}
	old = updated.DeepCopy()
	old.SetFinalizers(append(old.GetFinalizers(), "example.org/operator-hold"))
	if policy.allows(t, "executionworkspaces", providerUser, "", updated, old) {
		t.Fatal("provider removed unrelated finalizer")
	}
	old = fixture("executionworkspacecheckpoints")
	labels := old.GetLabels()
	delete(labels, "workspace.orka.ai/provider-name")
	old.SetLabels(labels)
	updated = old.DeepCopy()
	set(t, updated, "Ready", "status", "phase")
	if policy.allows(t, "executionworkspacecheckpoints", providerUser, "status", updated, old) {
		t.Fatal("checkpoint without core provider routing accepted status")
	}
	labels = updated.GetLabels()
	labels["workspace.orka.ai/provider-name"] = "alpha"
	updated.SetLabels(labels)
	if policy.allows(t, "executionworkspacecheckpoints", providerUser, "", updated, old) {
		t.Fatal("provider self-assigned checkpoint routing")
	}
}

func TestConfigurationAndCreateOwnership(t *testing.T) {
	policy := compilePolicy(t)
	for _, resource := range []string{"executionworkspaceproviders", "executionworkspaceclasses", "executionworkspacepools", "executionworkspacecheckpoints", "executionworkspaces"} {
		t.Run(resource, func(t *testing.T) {
			old := fixture(resource)
			updated := old.DeepCopy()
			set(t, updated, "changed", "spec", "testField")
			owner := operatorUser
			if resource == "executionworkspaces" {
				owner = coreUser
			}
			for _, identity := range []string{owner, providerUser, untrustedUser} {
				if got := policy.allows(t, resource, identity, "", updated, old); got != (identity == owner) {
					t.Errorf("%s updated spec allowed=%t", identity, got)
				}
			}
			created := fixture(resource)
			created.SetLabels(nil)
			created.SetFinalizers(nil)
			for _, identity := range []string{owner, providerUser, untrustedUser} {
				if got := policy.allows(t, resource, identity, "", created, nil); got != (identity == owner) {
					t.Errorf("%s create allowed=%t", identity, got)
				}
			}
			if !policy.allows(t, resource, untrustedUser, "", old.DeepCopy(), old) {
				t.Fatal("no-op unexpectedly rejected")
			}
		})
	}
}

func TestClassStatusIsCoreOwned(t *testing.T) {
	policy := compilePolicy(t)
	old := fixture("executionworkspaceclasses")
	updated := old.DeepCopy()
	set(t, updated, "sha256:resolved", "status", "profileHash")
	if !policy.allows(t, "executionworkspaceclasses", coreUser, "status", updated, old) {
		t.Fatal("core class resolution rejected")
	}
	if policy.allows(t, "executionworkspaceclasses", providerUser, "status", updated, old) {
		t.Fatal("provider changed class binding")
	}
}
