package v1alpha1

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralcel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	structuralpruning "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	openapivalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

func TestTypesRegisterAndDeepCopy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, object := range []runtime.Object{&SubstrateProviderConfig{}, &SubstrateProviderConfigList{}, &SubstrateWorkspaceProfile{}, &SubstrateWorkspaceProfileList{}} {
		gvks, _, err := scheme.ObjectKinds(object)
		if err != nil || len(gvks) != 1 || gvks[0].GroupVersion() != GroupVersion {
			t.Fatalf("%T: %v, %v", object, gvks, err)
		}
	}
	limit := int32(2)
	original := &SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Labels: map[string]string{"owner": "original"}}, Spec: SubstrateWorkspaceProfileSpec{
		TemplateRef: SubstrateTemplateReference{Name: "template"}, Suspend: &SubstrateSuspendPolicy{Mode: SubstrateSuspendModeDataOnly},
		Retention: &RetentionPolicy{MaxSuspendedWorkspaces: &limit},
	}}
	copy := original.DeepCopy()
	copy.Labels["owner"] = "copy"
	copy.Spec.Suspend.Mode = "Full"
	*copy.Spec.Retention.MaxSuspendedWorkspaces = 9
	if original.Labels["owner"] != "original" || original.Spec.Suspend.Mode != SubstrateSuspendModeDataOnly || *original.Spec.Retention.MaxSuspendedWorkspaces != 2 {
		t.Fatal("deep copy shares mutable profile data")
	}
}

func TestLegacyTemplateReferenceResolution(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ref       SubstrateTemplateReference
		namespace string
		want      SubstrateTemplateReference
		invalid   bool
	}{
		{name: "profile namespace default", ref: SubstrateTemplateReference{Name: " template "}, namespace: "tenant", want: SubstrateTemplateReference{Name: "template", Namespace: "tenant"}},
		{name: "native infrastructure namespace", ref: SubstrateTemplateReference{Name: "template", Namespace: " infrastructure "}, namespace: "tenant", want: SubstrateTemplateReference{Name: "template", Namespace: "infrastructure"}},
		{name: "missing name", namespace: "tenant", invalid: true},
		{name: "invalid name", ref: SubstrateTemplateReference{Name: "bad/name"}, namespace: "tenant", invalid: true},
		{name: "invalid namespace", ref: SubstrateTemplateReference{Name: "template", Namespace: "Bad"}, namespace: "tenant", invalid: true},
		{name: "missing default", ref: SubstrateTemplateReference{Name: "template"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.ref
			got, err := tc.ref.Resolve(tc.namespace)
			if (err != nil) != tc.invalid {
				t.Fatalf("Resolve error=%v, invalid=%v", err, tc.invalid)
			}
			if !tc.invalid && got != tc.want {
				t.Fatalf("reference=%#v, want %#v", got, tc.want)
			}
			if tc.ref != before {
				t.Fatal("resolution mutated immutable reference")
			}
		})
	}
}

func TestFullMemoryGateAndRetentionValidation(t *testing.T) {
	profile := &SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant"}, Spec: SubstrateWorkspaceProfileSpec{TemplateRef: SubstrateTemplateReference{Name: "template"}}}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []SubstrateSuspendMode{SubstrateSuspendModeDataOnly, "Full", "Memory", ""} {
		profile.Spec.Suspend = &SubstrateSuspendPolicy{Mode: mode}
		if err := profile.Validate(); (err != nil) != (mode != SubstrateSuspendModeDataOnly) {
			t.Fatalf("mode %q validation=%v", mode, err)
		}
	}
	profile.Spec.Suspend = nil
	negative := int32(-1)
	profile.Spec.Retention = &RetentionPolicy{MaxSuspendedWorkspaces: &negative}
	if profile.Validate() == nil {
		t.Fatal("negative retention accepted")
	}
}

type profileSchema struct {
	crd        apiextensionsv1.CustomResourceDefinition
	structural *structuralschema.Structural
	openapi    openapivalidation.SchemaValidator
	cel        *structuralcel.Validator
}

func loadSchema(t *testing.T, plural string) profileSchema {
	t.Helper()
	data, err := os.ReadFile("../../config/crd/" + GroupVersion.Group + "_" + plural + ".yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(data, &crd); err != nil {
		t.Fatal(err)
	}
	var internal apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &internal, nil); err != nil {
		t.Fatal(err)
	}
	structural, err := structuralschema.NewStructural(&internal)
	if err != nil {
		t.Fatal(err)
	}
	validator, _, err := openapivalidation.NewSchemaValidator(&internal)
	if err != nil {
		t.Fatal(err)
	}
	return profileSchema{crd: crd, structural: structural, openapi: validator, cel: structuralcel.NewValidator(structural, true, celconfig.PerCallLimit)}
}

func (s profileSchema) validate(t *testing.T, value, old map[string]any) field.ErrorList {
	t.Helper()
	errs := openapivalidation.ValidateCustomResource(field.NewPath("object"), value, s.openapi)
	if len(errs) == 0 && s.cel != nil {
		var oldValue any
		if old != nil {
			oldValue = old
		}
		celErrors, _ := s.cel.Validate(context.Background(), field.NewPath("object"), s.structural, value, oldValue, celconfig.RuntimeCELCostBudget)
		errs = append(errs, celErrors...)
	}
	return errs
}

func TestGeneratedSchemaScopeAndProviderBoundary(t *testing.T) {
	config := loadSchema(t, "substrateproviderconfigs")
	profile := loadSchema(t, "substrateworkspaceprofiles")
	if config.crd.Spec.Scope != apiextensionsv1.ClusterScoped || profile.crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
		t.Fatal("parameter reference scope changed")
	}
	for _, s := range []profileSchema{config, profile} {
		if s.crd.Spec.Group != GroupVersion.Group || len(s.crd.Spec.Versions) != 1 || s.crd.Spec.Versions[0].Name != GroupVersion.Version || !s.crd.Spec.Versions[0].Storage {
			t.Fatal("unexpected GVK/storage version")
		}
		spec := s.crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
		if len(spec.XValidations) == 0 || spec.XValidations[0].Rule != "self == oldSelf" {
			t.Fatal("immutable spec rule missing")
		}
		for _, key := range []string{"backend", "substrate", "agentSandbox"} {
			if _, exists := spec.Properties[key]; exists {
				t.Fatalf("closed union field %s leaked into provider schema", key)
			}
		}
	}
	if len(config.crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties) != 0 {
		t.Fatal("empty backend config gained unexpected fields")
	}
	if errs := config.validate(t, map[string]any{"spec": map[string]any{}}, nil); len(errs) != 0 {
		t.Fatal(errs)
	}
}

func TestGeneratedProfileValidationAndImmutability(t *testing.T) {
	schema := loadSchema(t, "substrateworkspaceprofiles")
	valid := map[string]any{"spec": map[string]any{"templateRef": map[string]any{"name": "template"}, "suspend": map[string]any{"mode": "DataOnly"}}}
	if errs := schema.validate(t, valid, nil); len(errs) != 0 {
		t.Fatal(errs)
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"full memory", func(spec map[string]any) { spec["suspend"].(map[string]any)["mode"] = "Full" }},
		{"missing mode", func(spec map[string]any) { delete(spec["suspend"].(map[string]any), "mode") }},
		{"missing template", func(spec map[string]any) { delete(spec, "templateRef") }},
		{"missing template name", func(spec map[string]any) { spec["templateRef"] = map[string]any{} }},
		{"empty template name", func(spec map[string]any) { spec["templateRef"] = map[string]any{"name": ""} }},
		{"negative retention", func(spec map[string]any) { spec["retention"] = map[string]any{"maxSuspendedWorkspaces": int64(-1)} }},
		{"zero warm window", func(spec map[string]any) { spec["retention"] = map[string]any{"suspendAfterDetach": "0s"} }},
		{"negative warm window", func(spec map[string]any) { spec["retention"] = map[string]any{"suspendAfterDetach": "-1m"} }},
		{"warm window over 1h", func(spec map[string]any) { spec["retention"] = map[string]any{"suspendAfterDetach": "1h0m1s"} }},
		{"unparsable warm window", func(spec map[string]any) { spec["retention"] = map[string]any{"suspendAfterDetach": "soon"} }},
		{"non-string warm window", func(spec map[string]any) { spec["retention"] = map[string]any{"suspendAfterDetach": int64(300)} }},
		{"warm window without suspend", func(spec map[string]any) {
			delete(spec, "suspend")
			spec["retention"] = map[string]any{"suspendAfterDetach": "5m"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := runtime.DeepCopyJSON(valid)
			tc.mutate(object["spec"].(map[string]any))
			if len(schema.validate(t, object, nil)) == 0 {
				t.Fatal("invalid profile accepted by generated schema")
			}
		})
	}
	updated := runtime.DeepCopyJSON(valid)
	updated["spec"].(map[string]any)["templateRef"].(map[string]any)["name"] = "another-template"
	if len(schema.validate(t, updated, valid)) == 0 {
		t.Fatal("functional spec mutation accepted")
	}
	updated = runtime.DeepCopyJSON(valid)
	updated["metadata"] = map[string]any{"labels": map[string]any{"description": "updated"}}
	if errs := schema.validate(t, updated, valid); len(errs) != 0 {
		t.Fatalf("metadata update rejected: %v", errs)
	}
	if !reflect.DeepEqual(valid["spec"], updated["spec"]) {
		t.Fatal("metadata update changed profile")
	}
}

func TestSuspendAfterDetachValidation(t *testing.T) {
	base := func(window time.Duration, suspend bool) *SubstrateWorkspaceProfile {
		p := &SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant"}, Spec: SubstrateWorkspaceProfileSpec{
			TemplateRef: SubstrateTemplateReference{Name: "template"},
			Retention:   &RetentionPolicy{SuspendAfterDetach: &metav1.Duration{Duration: window}},
		}}
		if suspend {
			p.Spec.Suspend = &SubstrateSuspendPolicy{Mode: SubstrateSuspendModeDataOnly}
		}
		return p
	}
	for _, tc := range []struct {
		name    string
		window  time.Duration
		suspend bool
		valid   bool
	}{
		{"one second", time.Second, true, true},
		{"five minutes", 5 * time.Minute, true, true},
		{"one hour", time.Hour, true, true},
		{"zero", 0, true, false},
		{"negative", -time.Minute, true, false},
		{"over one hour", time.Hour + time.Second, true, false},
		{"without suspend", 5 * time.Minute, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := base(tc.window, tc.suspend).Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate()=%v, valid=%v", err, tc.valid)
			}
		})
	}
	unset := base(0, false)
	unset.Spec.Retention.SuspendAfterDetach = nil
	if err := unset.Validate(); err != nil {
		t.Fatalf("unset warm window rejected without suspend: %v", err)
	}
}

func TestGeneratedSuspendAfterDetachSchema(t *testing.T) {
	schema := loadSchema(t, "substrateworkspaceprofiles")
	retention := schema.crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["retention"]
	window, ok := retention.Properties["suspendAfterDetach"]
	if !ok || window.Type != "string" {
		t.Fatalf("suspendAfterDetach missing from structural schema: %#v", window)
	}
	// A default would rewrite stored profiles and change their content hash.
	if window.Default != nil || retention.Default != nil {
		t.Fatal("suspendAfterDetach must not be defaulted")
	}
	for _, value := range []string{"1s", "30s", "5m", "15m0s", "1h", "60m"} {
		object := map[string]any{"spec": map[string]any{"templateRef": map[string]any{"name": "template"}, "suspend": map[string]any{"mode": "DataOnly"}, "retention": map[string]any{"suspendAfterDetach": value, "maxSuspendedWorkspaces": int64(2)}}}
		if errs := schema.validate(t, object, nil); len(errs) != 0 {
			t.Fatalf("%s rejected: %v", value, errs)
		}
		// Structural pruning keeps the field, so core reads what the operator wrote.
		pruned := runtime.DeepCopyJSON(object)
		structuralpruning.Prune(pruned, schema.structural, true)
		if !reflect.DeepEqual(pruned, object) {
			t.Fatalf("%s pruned: %v", value, pruned)
		}
	}
	// Existing profiles carry no warm window and remain valid and unchanged.
	existing := map[string]any{"spec": map[string]any{"templateRef": map[string]any{"name": "template"}, "suspend": map[string]any{"mode": "DataOnly"}, "retention": map[string]any{"maxSuspendedWorkspaces": int64(2)}}}
	pruned := runtime.DeepCopyJSON(existing)
	structuralpruning.Prune(pruned, schema.structural, true)
	if errs := schema.validate(t, existing, nil); len(errs) != 0 || !reflect.DeepEqual(pruned, existing) {
		t.Fatalf("existing profile changed or rejected: %v %v", errs, pruned)
	}
	typed := &SubstrateWorkspaceProfile{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(existing, typed); err != nil {
		t.Fatal(err)
	}
	if typed.Spec.Retention.SuspendAfterDetach != nil {
		t.Fatal("existing profile gained a warm window")
	}
	roundTrip, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip["spec"], existing["spec"]) {
		t.Fatalf("existing profile spec changed on round trip: %v", roundTrip["spec"])
	}
}
