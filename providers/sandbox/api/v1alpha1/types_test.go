package v1alpha1

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralcel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
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
	for _, object := range []runtime.Object{&SandboxProviderConfig{}, &SandboxProviderConfigList{}, &SandboxWorkspaceProfile{}, &SandboxWorkspaceProfileList{}} {
		gvks, _, err := scheme.ObjectKinds(object)
		if err != nil || len(gvks) != 1 || gvks[0].GroupVersion() != GroupVersion {
			t.Fatalf("%T: %v, %v", object, gvks, err)
		}
	}
	limit := int32(2)
	original := &SandboxWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Labels: map[string]string{"owner": "original"}}, Spec: SandboxWorkspaceProfileSpec{
		Suspend:   &SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: "1Gi", AccessModes: []string{"ReadWriteOnce"}}},
		Retention: &RetentionPolicy{MaxSuspendedWorkspaces: &limit},
	}}
	copy := original.DeepCopy()
	copy.Labels["owner"] = "copy"
	copy.Spec.Suspend.Volume.AccessModes[0] = "ReadWriteMany"
	*copy.Spec.Retention.MaxSuspendedWorkspaces = 9
	if original.Labels["owner"] != "original" || original.Spec.Suspend.Volume.AccessModes[0] != "ReadWriteOnce" || *original.Spec.Retention.MaxSuspendedWorkspaces != 2 {
		t.Fatal("deep copy shares mutable profile data")
	}
}

func TestLegacyVolumeNormalizationAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  SandboxSuspendPolicy
		want    SandboxDurableVolume
		invalid bool
	}{
		{name: "default writable mode", policy: SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: " 1Gi ", StorageClassName: " fast "}}, want: SandboxDurableVolume{Capacity: "1Gi", StorageClassName: "fast", AccessModes: []string{"ReadWriteOnce"}}},
		{name: "stable access mode order", policy: SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: "2Gi", AccessModes: []string{"ReadWriteOncePod", "ReadWriteMany"}}}, want: SandboxDurableVolume{Capacity: "2Gi", AccessModes: []string{"ReadWriteMany", "ReadWriteOncePod"}}},
		{name: "full memory", policy: SandboxSuspendPolicy{Mode: "Full", Volume: SandboxDurableVolume{Capacity: "1Gi"}}, invalid: true},
		{name: "missing scope", policy: SandboxSuspendPolicy{Volume: SandboxDurableVolume{Capacity: "1Gi"}}, invalid: true},
		{name: "zero", policy: SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: "0"}}, invalid: true},
		{name: "negative", policy: SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: "-1Gi"}}, invalid: true},
		{name: "invalid quantity", policy: SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: "many"}}, invalid: true},
		{name: "read only", policy: SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: "1Gi", AccessModes: []string{"ReadOnlyMany"}}}, invalid: true},
		{name: "invalid storage class", policy: SandboxSuspendPolicy{Mode: SandboxSuspendModeDataOnly, Volume: SandboxDurableVolume{Capacity: "1Gi", StorageClassName: "bad/name"}}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.policy.DeepCopy()
			got, err := tc.policy.ResolveVolume()
			if (err != nil) != tc.invalid {
				t.Fatalf("ResolveVolume error=%v, invalid=%v", err, tc.invalid)
			}
			if !tc.invalid && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("volume=%#v, want %#v", got, tc.want)
			}
			if !reflect.DeepEqual(&tc.policy, before) {
				t.Fatal("resolution mutated immutable profile")
			}
		})
	}
	profile := &SandboxWorkspaceProfile{}
	if err := profile.Validate(); err != nil {
		t.Fatalf("non-suspendable profile: %v", err)
	}
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
	config := loadSchema(t, "sandboxproviderconfigs")
	profile := loadSchema(t, "sandboxworkspaceprofiles")
	if config.crd.Spec.Scope != apiextensionsv1.ClusterScoped || profile.crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
		t.Fatal("parameter reference scope changed")
	}
	for _, s := range []profileSchema{config, profile} {
		if s.crd.Spec.Group != GroupVersion.Group || len(s.crd.Spec.Versions) != 1 || s.crd.Spec.Versions[0].Name != GroupVersion.Version || !s.crd.Spec.Versions[0].Storage {
			t.Fatal("unexpected GVK/storage version")
		}
		spec := s.crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
		if len(spec.XValidations) != 1 || spec.XValidations[0].Rule != "self == oldSelf" {
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

func TestGeneratedProfileValidationDefaultingAndImmutability(t *testing.T) {
	schema := loadSchema(t, "sandboxworkspaceprofiles")
	empty := map[string]any{}
	defaulting.Default(empty, schema.structural)
	if _, exists := empty["spec"]; !exists {
		t.Fatal("omitted spec must default to an immutable empty object")
	}
	valid := map[string]any{"spec": map[string]any{"suspend": map[string]any{"mode": "DataOnly", "volume": map[string]any{"capacity": "1Gi"}}}}
	defaulting.Default(valid, schema.structural)
	volume := valid["spec"].(map[string]any)["suspend"].(map[string]any)["volume"].(map[string]any)
	if !reflect.DeepEqual(volume["accessModes"], []any{"ReadWriteOnce"}) {
		t.Fatalf("default access modes = %v", volume["accessModes"])
	}
	if errs := schema.validate(t, valid, nil); len(errs) != 0 {
		t.Fatal(errs)
	}
	if len(schema.validate(t, valid, empty)) == 0 {
		t.Fatal("adding suspend to an existing empty profile bypassed immutability")
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"full memory", func(spec map[string]any) { spec["suspend"].(map[string]any)["mode"] = "Full" }},
		{"missing mode", func(spec map[string]any) { delete(spec["suspend"].(map[string]any), "mode") }},
		{"missing volume", func(spec map[string]any) { delete(spec["suspend"].(map[string]any), "volume") }},
		{"empty capacity", func(spec map[string]any) {
			spec["suspend"].(map[string]any)["volume"].(map[string]any)["capacity"] = ""
		}},
		{"read only", func(spec map[string]any) {
			spec["suspend"].(map[string]any)["volume"].(map[string]any)["accessModes"] = []any{"ReadOnlyMany"}
		}},
		{"negative retention", func(spec map[string]any) { spec["retention"] = map[string]any{"maxSuspendedWorkspaces": int64(-1)} }},
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
	updated["spec"].(map[string]any)["suspend"].(map[string]any)["volume"].(map[string]any)["capacity"] = "2Gi"
	if len(schema.validate(t, updated, valid)) == 0 {
		t.Fatal("functional spec mutation accepted")
	}
	updated = runtime.DeepCopyJSON(valid)
	updated["metadata"] = map[string]any{"labels": map[string]any{"description": "updated"}}
	if errs := schema.validate(t, updated, valid); len(errs) != 0 {
		t.Fatalf("metadata update rejected: %v", errs)
	}
	bytes, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var profile SandboxWorkspaceProfile
	if err := json.Unmarshal(bytes, &profile); err != nil {
		t.Fatal(err)
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(profile.Spec.Suspend.Volume.AccessModes, []string{"ReadWriteOnce"}) {
		t.Fatal("defaulted schema did not round-trip into type")
	}
}
