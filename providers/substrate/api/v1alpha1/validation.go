package v1alpha1

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Resolve applies the legacy native namespace default without mutating the
// profile. The referenced profile itself always remains in its class namespace.
func (r SubstrateTemplateReference) Resolve(profileNamespace string) (SubstrateTemplateReference, error) {
	r.Name = strings.TrimSpace(r.Name)
	r.Namespace = strings.TrimSpace(r.Namespace)
	if r.Namespace == "" {
		r.Namespace = profileNamespace
	}
	if len(validation.IsDNS1123Subdomain(r.Name)) != 0 {
		return SubstrateTemplateReference{}, fmt.Errorf("templateRef.name %q is invalid", r.Name)
	}
	if len(validation.IsDNS1123Label(r.Namespace)) != 0 {
		return SubstrateTemplateReference{}, fmt.Errorf("templateRef.namespace %q is invalid", r.Namespace)
	}
	return r, nil
}

// Validate checks profile-local requirements. Native infrastructure identity,
// snapshot policy, class expiry/reuse policy and checkpoint provenance require
// separate checks before allocation or resume; this method does not attest them.
func (p *SubstrateWorkspaceProfile) Validate() error {
	if _, err := p.Spec.TemplateRef.Resolve(p.Namespace); err != nil {
		return err
	}
	if p.Spec.Suspend != nil && p.Spec.Suspend.Mode != SubstrateSuspendModeDataOnly {
		return fmt.Errorf("suspend mode %q is not supported; only DataOnly is admitted and full-memory restore remains prohibited", p.Spec.Suspend.Mode)
	}
	if p.Spec.Retention != nil && p.Spec.Retention.MaxSuspendedWorkspaces != nil && *p.Spec.Retention.MaxSuspendedWorkspaces < 0 {
		return fmt.Errorf("maxSuspendedWorkspaces must not be negative")
	}
	return nil
}
