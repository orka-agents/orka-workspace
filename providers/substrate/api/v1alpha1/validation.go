package v1alpha1

import (
	"fmt"
	"strings"
	"time"

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
	if p.Spec.Retention != nil && p.Spec.Retention.SuspendAfterDetach != nil {
		window := p.Spec.Retention.SuspendAfterDetach.Duration
		if window <= 0 || window > MaxSuspendAfterDetach {
			return fmt.Errorf("suspendAfterDetach %s must be positive and at most %s", window, MaxSuspendAfterDetach)
		}
		if p.Spec.Suspend == nil || p.Spec.Suspend.Mode != SubstrateSuspendModeDataOnly {
			return fmt.Errorf("suspendAfterDetach requires suspend.mode DataOnly")
		}
	}
	if p.Spec.Placement != nil && p.Spec.Placement.SharedWorkers != nil {
		shared := p.Spec.Placement.SharedWorkers
		if shared.MaxActorsPerWorker < MinSharedActorsPerWorker || shared.MaxActorsPerWorker > MaxSharedActorsPerWorker {
			return fmt.Errorf("sharedWorkers.maxActorsPerWorker %d must be from %d to %d", shared.MaxActorsPerWorker, MinSharedActorsPerWorker, MaxSharedActorsPerWorker)
		}
		if lifetime := shared.WorkerLifetime(); lifetime%time.Second != 0 {
			return fmt.Errorf("sharedWorkers.maxWorkerLifetime %s must be a whole number of seconds", lifetime)
		}
		if lifetime := shared.WorkerLifetime(); lifetime < MinSharedWorkerLifetime || lifetime > MaxSharedWorkerLifetime {
			return fmt.Errorf("sharedWorkers.maxWorkerLifetime %s must be at least %s and at most %s", lifetime, MinSharedWorkerLifetime, MaxSharedWorkerLifetime)
		}
		if p.Spec.Suspend == nil || p.Spec.Suspend.Mode != SubstrateSuspendModeDataOnly {
			return fmt.Errorf("sharedWorkers requires suspend.mode DataOnly")
		}
	}
	return nil
}
