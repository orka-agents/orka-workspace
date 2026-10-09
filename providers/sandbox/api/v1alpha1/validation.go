package v1alpha1

import (
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Validate checks profile-local requirements. Admission must also verify class
// expiry/reuse policy and the live StorageClass and pin its immutable UID.
func (p *SandboxWorkspaceProfile) Validate() error {
	if p.Spec.Retention != nil && p.Spec.Retention.MaxSuspendedWorkspaces != nil && *p.Spec.Retention.MaxSuspendedWorkspaces < 0 {
		return fmt.Errorf("maxSuspendedWorkspaces must not be negative")
	}
	if p.Spec.Suspend != nil {
		_, err := p.Spec.Suspend.ResolveVolume()
		return err
	}
	return nil
}

// ResolveVolume validates the admitted mode and returns the same normalized PVC
// shape used by the legacy class binding. It does not mutate the stored profile.
func (s *SandboxSuspendPolicy) ResolveVolume() (SandboxDurableVolume, error) {
	if s.Mode != SandboxSuspendModeDataOnly {
		return SandboxDurableVolume{}, fmt.Errorf("suspend mode %q is not supported; only DataOnly is admitted", s.Mode)
	}
	result := s.Volume
	result.Capacity = strings.TrimSpace(result.Capacity)
	quantity, err := resource.ParseQuantity(result.Capacity)
	if err != nil || quantity.Sign() <= 0 {
		return SandboxDurableVolume{}, fmt.Errorf("capacity %q must be a positive storage quantity", s.Volume.Capacity)
	}
	result.AccessModes = slices.Clone(s.Volume.AccessModes)
	if len(result.AccessModes) == 0 {
		result.AccessModes = []string{"ReadWriteOnce"}
	}
	for _, mode := range result.AccessModes {
		switch mode {
		case "ReadWriteOnce", "ReadWriteOncePod", "ReadWriteMany":
		default:
			return SandboxDurableVolume{}, fmt.Errorf("access mode %q is not writable", mode)
		}
	}
	slices.Sort(result.AccessModes)
	result.StorageClassName = strings.TrimSpace(result.StorageClassName)
	if result.StorageClassName != "" && len(validation.IsDNS1123Subdomain(result.StorageClassName)) != 0 {
		return SandboxDurableVolume{}, fmt.Errorf("storage class %q is invalid", s.Volume.StorageClassName)
	}
	return result, nil
}
