package substrate

import (
	"context"
	"errors"
	"fmt"
	"slices"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
)

// egressPolicyName is the singleton name of an Actor's nested egress policy.
const egressPolicyName = "default"

// validateNativeEgress rejects the Kubernetes NetworkPolicy shape, which the
// v1 backend cannot enforce: Actor egress always leaves through the native
// egress gateway, so only a host-name allowlist describes it.
func validateNativeEgress(runtime *sdk.RuntimeWorkload) error {
	if runtime.NetworkPolicy != nil {
		return fmt.Errorf("native runtime cannot enforce Kubernetes NetworkPolicy egress; Agent Substrate v1 requires an admitted host-name egress allowlist")
	}
	if runtime.Egress == nil {
		return fmt.Errorf("native runtime requires an admitted host-name egress allowlist")
	}
	return runtime.Egress.Validate()
}

// compileEgressPolicy translates the admitted allowlist into one native rule
// per host pattern. Everything else stays denied, including DNS resolution.
func compileEgressPolicy(atespace string, egress *api.RuntimeEgressPolicy) (*pb.EgressPolicy, error) {
	if egress == nil {
		return nil, fmt.Errorf("native runtime has no admitted egress allowlist")
	}
	if err := egress.Validate(); err != nil {
		return nil, err
	}
	policy := &pb.EgressPolicy{Metadata: &pb.ResourceMetadata{Atespace: atespace, Name: egressPolicyName}}
	for _, rule := range egress.Rules {
		ports := &pb.Ports{Numbers: slices.Sorted(slices.Values(rule.Ports))}
		hosts := []string{rule.Host}
		switch rule.Protocol {
		case api.RuntimeEgressHTTP:
			policy.Rules = append(policy.Rules, &pb.EgressRule{Http: &pb.HttpRule{Hostnames: hosts, Ports: ports}})
		case api.RuntimeEgressHTTPS:
			policy.Rules = append(policy.Rules, &pb.EgressRule{Https: &pb.HttpsRule{Hostnames: hosts, Ports: ports}})
		case api.RuntimeEgressTLSPassthrough:
			policy.Rules = append(policy.Rules, &pb.EgressRule{TlsPassthrough: &pb.TlsPassthroughRule{Hostnames: hosts, Ports: ports}})
		default:
			return nil, fmt.Errorf("native egress protocol %q is unsupported", rule.Protocol)
		}
	}
	return policy, nil
}

func egressRulesDigest(rules []*pb.EgressRule) (string, error) {
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&pb.EgressPolicy{Rules: rules})
	return digest(data), err
}

// ensureEgressPolicy installs the compiled allowlist on the suspended Actor
// before every Resume. A compilation failure installs deny-all and refuses to
// boot. Writes carry the observed policy's metadata as preconditions.
func (d *Lifecycle) ensureEgressPolicy(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	desired, compileErr := compileEgressPolicy(record.Atespace, record.Request.Runtime.Egress)
	if compileErr != nil {
		desired = &pb.EgressPolicy{Metadata: &pb.ResourceMetadata{Atespace: record.Atespace, Name: egressPolicyName}}
	}
	hash, err := egressRulesDigest(desired.GetRules())
	if err != nil {
		return err
	}
	ref := actorRef(record)
	existing, err := d.control.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: ref, Name: egressPolicyName})
	switch {
	case status.Code(err) == codes.NotFound:
		if record.EgressPolicy.UID != "" {
			return fmt.Errorf("native Actor egress policy disappeared: %w", sdk.ErrStaleIdentity)
		}
		if !record.EgressPolicy.CreateIssued {
			record.EgressPolicy.CreateIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		existing, err = d.control.CreateActorEgressPolicy(ctx, &pb.CreateActorEgressPolicyRequest{Actor: ref, EgressPolicy: desired})
		if status.Code(err) == codes.AlreadyExists {
			return fmt.Errorf("native Actor egress policy creation raced; retrying from observation")
		}
		if err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		if existing.GetMetadata().GetUid() == "" || record.EgressPolicy.UID != "" && existing.GetMetadata().GetUid() != record.EgressPolicy.UID {
			return fmt.Errorf("native Actor egress policy lifetime changed: %w", sdk.ErrStaleIdentity)
		}
		current, err := egressRulesDigest(existing.GetRules())
		if err != nil {
			return err
		}
		if current != hash {
			update := &pb.EgressPolicy{Metadata: proto.Clone(existing.GetMetadata()).(*pb.ResourceMetadata), Rules: desired.GetRules()}
			existing, err = d.control.UpdateActorEgressPolicy(ctx, &pb.UpdateActorEgressPolicyRequest{Actor: ref, EgressPolicy: update})
			if err != nil {
				return err
			}
		}
	}
	applied, err := egressRulesDigest(existing.GetRules())
	if err != nil {
		return err
	}
	meta := existing.GetMetadata()
	if meta.GetUid() == "" || meta.GetAtespace() != record.Atespace || meta.GetName() != egressPolicyName || applied != hash {
		return fmt.Errorf("native Actor egress policy differs from the admitted allowlist: %w", sdk.ErrStaleIdentity)
	}
	if record.EgressPolicy.UID != meta.GetUid() || record.EgressPolicy.Digest != hash || record.EgressDenyAll != (compileErr != nil) {
		record.EgressPolicy.UID = meta.GetUid()
		record.EgressPolicy.Digest = hash
		record.EgressDenyAll = compileErr != nil
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	if compileErr != nil {
		return fmt.Errorf("native egress allowlist did not compile; the Actor denies all egress and is not resumed: %w", compileErr)
	}
	return nil
}

// verifyEgressPolicy read-only checks the exact applied allowlist.
func (d *Lifecycle) verifyEgressPolicy(ctx context.Context, record *journalRecord) error {
	if record.EgressPolicy.UID == "" || record.EgressPolicy.Digest == "" || record.EgressDenyAll {
		return fmt.Errorf("native Actor has no applied admitted egress allowlist")
	}
	desired, err := compileEgressPolicy(record.Atespace, record.Request.Runtime.Egress)
	if err != nil {
		return err
	}
	hash, err := egressRulesDigest(desired.GetRules())
	if err != nil {
		return err
	}
	existing, err := d.control.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: actorRef(record), Name: egressPolicyName})
	if status.Code(err) == codes.NotFound {
		return errors.New("native Actor egress policy disappeared")
	}
	if err != nil {
		return err
	}
	applied, err := egressRulesDigest(existing.GetRules())
	if err != nil {
		return err
	}
	if existing.GetMetadata().GetUid() != record.EgressPolicy.UID || applied != record.EgressPolicy.Digest || hash != record.EgressPolicy.Digest {
		return fmt.Errorf("native Actor egress policy changed from the admitted allowlist: %w", sdk.ErrStaleIdentity)
	}
	return nil
}
