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
	"k8s.io/apimachinery/pkg/util/validation"
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
// Credential header replacements compile into gateway header effects that
// name Kubernetes Secret keys; the credential value never passes through this
// provider. Without a configured credential provider any replacement fails
// compilation, so the Actor is never resumed with a rule the gateway would
// forward without its credential.
func compileEgressPolicy(atespace, workloadNamespace string, egress *api.RuntimeEgressPolicy, config Config) (*pb.EgressPolicy, error) {
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
		effects, err := compileCredentialHeaders(workloadNamespace, rule.ReplaceHeaders, config)
		if err != nil {
			return nil, err
		}
		switch rule.Protocol {
		case api.RuntimeEgressHTTP:
			policy.Rules = append(policy.Rules, &pb.EgressRule{Http: &pb.HttpRule{Hostnames: hosts, Ports: ports, Effects: effects}})
		case api.RuntimeEgressHTTPS:
			policy.Rules = append(policy.Rules, &pb.EgressRule{Https: &pb.HttpsRule{Hostnames: hosts, Ports: ports, Effects: effects}})
		case api.RuntimeEgressTLSPassthrough:
			if effects != nil {
				return nil, fmt.Errorf("native TLS passthrough cannot replace credential headers")
			}
			policy.Rules = append(policy.Rules, &pb.EgressRule{TlsPassthrough: &pb.TlsPassthroughRule{Hostnames: hosts, Ports: ports}})
		default:
			return nil, fmt.Errorf("native egress protocol %q is unsupported", rule.Protocol)
		}
	}
	return policy, nil
}

// compileCredentialHeaders maps header replacements to the upstream
// ate-secret://k8s.io/default/<namespace>/<secret>/<key> references served by
// the Kubernetes Secrets credential provider.
func compileCredentialHeaders(workloadNamespace string, headers []api.RuntimeEgressCredentialHeader, config Config) (*pb.HttpRuleEffects, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	if !config.EgressCredentialInjectionEnabled() {
		return nil, fmt.Errorf("native egress credential injection is not configured for this provider")
	}
	effects := &pb.HttpRuleEffects{}
	for _, header := range headers {
		namespace, err := config.credentialNamespace(workloadNamespace, header.SecretKeyRef)
		if err != nil {
			return nil, err
		}
		uri := fmt.Sprintf("ate-secret://%s/default/%s/%s/%s", config.EgressCredentialProvider, namespace, header.SecretKeyRef.Name, header.SecretKeyRef.Key)
		effects.ReplaceHeaders = append(effects.ReplaceHeaders, &pb.CredentialHeader{Header: header.Header, Prefix: header.Prefix, CredentialUri: uri})
	}
	return effects, nil
}

// credentialNamespace resolves a Secret reference's namespace: the workload's
// own, or one the operator explicitly listed for this provider.
func (c Config) credentialNamespace(workloadNamespace string, ref api.RuntimeEgressSecretKeyReference) (string, error) {
	namespace := ref.Namespace
	if namespace == "" || namespace == workloadNamespace {
		if len(validation.IsDNS1123Label(workloadNamespace)) != 0 {
			return "", fmt.Errorf("native egress credential requires a workload namespace")
		}
		return workloadNamespace, nil
	}
	if !slices.Contains(c.EgressCredentialNamespaces, namespace) {
		return "", fmt.Errorf("native egress credential namespace %q is not permitted for this provider", namespace)
	}
	return namespace, nil
}

// admitEgressCredentials refuses a workload whose credential replacements this
// installation cannot enforce, before any native object is created.
func (c Config) admitEgressCredentials(request sdk.WorkloadRequest) error {
	if request.Runtime == nil || !request.Runtime.Egress.HasCredentialInjection() {
		return nil
	}
	if !slices.Contains(request.Runtime.RequiredFeatures, api.WorkspaceFeatureEgressCredentialInjection) {
		return fmt.Errorf("native egress credential replacement requires the %s feature", api.WorkspaceFeatureEgressCredentialInjection)
	}
	for _, rule := range request.Runtime.Egress.Rules {
		if _, err := compileCredentialHeaders(request.Key.Namespace, rule.ReplaceHeaders, c); err != nil {
			return err
		}
	}
	return nil
}

func egressRulesDigest(rules []*pb.EgressRule) (string, error) {
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&pb.EgressPolicy{Rules: rules})
	return digest(data), err
}

// ensureEgressPolicy installs the compiled allowlist on the suspended Actor
// before every Resume. A compilation failure installs deny-all and refuses to
// boot. Writes carry the observed policy's metadata as preconditions.
func (d *Lifecycle) ensureEgressPolicy(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	desired, compileErr := compileEgressPolicy(record.Atespace, record.Request.Key.Namespace, record.Request.Runtime.Egress, d.config)
	return d.applyEgressPolicy(ctx, cm, record, desired, compileErr)
}

// denyAllEgressPolicy installs the empty policy unconditionally. Revocation
// never re-applies compiled rules: a journal that fails admission for any
// reason must not keep or change credential-bearing effects.
func (d *Lifecycle) denyAllEgressPolicy(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, cause error) error {
	return d.applyEgressPolicy(ctx, cm, record, nil, cause)
}

func (d *Lifecycle) applyEgressPolicy(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, desired *pb.EgressPolicy, compileErr error) error {
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

// revokeWithdrawnEgressCredentials runs when observation or workload
// validation fails. Either can stop reconciliation before EnsureAllocation's
// revocation, so a journaled allowlist that no longer admits or compiles -
// withdrawn provider configuration or a header this provider now reserves -
// would otherwise keep credential-bearing gateway effects applied. The journal
// is read without re-validating its request, which may be what became invalid;
// compilation then fails and the Actor's policy becomes deny-all.
func (d *Lifecycle) revokeWithdrawnEgressCredentials(ctx context.Context, key sdk.AllocationKey) error {
	// An unreadable journal is already the reported error; only a revocation
	// that was actually attempted can add a distinct failure.
	cm, record, err := d.readForEgressRevocation(ctx, key)
	if err != nil || record.Request.Runtime == nil || !record.Request.Runtime.Egress.HasCredentialInjection() {
		return nil
	}
	if record.EgressPolicy.UID == "" || record.EgressDenyAll {
		return nil
	}
	cause := d.config.admitEgressCredentials(record.Request)
	if cause == nil {
		if _, cause = compileEgressPolicy(record.Atespace, record.Request.Key.Namespace, record.Request.Runtime.Egress, d.config); cause == nil {
			return nil
		}
	}
	return d.denyAllEgressPolicy(ctx, cm, record, cause)
}

// verifyEgressPolicy read-only checks the exact applied allowlist.
func (d *Lifecycle) verifyEgressPolicy(ctx context.Context, record *journalRecord) error {
	if record.EgressPolicy.UID == "" || record.EgressPolicy.Digest == "" || record.EgressDenyAll {
		return fmt.Errorf("native Actor has no applied admitted egress allowlist")
	}
	desired, err := compileEgressPolicy(record.Atespace, record.Request.Key.Namespace, record.Request.Runtime.Egress, d.config)
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
