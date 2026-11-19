package substrate

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Shared workers (provider ADR 0008, Orka ADR 0056) let up to N data-only
// Actors of one namespace, profile and Actor size share one gVisor worker.
// Upstream has no per-Actor sandbox-exit attestation, so a per-Actor stop is
// proven with the owner-approved interim evidence: egress revoked before
// suspension, the Actor SUSPENDED at a later version with the expected VOLUMES
// snapshot, and its assignment absent from the same fenced worker at an
// unchanged epoch and version. The residue is bounded by a maximum worker
// lifetime, and any doubt takes the strong path: the pool is closed and
// deleted, and every hosted Actor proves exact worker Pod UID absence.
const (
	// sharedPoolLabel carries a shared pool's random, journaled value. The
	// derived template selects it, so only allocations that claimed a slot in
	// that pool's ledger can be placed there.
	sharedPoolLabel = "substrate.workspace.orka.ai/shared-pool"
	// sharedRetireIntentAnnotation records a decided strong retirement whose
	// Delete has not succeeded yet. It is written by a merge patch, separate
	// from the ledger, so it survives a failed ledger write; admission
	// refuses the pool and the bounds loop retries the Delete.
	sharedRetireIntentAnnotation = "substrate.workspace.orka.ai/shared-retire"
	// sharedLedgerAnnotation is the pool's resourceVersion-protected ledger.
	sharedLedgerAnnotation = "substrate.workspace.orka.ai/shared-ledger"
	// sharedDeadlineAnnotation is the pool's hard lifetime deadline (RFC 3339),
	// written once at creation and never by a ledger update, so a corrupted or
	// rewritten ledger can never extend the bound.
	sharedDeadlineAnnotation = "substrate.workspace.orka.ai/shared-deadline"
	sharedLedgerVersion      = "substrate.workspace.shared-ledger.v1"
	sharedPoolPrefix         = "ate-shared-"
	// sharedIdleGrace keeps an empty admitting pool warm for a follow-up turn.
	sharedIdleGrace = 5 * time.Minute
	// sharedMaxProofAttempts bounds interim proofs that keep observing a
	// changing worker version; then the strong path applies.
	sharedMaxProofAttempts = 5
	// sharedLedgerMaxBytes bounds the ledger annotation. Its fields are
	// bounded below so that any valid ledger, with a full retirement reason,
	// stays far under it: a retirement write can never overflow.
	sharedLedgerMaxBytes = 32 << 10
	// sharedLedgerFieldMax bounds each scope field and entry identity.
	sharedLedgerFieldMax = 128
	// sharedLedgerJournalMax bounds an entry's journal marker
	// (<namespace>/<name>/<uid>/<sequence>).
	sharedLedgerJournalMax = 256
	// sharedLedgerReasonMax bounds the recorded retirement reason.
	sharedLedgerReasonMax = 512
	// sharedLedgerRetirementHeadroom is reserved below sharedLedgerMaxBytes
	// for the retirement state and reason.
	sharedLedgerRetirementHeadroom = 1 << 10
	// operatorTaintAnnotation is Orka core's durable operator-access marker
	// on an ExecutionWorkspace (Orka ADR 0044).
	operatorTaintAnnotation = "acp.workspace.orka.ai/operator-tainted"
	// workerPodTerminationGrace is the termination grace Agent Substrate
	// v1.0.0 hardcodes on every worker Pod
	// (cmd/atecontroller/internal/controllers/workerpool_apply.go,
	// workerTerminationGracePeriodSeconds). The worker drains its Actors for
	// at most 30 minutes of it (cmd/ateworker-gvisor/main.go,
	// workloadGracePeriod).
	workerPodTerminationGrace = 3600 * time.Second
	// sharedRetireMargin is kept between the end of the worker Pod's grace
	// and maxWorkerLifetime; at lifetime minus this margin a worker Pod that
	// still exists is re-deleted with a one-second grace.
	sharedRetireMargin = 5 * time.Minute
	// sharedAdmissionLead closes admission this long before strong retirement
	// begins, so a newly admitted Actor has time to run.
	sharedAdmissionLead = time.Hour
	// dnsRelayPolicyTTL is the worker DNS relay's fixed policy cache
	// (internal/atunnel/egresspolicy.go, egressPolicyTTL).
	dnsRelayPolicyTTL = 5 * time.Second
	// sharedEgressSettleMargin is added to the longest policy cache.
	sharedEgressSettleMargin = 5 * time.Second
	// sharedEgressSettle is the settle window with the default gateway TTL.
	sharedEgressSettle = upstreamEgressPolicyCacheTTL + sharedEgressSettleMargin
	// sharedEscalationGrace is the grace of the escalating re-delete. One
	// second, not zero: a zero grace removes the Pod object without the
	// kubelet confirming its containers stopped, which would forge the
	// absence proof.
	sharedEscalationGrace int64 = 1
)

// errSharedWorkersNotDedicated refuses a shared-worker profile on a provider
// whose operator has not asserted a dedicated Substrate install.
var errSharedWorkersNotDedicated = errors.New("SubstrateWorkspaceProfile requests sharedWorkers, but this provider was not started with --substrate-dedicated-install; shared workers are only allowed on an Agent Substrate install dedicated to this Orka")

// Ledger admission states. Draining admits no new claims but lets existing
// claimants boot; Retiring and Quarantined pools are being deleted.
const (
	sharedAdmitting   = "Admitting"
	sharedDraining    = "Draining"
	sharedRetiring    = "Retiring"
	sharedQuarantined = "Quarantined"
)

// sharedNow is the provider clock for ledger birth, admission and lifetime.
var sharedNow = time.Now

// sharedScope is everything a shared pool is confined to. Two allocations
// share a pool only when their scopes are equal.
type sharedScope struct {
	Namespace     string          `json:"namespace"`
	Atespace      string          `json:"atespace"`
	ProfileUID    string          `json:"profileUID"`
	ProfileHash   string          `json:"profileHash"`
	SourceUID     string          `json:"sourceUID"`
	Shape         string          `json:"shape"`
	CPUMillicores int64           `json:"cpuMillicores"`
	MemoryBytes   int64           `json:"memoryBytes"`
	MaxActors     int32           `json:"maxActors"`
	Lifetime      metav1.Duration `json:"lifetime"`
}

func (s sharedScope) valid() bool {
	for _, field := range []string{s.Namespace, s.Atespace, s.ProfileUID, s.ProfileHash, s.SourceUID, s.Shape} {
		if len(field) > sharedLedgerFieldMax {
			return false
		}
	}
	return s.Namespace != "" && s.Atespace != "" && s.ProfileUID != "" && s.ProfileHash != "" && s.SourceUID != "" && s.Shape != "" &&
		s.CPUMillicores > 0 && s.MemoryBytes > 0 && s.MaxActors >= profile.MinSharedActorsPerWorker && s.MaxActors <= profile.MaxSharedActorsPerWorker &&
		s.Lifetime.Duration >= profile.MinSharedWorkerLifetime && s.Lifetime.Duration <= profile.MaxSharedWorkerLifetime
}

// capacity is what a worker sized for MaxActors Actors reports.
func (s sharedScope) capacity() (cpu, memory int64) {
	return s.CPUMillicores * int64(s.MaxActors), s.MemoryBytes * int64(s.MaxActors)
}

// sharedWorkerIdentity is a worker process lifetime: a restart registers a
// higher epoch and loses every sandbox, so it is a different identity.
type sharedWorkerIdentity struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	PodUID string `json:"podUID"`
	Epoch  int64  `json:"epoch"`
}

type sharedLedgerEntry struct {
	// Journal is the exact journal lifetime and sequence holding the slot.
	Journal string `json:"journal"`
	// Actor is <atespace>/<name>; ActorUID is written before Resume, so an
	// assignment never precedes its entry.
	Actor    string `json:"actor"`
	ActorUID string `json:"actorUID,omitempty"`
}

type sharedLedger struct {
	Version string      `json:"version"`
	Scope   sharedScope `json:"scope"`
	BornAt  metav1.Time `json:"bornAt"`
	State   string      `json:"state"`
	Reason  string      `json:"reason,omitempty"`
	// Worker is the pool's first fenced worker. A different worker in the
	// pool means its Pod was replaced, which is doubt.
	Worker     *sharedWorkerIdentity `json:"worker,omitempty"`
	EmptySince *metav1.Time          `json:"emptySince,omitempty"`
	Entries    []sharedLedgerEntry   `json:"entries"`
}

// sharedSuspendFence pins the suspend the interim evidence is about.
type sharedSuspendFence struct {
	Issued              bool   `json:"issued,omitempty"`
	PriorVersion        int64  `json:"priorVersion,omitempty"`
	PriorGeneration     int32  `json:"priorGeneration,omitempty"`
	PriorSnapshotDigest string `json:"priorSnapshotDigest,omitempty"`
	ObservedVersion     int64  `json:"observedVersion,omitempty"`
	SnapshotDigest      string `json:"snapshotDigest,omitempty"`
}

// sharedTerminationProof is the journaled interim evidence.
type sharedTerminationProof struct {
	WorkerUID     string           `json:"workerUID"`
	Epoch         int64            `json:"epoch"`
	WorkerVersion int64            `json:"workerVersion"`
	ActorVersion  int64            `json:"actorVersion"`
	At            metav1.MicroTime `json:"at"`
}

// sharedPlacement is the journal's record of a shared pool slot.
type sharedPlacement struct {
	Scope sharedScope `json:"scope"`
	// Pool and Label identify the selected pool; UID is saved only after
	// this record's own entry is observed in its ledger.
	Pool       string `json:"pool"`
	UID        string `json:"uid,omitempty"`
	Label      string `json:"label"`
	SpecDigest string `json:"specDigest"`
	// Create is the exact specification when this record creates the pool.
	Create       json.RawMessage `json:"create,omitempty"`
	CreateIssued bool            `json:"createIssued,omitempty"`
	ClaimIssued  bool            `json:"claimIssued,omitempty"`
	// OperatorLabels are the operator pool's labels, kept as resume and
	// checkpoint provenance; the shared pool itself never carries them.
	OperatorLabels map[string]string `json:"operatorLabels,omitempty"`
	// EgressRevoked records the verified deny-all egress before suspension,
	// and EgressRevokedAt when it was first stored.
	EgressRevoked   bool                    `json:"egressRevoked,omitempty"`
	EgressRevokedAt *metav1.MicroTime       `json:"egressRevokedAt,omitempty"`
	Suspend         *sharedSuspendFence     `json:"suspend,omitempty"`
	Proof           *sharedTerminationProof `json:"proof,omitempty"`
	ProofAttempts   int                     `json:"proofAttempts,omitempty"`
	// Strong is why retirement takes the strong Pod-absence path. Once set,
	// it is never cleared.
	Strong   string `json:"strong,omitempty"`
	Released bool   `json:"released,omitempty"`
}

var errSharedEgressRevoked = errors.New("shared worker retirement revokes egress before suspension")

// errSharedLedgerInvalid marks a pool whose ledger annotation cannot be read.
var errSharedLedgerInvalid = errors.New("shared worker ledger is invalid")

func randomLabel() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}

func sharedLabels(label string) map[string]string {
	return map[string]string{sharedPoolLabel: label}
}

func actorKey(record *journalRecord) string { return record.Atespace + "/" + record.Actor.Name }

// sharedPolicyFor returns the profile's shared-worker policy when this
// allocation may share. Everything that is not data-only and gateway-credentialed,
// every restore from a public checkpoint, and every tainted, quarantined or
// failed workspace keeps the private single-Actor path.
//
// A profile that opts in on a provider without --substrate-dedicated-install is
// refused outright: placement upstream checks no tenant, so only an install
// dedicated to this Orka may share.
func (d *Lifecycle) sharedPolicyFor(object *profile.SubstrateWorkspaceProfile, workspace *api.ExecutionWorkspace, record *journalRecord) (*profile.SharedWorkersPolicy, error) {
	if object.Spec.Placement == nil || object.Spec.Placement.SharedWorkers == nil {
		return nil, nil
	}
	if !d.config.DedicatedInstall {
		return nil, errSharedWorkersNotDedicated
	}
	request := record.Request
	if !record.SuspendEnabled || request.RestoreFrom != nil || request.Runtime == nil {
		return nil, nil
	}
	// The provider-proxy token must never enter a shared sandbox: a leaked
	// sandbox may keep it in memory after its Actor stopped.
	if !request.Runtime.Egress.HasCredentialInjection() || !slices.Contains(request.Runtime.RequiredFeatures, api.WorkspaceFeatureEgressCredentialInjection) {
		return nil, nil
	}
	if workspaceSuspect(workspace) {
		return nil, nil
	}
	return object.Spec.Placement.SharedWorkers, nil
}

// workspaceSuspect reports a tainted, quarantined or failed workspace.
func workspaceSuspect(workspace *api.ExecutionWorkspace) bool {
	if _, tainted := workspace.Annotations[operatorTaintAnnotation]; tainted {
		return true
	}
	return workspace.Labels[api.QuarantinedLabel] != "" || workspace.Spec.DesiredState == api.ExecutionWorkspaceDesiredQuarantined ||
		workspace.Status.State == api.ExecutionWorkspaceStateQuarantined || workspace.Status.State == api.ExecutionWorkspaceStateFailed
}

// buildSharedPoolSpec is buildPrivatePoolSpec sized for scope.MaxActors.
func buildSharedPoolSpec(source *unstructured.Unstructured, scope sharedScope, label string) (json.RawMessage, error) {
	if scope.MaxActors < 1 || scope.CPUMillicores > math.MaxInt64/int64(scope.MaxActors) || scope.MemoryBytes > (math.MaxInt64-gvisorReservedMemoryBytes)/int64(scope.MaxActors) {
		return nil, fmt.Errorf("shared worker size overflows")
	}
	cpu, memory := scope.capacity()
	return buildPrivatePoolSpec(source, cpu, memory, sharedLabels(label))
}

// prepareSharedPlacement provisionally binds a new record to a pool it would
// create itself. ensureSharedRuntimePool may still move it to an existing
// pool with a free slot before the template exists.
func (d *Lifecycle) prepareSharedPlacement(record *journalRecord, source *unstructured.Unstructured, policy *profile.SharedWorkersPolicy, profileUID types.UID) error {
	shape, err := runtimePoolRestoreDigest(record.RuntimePoolSpec, false)
	if err != nil {
		return err
	}
	cpu, memory := expectedWorkerCapacity(record)
	scope := sharedScope{Namespace: record.Request.Key.Namespace, Atespace: record.Atespace, ProfileUID: string(profileUID), ProfileHash: record.Request.ParametersBinding.ProfileHash, SourceUID: string(source.GetUID()), Shape: shape, CPUMillicores: cpu, MemoryBytes: memory, MaxActors: policy.MaxActorsPerWorker, Lifetime: metav1.Duration{Duration: policy.WorkerLifetime()}}
	if !scope.valid() {
		return fmt.Errorf("shared worker policy is invalid")
	}
	// The operator's own labels are taken while the record still carries
	// its private birth labels.
	operator := operatorPoolLabels(record)
	record.SharedWorker = &sharedPlacement{Scope: scope, OperatorLabels: operator}
	return d.bindNewSharedPool(record, source)
}

// sharedScopeCommitment is the part of a shared pool's name that commits to
// its scope: a domain-separated SHA-256 of the canonical scope, which includes
// the profile UID, profile hash and lifetime. A pool's name is immutable and
// set when it is created, so unlike the ledger it cannot be rewritten.
func sharedScopeCommitment(scope sharedScope) string {
	data, err := json.Marshal(scope)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte("orka.ai/substrate-shared-pool-scope/v1\x00"), data...))
	return hex.EncodeToString(sum[:sharedCommitmentBytes])
}

// sharedCommitmentBytes and sharedNonceBytes size a committed pool name:
// ate-shared-<16 hex commitment>-<16 hex nonce>, 44 characters.
const (
	sharedCommitmentBytes = 8
	sharedNonceBytes      = 8
)

// newSharedPoolName returns a fresh pool name committing to scope.
func newSharedPoolName(scope sharedScope) (string, error) {
	commitment := sharedScopeCommitment(scope)
	if commitment == "" {
		return "", fmt.Errorf("shared worker scope cannot be committed")
	}
	var nonce [sharedNonceBytes]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return sharedPoolPrefix + commitment + "-" + hex.EncodeToString(nonce[:]), nil
}

// sharedNameCommitment returns the scope commitment in a pool's name, and
// false for a name that does not follow the committed scheme (a pool created
// before it), which commits to nothing.
func sharedNameCommitment(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, sharedPoolPrefix)
	if !ok {
		return "", false
	}
	commitment, nonce, ok := strings.Cut(rest, "-")
	if !ok || len(commitment) != 2*sharedCommitmentBytes || len(nonce) != 2*sharedNonceBytes {
		return "", false
	}
	if _, err := hex.DecodeString(commitment); err != nil {
		return "", false
	}
	if _, err := hex.DecodeString(nonce); err != nil {
		return "", false
	}
	return commitment, true
}

// sharedNameBinding reports whether a pool's name commits to scope
// (committed and bound), commits to another scope (committed, not bound:
// doubt), or follows no commitment scheme (not committed).
func sharedNameBinding(pool *unstructured.Unstructured, scope sharedScope) (committed, bound bool) {
	commitment, committed := sharedNameCommitment(pool.GetName())
	return committed, committed && commitment == sharedScopeCommitment(scope)
}

// bindNewSharedPool rebinds the record to a fresh pool of its own.
func (d *Lifecycle) bindNewSharedPool(record *journalRecord, source *unstructured.Unstructured) error {
	name, err := newSharedPoolName(record.SharedWorker.Scope)
	if err != nil {
		return err
	}
	label, err := randomLabel()
	if err != nil {
		return err
	}
	spec, err := buildSharedPoolSpec(source, record.SharedWorker.Scope, label)
	if err != nil {
		return err
	}
	shared := record.SharedWorker
	*shared = sharedPlacement{Scope: shared.Scope, OperatorLabels: shared.OperatorLabels, Pool: name, Label: label, SpecDigest: digest(spec), Create: spec}
	return bindSharedPool(record)
}

// bindSharedPool sets the record's pool reference, its pool labels, and the
// template's worker selector. Agent Substrate copies WorkerPool labels onto
// Workers and schedules only by those labels
// (cmd/atecontroller/internal/workersync/syncer.go,
// cmd/ateapi/internal/scheduling/scheduling.go), so a shared pool carries
// only the provider-owned random label, never the operator pool's labels a
// foreign template might select. Sandbox class and config, image and
// scheduling come from the copied spec, not from labels.
func bindSharedPool(record *journalRecord) error {
	shared := record.SharedWorker
	record.RuntimePool = nativeReference{Name: shared.Pool, UID: shared.UID, Digest: shared.SpecDigest}
	record.RuntimePoolLabels = sharedLabels(shared.Label)
	record.TemplateSpec.WorkerSelector = &pb.Selector{MatchLabels: sharedLabels(shared.Label)}
	var err error
	record.Template.Digest, err = templateDigest(record.TemplateSpec)
	return err
}

func readSharedLedger(pool *unstructured.Unstructured) (*sharedLedger, error) {
	raw := pool.GetAnnotations()[sharedLedgerAnnotation]
	if raw == "" || len(raw) > sharedLedgerMaxBytes {
		return nil, fmt.Errorf("shared worker ledger is missing or oversized")
	}
	ledger := &sharedLedger{}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(ledger); err != nil || ledger.Version != sharedLedgerVersion || !ledger.Scope.valid() || ledger.BornAt.IsZero() {
		return nil, fmt.Errorf("shared worker ledger is invalid")
	}
	switch ledger.State {
	case sharedAdmitting, sharedDraining, sharedRetiring, sharedQuarantined:
	default:
		return nil, fmt.Errorf("shared worker ledger state is invalid")
	}
	if len(ledger.Entries) > int(ledger.Scope.MaxActors) {
		return nil, fmt.Errorf("shared worker ledger exceeds its slots")
	}
	if len(ledger.Reason) > sharedLedgerReasonMax {
		return nil, fmt.Errorf("shared worker ledger reason is oversized")
	}
	if worker := ledger.Worker; worker != nil && (len(worker.Name) > sharedLedgerFieldMax || len(worker.UID) > sharedLedgerFieldMax || len(worker.PodUID) > sharedLedgerFieldMax) {
		return nil, fmt.Errorf("shared worker ledger worker identity is oversized")
	}
	// Every variable-length field is bounded above, so an accepted ledger
	// always leaves room for its retirement state and reason; this is checked
	// again explicitly so a future field cannot erode that headroom.
	// Measured on the size this provider would write back, so no escaping
	// or re-encoding can make an accepted ledger overflow on retirement.
	reencoded, err := encodeSharedLedger(ledger)
	if err != nil || len(raw) > sharedLedgerMaxBytes-sharedLedgerRetirementHeadroom || len(reencoded) > sharedLedgerMaxBytes-sharedLedgerRetirementHeadroom {
		return nil, fmt.Errorf("shared worker ledger leaves no room for its retirement write")
	}
	seen := map[string]bool{}
	for _, entry := range ledger.Entries {
		if len(entry.Journal) > sharedLedgerJournalMax || len(entry.Actor) > sharedLedgerFieldMax || len(entry.ActorUID) > sharedLedgerFieldMax {
			return nil, fmt.Errorf("shared worker ledger entry is oversized")
		}
		if entry.Journal == "" || entry.Actor == "" || seen[entry.Journal] || seen["actor:"+entry.Actor] {
			return nil, fmt.Errorf("shared worker ledger has a duplicate or empty entry")
		}
		seen[entry.Journal], seen["actor:"+entry.Actor] = true, true
	}
	return ledger, nil
}

// encodeSharedLedger is the one ledger encoding, without HTML escaping, so a
// field never expands to several times its length (json.Marshal escapes
// <, > and & as six-byte sequences).
func encodeSharedLedger(ledger *sharedLedger) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(ledger); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

func setSharedLedger(pool *unstructured.Unstructured, ledger *sharedLedger) error {
	data, err := encodeSharedLedger(ledger)
	if err != nil {
		return err
	}
	if len(data) > sharedLedgerMaxBytes {
		return fmt.Errorf("shared worker ledger exceeds its size bound")
	}
	annotations := maps.Clone(pool.GetAnnotations())
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[sharedLedgerAnnotation] = string(data)
	pool.SetAnnotations(annotations)
	return nil
}

// sharedBorn is the earlier of the ledger's birth and the pool's creation.
func sharedBorn(pool *unstructured.Unstructured, ledger *sharedLedger) time.Time {
	born := ledger.BornAt.Time
	if created := pool.GetCreationTimestamp().Time; !created.IsZero() && created.Before(born) {
		born = created
	}
	return born
}

// sharedDeadline is the hard bound: the worker Pod is gone by then. It is the
// earliest of the ledger's birth plus lifetime and the creation-time deadline
// annotation; a missing or unreadable annotation bounds the pool by its
// creation plus the minimum lifetime, so it never lengthens the bound.
func sharedDeadline(pool *unstructured.Unstructured, ledger *sharedLedger) time.Time {
	deadline := sharedBorn(pool, ledger).Add(ledger.Scope.Lifetime.Duration)
	if recorded, ok := sharedRecordedDeadline(pool); !ok {
		deadline = minTime(deadline, pool.GetCreationTimestamp().Add(profile.MinSharedWorkerLifetime))
	} else {
		deadline = minTime(deadline, recorded)
	}
	return deadline
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// sharedRecordedDeadline reads the creation-time deadline annotation.
func sharedRecordedDeadline(pool *unstructured.Unstructured) (time.Time, bool) {
	raw := pool.GetAnnotations()[sharedDeadlineAnnotation]
	deadline, err := time.Parse(time.RFC3339Nano, raw)
	return deadline, err == nil && raw != ""
}

// setSharedDeadline writes a new pool's deadline annotation.
func setSharedDeadline(pool *unstructured.Unstructured, ledger *sharedLedger) {
	annotations := maps.Clone(pool.GetAnnotations())
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[sharedDeadlineAnnotation] = ledger.BornAt.Add(ledger.Scope.Lifetime.Duration).UTC().Format(time.RFC3339Nano)
	pool.SetAnnotations(annotations)
}

// sharedRetireAt is when strong retirement begins: early enough for the
// worker Pod's whole termination grace, which contains the worker's drain,
// to end a margin before the deadline.
func sharedRetireAt(pool *unstructured.Unstructured, ledger *sharedLedger) time.Time {
	return sharedDeadline(pool, ledger).Add(-workerPodTerminationGrace - sharedRetireMargin)
}

// sharedEscalateAt is when a worker Pod that still exists is re-deleted with
// a one-second grace.
func sharedEscalateAt(pool *unstructured.Unstructured, ledger *sharedLedger) time.Time {
	return sharedDeadline(pool, ledger).Add(-sharedRetireMargin)
}

// sharedAdmissionEnds is when a pool stops admitting new claims.
func sharedAdmissionEnds(pool *unstructured.Unstructured, ledger *sharedLedger) time.Time {
	return sharedRetireAt(pool, ledger).Add(-sharedAdmissionLead)
}

func sharedAdmits(pool *unstructured.Unstructured, ledger *sharedLedger, now time.Time) bool {
	return pool.GetDeletionTimestamp() == nil && !sharedRetireIntended(pool) && ledger.State == sharedAdmitting && now.Before(sharedAdmissionEnds(pool, ledger)) && len(ledger.Entries) < int(ledger.Scope.MaxActors)
}

func ledgerEntry(ledger *sharedLedger, marker string) *sharedLedgerEntry {
	for i := range ledger.Entries {
		if ledger.Entries[i].Journal == marker {
			return &ledger.Entries[i]
		}
	}
	return nil
}

// sharedPoolMatches checks a pool's identity against the record's binding:
// labels, specification digest, no owner, and a ledger of the same scope.
func sharedPoolMatches(pool *unstructured.Unstructured, record *journalRecord) (*sharedLedger, error) {
	shared := record.SharedWorker
	actual, ok := poolSpecDigest(pool)
	if pool.GetUID() == "" || !ok || actual != shared.SpecDigest || !maps.Equal(pool.GetLabels(), record.RuntimePoolLabels) || len(pool.GetOwnerReferences()) != 0 {
		return nil, sdk.ErrStaleIdentity
	}
	ledger, err := readSharedLedger(pool)
	if err != nil {
		return nil, err
	}
	if ledger.Scope != shared.Scope {
		return nil, fmt.Errorf("shared worker ledger scope differs from this allocation: %w", sdk.ErrStaleIdentity)
	}
	if committed, bound := sharedNameBinding(pool, ledger.Scope); committed && !bound {
		return nil, fmt.Errorf("shared worker name does not commit to its ledger scope: %w", sdk.ErrStaleIdentity)
	}
	if recorded, ok := sharedRecordedDeadline(pool); !ok || !recorded.Equal(ledger.BornAt.Add(ledger.Scope.Lifetime.Duration)) {
		return nil, fmt.Errorf("shared worker lifetime evidence is missing or inconsistent: %w", sdk.ErrStaleIdentity)
	}
	return ledger, nil
}

func (d *Lifecycle) getSharedPool(ctx context.Context, record *journalRecord) (*unstructured.Unstructured, error) {
	pool := poolObject()
	err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.SharedWorker.Pool}, pool)
	return pool, err
}

// selectSharedPool returns an admitting pool of the record's scope with a
// free slot, preferring the fullest so empty pools can be collected.
func (d *Lifecycle) selectSharedPool(ctx context.Context, record *journalRecord, source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion(workerPoolAPIVersion())
	pools.SetKind("WorkerPoolList")
	if err := d.client.List(ctx, pools, client.InNamespace(record.Placement.Namespace), client.HasLabels{sharedPoolLabel}); err != nil {
		return nil, err
	}
	now := sharedNow()
	var best *unstructured.Unstructured
	bestEntries := -1
	for i := range pools.Items {
		pool := &pools.Items[i]
		ledger, err := readSharedLedger(pool)
		if err != nil || ledger.Scope != record.SharedWorker.Scope || !sharedAdmits(pool, ledger, now) || len(pool.GetOwnerReferences()) != 0 {
			continue
		}
		label := pool.GetLabels()[sharedPoolLabel]
		if label == "" || !maps.Equal(pool.GetLabels(), sharedLabels(label)) {
			continue
		}
		if suspect, err := d.sharedPoolHostsSuspect(ctx, pool, ledger); err != nil || suspect {
			continue
		}
		spec, err := buildSharedPoolSpec(source, ledger.Scope, label)
		if err != nil {
			return nil, err
		}
		if actual, ok := poolSpecDigest(pool); !ok || actual != digest(spec) {
			continue
		}
		if len(ledger.Entries) > bestEntries {
			best, bestEntries = pool, len(ledger.Entries)
		}
	}
	return best, nil
}

// ensureSharedRuntimePool selects or creates the record's shared pool and
// claims its slot. The journal records claimIssued or createIssued before the
// write and the pool UID only after this record's entry is observed. Before
// the template exists, any doubt moves the record to another pool.
//
//nolint:gocyclo // Selection, creation, claim and fallback stay in journal order.
func (d *Lifecycle) ensureSharedRuntimePool(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	shared := record.SharedWorker
	if shared.Released {
		return sdk.ErrStaleIdentity
	}
	marker := claimMarker(cm, record)
	if shared.UID != "" {
		// A bound pool may already be retiring while its Actor is still
		// observed for drain; prepareSharedWorker refuses it before any boot.
		_, err := d.verifySharedRuntimePool(ctx, cm, record, true)
		return err
	}
	if !d.config.DedicatedInstall {
		return errSharedWorkersNotDedicated
	}
	if err := d.requireSharedProvenance(ctx); err != nil {
		return err
	}
	movable := !record.Template.CreateIssued && !shared.CreateIssued
	if movable {
		// The workspace may have become tainted, quarantined or failed since
		// its allocation was prepared; before any template exists it simply
		// takes the private single-Actor path.
		workspace := &api.ExecutionWorkspace{}
		if err := d.client.Get(ctx, clientKey(record.Request.Key), workspace); err != nil {
			return err
		}
		if workspaceSuspect(workspace) {
			return d.abandonSharedToPrivate(ctx, cm, record, "the workspace became tainted, quarantined or failed")
		}
	}
	source := poolObject()
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.Placement.Name}, source); err != nil {
		return err
	}
	if source.GetUID() != record.Placement.UID || source.GetDeletionTimestamp() != nil {
		return sdk.ErrStaleIdentity
	}
	if movable && shared.Create != nil && !shared.ClaimIssued {
		candidate, err := d.selectSharedPool(ctx, record, source)
		if err != nil {
			return err
		}
		if candidate != nil {
			label := candidate.GetLabels()[sharedPoolLabel]
			specDigest, _ := poolSpecDigest(candidate)
			*shared = sharedPlacement{Scope: shared.Scope, OperatorLabels: shared.OperatorLabels, Pool: candidate.GetName(), Label: label, SpecDigest: specDigest}
			if err := bindSharedPool(record); err != nil {
				return err
			}
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
	}
	abandon := func(reason string) error {
		if !movable {
			return fmt.Errorf("shared worker slot is unavailable after native creation (%s): %w", reason, sdk.ErrStaleIdentity)
		}
		if err := d.bindNewSharedPool(record, source); err != nil {
			return err
		}
		ctrl.LoggerFrom(ctx).Info("Substrate shared worker slot not claimed; creating a new shared worker", "workspaceNamespace", record.Request.Key.Namespace, "workspaceName", record.Request.Key.Name, "sequence", record.Request.Sequence, "reason", reason)
		return d.save(ctx, cm, record)
	}
	pool, err := d.getSharedPool(ctx, record)
	if apierrors.IsNotFound(err) {
		if shared.Create == nil {
			return abandon("the shared worker disappeared")
		}
		// Every creation attempt, including a retry after a crash or a failed
		// Create, needs a complete inventory that no foreign template could
		// reach the new pool. The issuance marker never stands in for it.
		if err := d.refuseForeignTemplates(ctx, shared.Scope.Atespace, sharedLabels(shared.Label), nil); err != nil {
			return err
		}
		if !shared.CreateIssued {
			shared.CreateIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		created := poolObject()
		created.SetNamespace(record.Placement.Namespace)
		created.SetName(shared.Pool)
		created.SetLabels(maps.Clone(record.RuntimePoolLabels))
		var spec map[string]any
		if err := json.Unmarshal(shared.Create, &spec); err != nil {
			return err
		}
		created.Object["spec"] = spec
		ledger := &sharedLedger{Version: sharedLedgerVersion, Scope: shared.Scope, BornAt: metav1.NewTime(sharedNow().UTC().Truncate(time.Second)), State: sharedAdmitting, Entries: []sharedLedgerEntry{{Journal: marker, Actor: actorKey(record)}}}
		if err := setSharedLedger(created, ledger); err != nil {
			return err
		}
		setSharedDeadline(created, ledger)
		err = d.client.Create(ctx, created)
		if apierrors.IsAlreadyExists(err) {
			created = poolObject()
			err = d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: shared.Pool}, created)
		}
		if err != nil {
			return err
		}
		pool = created
	} else if err != nil {
		return err
	}
	if pool.GetDeletionTimestamp() != nil {
		return abandon("the shared worker is deleting")
	}
	ledger, err := sharedPoolMatches(pool, record)
	if err != nil {
		return abandon("the shared worker no longer matches: " + err.Error())
	}
	if ledgerEntry(ledger, marker) == nil {
		if shared.Create != nil {
			return abandon("the created shared worker does not hold this allocation")
		}
		if !sharedAdmits(pool, ledger, sharedNow()) {
			return abandon("the shared worker no longer admits Actors")
		}
		if slices.ContainsFunc(ledger.Entries, func(entry sharedLedgerEntry) bool { return entry.Actor == actorKey(record) }) {
			return abandon("the shared worker already lists this Actor")
		}
		if suspect, err := d.sharedPoolHostsSuspect(ctx, pool, ledger); err != nil {
			return err
		} else if suspect {
			return abandon("the shared worker hosts a tainted, quarantined or failed workspace")
		}
		if err := d.refuseForeignTemplates(ctx, shared.Scope.Atespace, sharedLabels(shared.Label), pool); err != nil {
			return err
		}
		if !shared.ClaimIssued {
			shared.ClaimIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		ledger.Entries = append(ledger.Entries, sharedLedgerEntry{Journal: marker, Actor: actorKey(record)})
		ledger.EmptySince = nil
		if err := setSharedLedger(pool, ledger); err != nil {
			return err
		}
		// The read resourceVersion is the precondition: a claim never
		// overfills a pool or lands on one that stopped admitting.
		if err := d.client.Update(ctx, pool); err != nil {
			return err
		}
	}
	shared.UID = string(pool.GetUID())
	record.RuntimePool.UID = shared.UID
	stamp(&record.Timing.WorkerPoolCreatedAt)
	return d.save(ctx, cm, record)
}

// verifySharedRuntimePool re-reads the bound pool and its ledger. Observation
// and retirement may continue while a pool this provider is retiring
// terminates; startup never does.
func (d *Lifecycle) verifySharedRuntimePool(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, allowRetiring bool) (*sharedLedger, error) {
	shared := record.SharedWorker
	if shared.UID == "" || shared.Released || record.RuntimePool.UID != shared.UID {
		return nil, sdk.ErrStaleIdentity
	}
	pool, err := d.getSharedPool(ctx, record)
	if err != nil {
		return nil, err
	}
	if string(pool.GetUID()) != shared.UID {
		return nil, sdk.ErrStaleIdentity
	}
	ledger, err := sharedPoolMatches(pool, record)
	if err != nil {
		return nil, err
	}
	retiring := pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool) || ledger.State == sharedRetiring || ledger.State == sharedQuarantined
	if retiring && !allowRetiring {
		return nil, fmt.Errorf("shared worker is retiring: %w", sdk.ErrStaleIdentity)
	}
	if ledgerEntry(ledger, claimMarker(cm, record)) == nil {
		return nil, fmt.Errorf("shared worker ledger no longer holds this allocation: %w", sdk.ErrStaleIdentity)
	}
	if record.Worker != nil && ledger.Worker != nil && (ledger.Worker.Name != record.Worker.Name || ledger.Worker.UID != record.Worker.UID || ledger.Worker.PodUID != record.Worker.PodUID || ledger.Worker.Epoch != record.Worker.Epoch) {
		return nil, fmt.Errorf("shared worker was replaced: %w", sdk.ErrStaleIdentity)
	}
	return ledger, nil
}

// verifySharedRuntimePoolReadOnly is verifySharedRuntimePool for read-only
// observation, which has no journal ConfigMap in hand.
func (d *Lifecycle) verifySharedRuntimePoolReadOnly(ctx context.Context, record *journalRecord) error {
	cm := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, journalKey(record.Request.Key), cm); err != nil {
		return err
	}
	_, err := d.verifySharedRuntimePool(ctx, cm, record, true)
	return err
}

// updateSharedLedger applies mutate to the pool's current ledger with the
// read resourceVersion as precondition. A missing or replaced pool is
// reported by found=false.
func (d *Lifecycle) updateSharedLedger(ctx context.Context, namespace, name, uid string, mutate func(*sharedLedger) (bool, error)) (bool, error) {
	pool := poolObject()
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if string(pool.GetUID()) != uid {
		return false, nil
	}
	ledger, err := readSharedLedger(pool)
	if err != nil {
		return true, fmt.Errorf("%w: %w", errSharedLedgerInvalid, err)
	}
	changed, err := mutate(ledger)
	if err != nil || !changed {
		return true, err
	}
	if err := setSharedLedger(pool, ledger); err != nil {
		return true, err
	}
	return true, d.client.Update(ctx, pool)
}

// listWorkerAssignments lists every Actor assignment on a worker.
func (d *Lifecycle) listWorkerAssignments(ctx context.Context, workerName string) ([]*pb.ActorAssignment, error) {
	var assignments []*pb.ActorAssignment
	page := ""
	seen := map[string]bool{}
	for {
		response, err := d.control.ListWorkerActorAssignments(ctx, &pb.ListWorkerActorAssignmentsRequest{Worker: &pb.ObjectRef{Name: workerName}, Options: &pb.ListOptions{PageSize: 1000, PageToken: page}})
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, response.GetActorAssignments()...)
		page = response.GetNextPageToken()
		if page == "" {
			return assignments, nil
		}
		if seen[page] || len(seen) >= 100 {
			return nil, fmt.Errorf("native assignment pagination repeated a token or exceeded its bound")
		}
		seen[page] = true
	}
}

func assignmentRef(assignment *pb.ActorAssignment) string {
	return assignment.GetActor().GetAtespace() + "/" + assignment.GetActor().GetName()
}

func ledgerHolds(ledger *sharedLedger, assignment *pb.ActorAssignment) bool {
	ref := assignmentRef(assignment)
	return slices.ContainsFunc(ledger.Entries, func(entry sharedLedgerEntry) bool {
		return entry.ActorUID != "" && entry.ActorUID == assignment.GetActorUid() && entry.Actor == ref
	})
}

// foreignAssignment reports an assignment that is not in the ledger. The
// assignments are listed before the ledger is read, so an entry written
// before a concurrent Resume is always seen. An entry is released only after
// its Actor is deleted, so a listed assignment whose entry was released in
// between is stale: an unmatched assignment is foreign only if a second
// listing, taken after the ledger read, still reports it.
func (d *Lifecycle) foreignAssignment(ctx context.Context, workerName string, assignments []*pb.ActorAssignment, ledger *sharedLedger) (bool, error) {
	var unmatched []*pb.ActorAssignment
	for _, assignment := range assignments {
		if !ledgerHolds(ledger, assignment) {
			unmatched = append(unmatched, assignment)
		}
	}
	if len(unmatched) == 0 {
		return false, nil
	}
	again, err := d.listWorkerAssignments(ctx, workerName)
	if err != nil {
		return false, err
	}
	for _, assignment := range unmatched {
		if slices.ContainsFunc(again, func(current *pb.ActorAssignment) bool {
			return current.GetActorUid() == assignment.GetActorUid() && assignmentRef(current) == assignmentRef(assignment)
		}) {
			return true, nil
		}
	}
	return false, nil
}

// sharedAssignmentsInLedger lists the worker's assignments, then re-reads the
// ledger. It fails closed on any assigned Actor that is not a ledger entry
// with the same UID, and reports whether this record's Actor is assigned.
func (d *Lifecycle) sharedAssignmentsInLedger(ctx context.Context, record *journalRecord, workerName string) (assigned bool, count int, err error) {
	assignments, err := d.listWorkerAssignments(ctx, workerName)
	if err != nil {
		return false, 0, err
	}
	pool, err := d.getSharedPool(ctx, record)
	if err != nil {
		return false, 0, err
	}
	if string(pool.GetUID()) != record.SharedWorker.UID {
		return false, 0, sdk.ErrStaleIdentity
	}
	ledger, err := readSharedLedger(pool)
	if err != nil {
		return false, 0, err
	}
	foreign, err := d.foreignAssignment(ctx, workerName, assignments, ledger)
	if err != nil {
		return false, 0, err
	}
	if foreign {
		return false, 0, fmt.Errorf("native shared worker hosts an Actor outside its ledger; admission is closed")
	}
	for _, assignment := range assignments {
		ref := assignmentRef(assignment)
		if ref == actorKey(record) || assignment.GetActorUid() == record.Actor.UID {
			if record.Actor.UID == "" || assignment.GetActorUid() != record.Actor.UID || ref != actorKey(record) {
				return false, 0, sdk.ErrStaleIdentity
			}
			assigned = true
		}
	}
	return assigned, len(assignments), nil
}

// prepareSharedWorker replaces the private "ACTIVE and empty" pre-Resume
// check: the pool's only worker is ACTIVE, gVisor, sized for the scope, at a
// settled registered epoch, the ledger's fenced worker, below its slot count,
// and hosts only ledger Actors.
//
//nolint:gocyclo // Every pre-Resume fence is checked in order.
func (d *Lifecycle) prepareSharedWorker(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (bool, error) {
	if !d.config.DedicatedInstall {
		return false, errSharedWorkersNotDedicated
	}
	if err := d.verifyInfrastructureReadOnly(ctx, record); err != nil {
		return false, err
	}
	shared := record.SharedWorker
	ledger, err := d.verifySharedRuntimePool(ctx, cm, record, false)
	if err != nil {
		return false, err
	}
	// One pool snapshot drives the ledger checks, the suspicion check and
	// any retirement, whose write is preconditioned on its resourceVersion:
	// a concurrent ledger change conflicts and is re-evaluated, never retired.
	pool, err := d.getSharedPool(ctx, record)
	if err != nil {
		return false, err
	}
	if string(pool.GetUID()) != shared.UID {
		return false, sdk.ErrStaleIdentity
	}
	if ledger, err = sharedPoolMatches(pool, record); err != nil {
		return false, err
	}
	if ledgerEntry(ledger, claimMarker(cm, record)) == nil {
		return false, fmt.Errorf("shared worker ledger no longer holds this allocation: %w", sdk.ErrStaleIdentity)
	}
	if ledger.State != sharedAdmitting && ledger.State != sharedDraining || !sharedNow().Before(sharedRetireAt(pool, ledger)) {
		return false, fmt.Errorf("shared worker is past its lifetime or retiring: %w", sdk.ErrStaleIdentity)
	}
	// Re-checked immediately before Resume, never only when the allocation was
	// prepared: the template already selects this pool, so suspicion here
	// fails closed instead of moving the record.
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, clientKey(record.Request.Key), workspace); err != nil {
		return false, err
	}
	if workspaceSuspect(workspace) {
		return false, fmt.Errorf("the workspace became tainted, quarantined or failed before its shared boot: %w", sdk.ErrStaleIdentity)
	}
	if suspect, err := d.sharedPoolHostsSuspect(ctx, pool, ledger); err != nil {
		return false, err
	} else if suspect {
		if err := d.retireSharedPool(ctx, pool, sharedRetiring, "the shared worker hosts a tainted, quarantined or failed workspace"); err != nil {
			return false, err
		}
		return false, fmt.Errorf("the shared worker hosts a tainted, quarantined or failed workspace: %w", sdk.ErrStaleIdentity)
	}
	if err := d.refuseForeignTemplates(ctx, shared.Scope.Atespace, record.RuntimePoolLabels, pool); err != nil {
		return false, err
	}
	if err := d.requireSharedProvenance(ctx); err != nil {
		return false, err
	}
	selected, err := d.privatePoolWorker(ctx, record)
	if err != nil || selected == nil {
		return false, err
	}
	status := selected.GetStatus()
	if status.GetState() == pb.WorkerState_WORKER_STATE_UNAVAILABLE || len(status.GetSandboxClasses()) == 0 || status.GetRegisteredEpoch() <= 0 {
		return false, nil
	}
	if selected.GetMetadata().GetUid() == "" || status.GetState() != pb.WorkerState_WORKER_STATE_ACTIVE {
		return false, fmt.Errorf("shared native worker must be active")
	}
	if status.GetObservedEpoch() != status.GetRegisteredEpoch() {
		// The control plane has not yet released Actors of an earlier
		// process; wait until the registered epoch is observed.
		return false, nil
	}
	if err := verifySingleActorWorker(record, selected); err != nil {
		return false, err
	}
	for key, value := range record.TemplateSpec.GetWorkerSelector().GetMatchLabels() {
		if selected.GetLabels()[key] != value {
			return false, sdk.ErrStaleIdentity
		}
	}
	identity := &sharedWorkerIdentity{Name: selected.GetMetadata().GetName(), UID: selected.GetMetadata().GetUid(), PodUID: selected.GetWorkerPodUid(), Epoch: status.GetRegisteredEpoch()}
	if ledger.Worker != nil && *ledger.Worker != *identity {
		return false, fmt.Errorf("shared worker was replaced: %w", sdk.ErrStaleIdentity)
	}
	entry := ledgerEntry(ledger, claimMarker(cm, record))
	if record.Actor.UID == "" || entry.ActorUID != "" && entry.ActorUID != record.Actor.UID {
		return false, sdk.ErrStaleIdentity
	}
	if ledger.Worker == nil || entry.ActorUID == "" {
		// The Actor UID is in the ledger before Resume, so its assignment can
		// never precede its entry.
		marker := claimMarker(cm, record)
		found, err := d.updateSharedLedger(ctx, record.Placement.Namespace, shared.Pool, shared.UID, func(current *sharedLedger) (bool, error) {
			if current.Scope != shared.Scope {
				return false, sdk.ErrStaleIdentity
			}
			if current.Worker == nil {
				current.Worker = identity
			} else if *current.Worker != *identity {
				return false, fmt.Errorf("shared worker was replaced: %w", sdk.ErrStaleIdentity)
			}
			own := ledgerEntry(current, marker)
			if own == nil || own.ActorUID != "" && own.ActorUID != record.Actor.UID {
				return false, sdk.ErrStaleIdentity
			}
			own.ActorUID = record.Actor.UID
			return true, nil
		})
		if err != nil {
			return false, err
		}
		if !found {
			return false, sdk.ErrStaleIdentity
		}
		return false, nil
	}
	assigned, count, err := d.sharedAssignmentsInLedger(ctx, record, selected.GetMetadata().GetName())
	if err != nil {
		return false, err
	}
	if assigned {
		return false, fmt.Errorf("shared worker reports this Actor before its boot: %w", sdk.ErrStaleIdentity)
	}
	if count >= int(shared.Scope.MaxActors) {
		return false, fmt.Errorf("shared native worker has no free slot")
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: selected.WorkerNamespace, Name: selected.WorkerPod}, pod); err != nil {
		return false, err
	}
	fence := &workerFence{Name: selected.Metadata.Name, UID: selected.Metadata.Uid, Namespace: selected.WorkerNamespace, Pool: selected.WorkerPool, Pod: selected.WorkerPod, PodUID: selected.WorkerPodUid, AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.InstanceID, Epoch: status.GetRegisteredEpoch()}
	if string(pod.UID) != fence.PodUID || pod.DeletionTimestamp != nil || pod.Labels[workerPoolLabel] != fence.Pool || pod.Labels[sharedPoolLabel] != shared.Label {
		return false, sdk.ErrStaleIdentity
	}
	if record.Worker != nil && *record.Worker != *fence {
		return false, sdk.ErrStaleIdentity
	}
	if record.Worker == nil {
		record.Worker = fence
		stamp(&record.Timing.WorkerActiveAt)
		if err := d.save(ctx, cm, record); err != nil {
			return false, err
		}
	}
	return true, nil
}

// sharedWorkerIdentityMatches checks a native Worker against the fence.
func sharedWorkerIdentityMatches(worker *pb.Worker, f *workerFence) bool {
	return worker.GetMetadata().GetName() == f.Name && worker.GetMetadata().GetUid() == f.UID && worker.GetWorkerPodUid() == f.PodUID &&
		worker.GetWorkerNamespace() == f.Namespace && worker.GetWorkerPool() == f.Pool && worker.GetWorkerPod() == f.Pod
}

// sharedStrongReason reports why retirement must take the strong path, or ""
// when the interim evidence may still apply. Upstream read errors are doubt.
//
//nolint:gocyclo // Every reason for the strong path is listed in one place.
func (d *Lifecycle) sharedStrongReason(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (string, error) {
	shared := record.SharedWorker
	if shared.Strong != "" {
		return shared.Strong, nil
	}
	if record.Failure != "" {
		return "the instance failed", nil
	}
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, clientKey(record.Request.Key), workspace); err != nil {
		if apierrors.IsNotFound(err) {
			return "the workspace disappeared", nil
		}
		return "", err
	}
	if workspace.UID != record.Request.Key.WorkspaceUID {
		return "the workspace was replaced", nil
	}
	if workspaceSuspect(workspace) {
		return "the workspace is tainted, quarantined or failed", nil
	}
	pool, err := d.getSharedPool(ctx, record)
	switch {
	case apierrors.IsNotFound(err):
		return "the shared worker pool disappeared", nil
	case err != nil:
		return "", err
	case string(pool.GetUID()) != shared.UID:
		return "the shared worker pool was replaced", nil
	case pool.GetDeletionTimestamp() != nil:
		return "the shared worker pool is deleting", nil
	case sharedRetireIntended(pool):
		return "the shared worker's strong retirement was decided but its deletion has not completed", nil
	}
	ledger, err := sharedPoolMatches(pool, record)
	if err != nil {
		return "the shared worker ledger is in doubt", nil
	}
	if ledger.State == sharedRetiring || ledger.State == sharedQuarantined {
		return "the shared worker is " + strings.ToLower(ledger.State), nil
	}
	if ledgerEntry(ledger, claimMarker(cm, record)) == nil {
		return "the shared worker ledger no longer holds this allocation", nil
	}
	if !sharedNow().Before(sharedRetireAt(pool, ledger)) {
		return "the shared worker reached its maximum lifetime", nil
	}
	if record.Worker == nil {
		return "the boot has no fenced worker", nil
	}
	if ledger.Worker != nil && (ledger.Worker.UID != record.Worker.UID || ledger.Worker.PodUID != record.Worker.PodUID || ledger.Worker.Epoch != record.Worker.Epoch) {
		return "the shared worker was replaced", nil
	}
	for _, fence := range record.RetirementWorkers {
		if fence.PodUID != record.Worker.PodUID {
			return "the Actor ran on a replacement worker", nil
		}
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Worker.Namespace, Name: record.Worker.Pod}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return "the shared worker Pod is gone", nil
		}
		return "", err
	}
	if string(pod.UID) != record.Worker.PodUID {
		return "the shared worker Pod was replaced", nil
	}
	if pod.DeletionTimestamp != nil {
		return "the shared worker Pod is terminating", nil
	}
	return "", nil
}

// markSharedStrong persists the strong path for this record.
func (d *Lifecycle) markSharedStrong(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, reason string) error {
	if record.SharedWorker.Strong != "" {
		return nil
	}
	ctx, cancel := strongCleanupContext(ctx)
	defer cancel()
	record.SharedWorker.Strong = reason
	ctrl.LoggerFrom(ctx).Info("Substrate shared worker retirement takes the strong path", "workspaceNamespace", record.Request.Key.Namespace, "workspaceName", record.Request.Key.Name, "sequence", record.Request.Sequence, "reason", reason)
	return d.save(ctx, cm, record)
}

// retireSharedPool closes a pool's ledger and deletes the pool with UID
// precondition and foreground propagation. Each hosted allocation then proves
// exact worker Pod UID absence. Deleting the worker Pod is how co-hosted
// Actors are suspended: Orka core drains each for data-only suspension inside
// the worker's eviction window.
func (d *Lifecycle) retireSharedPool(ctx context.Context, pool *unstructured.Unstructured, state, reason string) error {
	return d.hardRetireSharedPool(ctx, pool, state, reason)
}

// revokeSharedEgress installs and verifies deny-all egress, removing every
// credential-injection effect, before the Actor is suspended.
func (d *Lifecycle) revokeSharedEgress(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if record.SharedWorker.EgressRevoked {
		return nil
	}
	if err := d.applyEgressPolicy(ctx, cm, record, nil, errSharedEgressRevoked); err != nil && !errors.Is(err, errEgressDeniedAll) {
		return err
	}
	if err := d.verifyEgressDenyAll(ctx, record); err != nil {
		return err
	}
	record.SharedWorker.EgressRevoked = true
	stamp(&record.SharedWorker.EgressRevokedAt)
	return d.save(ctx, cm, record)
}

func (d *Lifecycle) verifyEgressDenyAll(ctx context.Context, record *journalRecord) error {
	empty, err := egressRulesDigest(nil)
	if err != nil {
		return err
	}
	existing, err := d.control.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: actorRef(record), Name: egressPolicyName})
	if err != nil {
		return err
	}
	applied, err := egressRulesDigest(existing.GetRules())
	if err != nil {
		return err
	}
	if !record.EgressDenyAll || record.EgressPolicy.UID == "" || existing.GetMetadata().GetUid() != record.EgressPolicy.UID || applied != empty || record.EgressPolicy.Digest != empty {
		return fmt.Errorf("native Actor egress is not deny-all")
	}
	return nil
}

// prepareSharedSuspend runs before SuspendActor on a shared worker and
// reports whether SuspendActor may be issued now. Any doubt (including an
// unreadable worker, ledger or workspace, or a failed egress revocation)
// selects the strong path and starts retiring the pool at once; the data
// capture still proceeds. On the interim path the verified deny-all egress
// must have been stored for sharedEgressSettle first, so gateway and DNS relay
// caches no longer hold the admitted allowlist.
func (d *Lifecycle) prepareSharedSuspend(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, actor *pb.Actor) (bool, error) {
	shared := record.SharedWorker
	reason, err := d.sharedStrongReason(ctx, cm, record)
	if err != nil {
		reason = "the shared worker could not be verified before suspension: " + err.Error()
	}
	if reason == "" {
		if err := d.verifyRetiringSharedWorker(ctx, record); err != nil {
			reason = "the shared worker could not be verified before suspension: " + err.Error()
		}
	}
	if reason == "" {
		if err := d.revokeSharedEgress(ctx, cm, record); err != nil {
			reason = "egress could not be revoked before suspension: " + err.Error()
		}
	}
	if reason != "" {
		if err := d.sharedDoubt(ctx, cm, record, reason); err != nil {
			return false, err
		}
	}
	if shared.Strong == "" && sharedNow().Before(shared.EgressRevokedAt.Add(d.config.egressSettle())) {
		return false, nil
	}
	if shared.Suspend == nil || !shared.Suspend.Issued {
		shared.Suspend = &sharedSuspendFence{Issued: true, PriorVersion: actor.GetMetadata().GetVersion(), PriorGeneration: actor.GetStatus().GetLastAssignedGeneration(), PriorSnapshotDigest: snapshotDigest(latestSnapshot(actor))}
	}
	return true, nil
}

// sharedSuspendFenceOf returns the issued shared suspend fence, if any.
func sharedSuspendFenceOf(record *journalRecord) *sharedSuspendFence {
	if record.SharedWorker == nil || record.SharedWorker.Suspend == nil || !record.SharedWorker.Suspend.Issued {
		return nil
	}
	return record.SharedWorker.Suspend
}

// verifyRetiringSharedWorker checks the live fenced worker and that every
// assignment on it is a ledger Actor.
func (d *Lifecycle) verifyRetiringSharedWorker(ctx context.Context, record *journalRecord) error {
	f := record.Worker
	if f == nil {
		return fmt.Errorf("native worker fence is missing")
	}
	worker, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: &pb.ObjectRef{Name: f.Name}})
	if err != nil {
		return err
	}
	if !sharedWorkerIdentityMatches(worker, f) || worker.GetStatus().GetRegisteredEpoch() != f.Epoch {
		return sdk.ErrStaleIdentity
	}
	// MarkWorkerUnavailable changes only the state, never identity or epoch.
	if worker.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_ACTIVE {
		return fmt.Errorf("the fenced shared worker is %s", worker.GetStatus().GetState())
	}
	if err := verifySingleActorWorkerShape(record, worker, true); err != nil {
		return err
	}
	_, _, err = d.sharedAssignmentsInLedger(ctx, record, f.Name)
	return err
}

// proveSharedTermination evaluates the interim evidence for a SUSPENDED
// Actor. It returns a proof, a doubt (strong path), or neither when only the
// worker version moved and the proof should be retried.
//
//nolint:gocyclo // The interim evidence is a fixed sequence of exact checks.
func (d *Lifecycle) proveSharedTermination(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, actor *pb.Actor) (*sharedTerminationProof, string) {
	shared := record.SharedWorker
	fence := shared.Suspend
	f := record.Worker
	// (1) The Actor is SUSPENDED at a later version with the expected
	// completed data-only snapshot, and holds no worker assignment.
	snapshot := latestSnapshot(actor)
	if fence == nil || !fence.Issued || fence.SnapshotDigest == "" || actor.GetMetadata().GetUid() != record.Actor.UID || actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_SUSPENDED ||
		actor.GetStatus().GetWorkerAssignment() != nil || actor.GetMetadata().GetVersion() <= fence.PriorVersion ||
		snapshotDigest(snapshot) != fence.SnapshotDigest || verifyDataSnapshot(snapshot, pb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR) != nil || snapshot.GetGeneration() <= fence.PriorGeneration {
		return nil, "the suspended Actor does not match the issued suspend"
	}
	// (2) Its assignment is absent from the same fenced worker, at the same
	// settled epoch and an unchanged worker version.
	before, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: &pb.ObjectRef{Name: f.Name}})
	if err != nil {
		return nil, "the fenced worker could not be read: " + status.Code(err).String()
	}
	if !sharedWorkerIdentityMatches(before, f) || before.GetStatus().GetRegisteredEpoch() != f.Epoch || before.GetStatus().GetObservedEpoch() != f.Epoch {
		return nil, "the fenced worker's identity or epoch changed"
	}
	if before.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_ACTIVE {
		return nil, "the fenced worker is " + before.GetStatus().GetState().String()
	}
	assigned, _, err := d.sharedAssignmentsInLedger(ctx, record, f.Name)
	if err != nil {
		return nil, "the fenced worker's assignments are in doubt: " + err.Error()
	}
	if assigned {
		return nil, "the fenced worker still lists the suspended Actor"
	}
	after, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: &pb.ObjectRef{Name: f.Name}})
	if err != nil {
		return nil, "the fenced worker could not be re-read: " + status.Code(err).String()
	}
	if !sharedWorkerIdentityMatches(after, f) || after.GetStatus().GetRegisteredEpoch() != f.Epoch || after.GetStatus().GetObservedEpoch() != f.Epoch {
		return nil, "the fenced worker's identity or epoch changed during the proof"
	}
	if after.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_ACTIVE {
		return nil, "the fenced worker is " + after.GetStatus().GetState().String()
	}
	if after.GetMetadata().GetVersion() != before.GetMetadata().GetVersion() {
		if shared.ProofAttempts+1 >= sharedMaxProofAttempts {
			return nil, "the fenced worker kept changing during the proof"
		}
		shared.ProofAttempts++
		if err := d.save(ctx, cm, record); err != nil {
			return nil, "the proof attempt could not be recorded"
		}
		return nil, ""
	}
	// (3) Egress is still deny-all: no credential-injection effect remains.
	if err := d.verifyEgressDenyAll(ctx, record); err != nil {
		return nil, "the revoked egress could not be verified"
	}
	current, err := d.actor(ctx, record)
	if err != nil || current == nil || current.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() || current.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_SUSPENDED || current.GetStatus().GetWorkerAssignment() != nil {
		return nil, "the suspended Actor changed during the proof"
	}
	return &sharedTerminationProof{WorkerUID: f.UID, Epoch: f.Epoch, WorkerVersion: after.GetMetadata().GetVersion(), ActorVersion: actor.GetMetadata().GetVersion(), At: *milestone()}, ""
}

// stopShared retires a record on a shared worker. A never-booted Actor only
// releases its slot. Otherwise the interim evidence applies unless there is
// doubt, which takes the strong path; neighbours are never touched outside
// that explicit path.
//
//nolint:gocyclo // Retirement steps follow the journal in order.
func (d *Lifecycle) stopShared(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, actor *pb.Actor) error {
	shared := record.SharedWorker
	if shared.UID == "" && !shared.Released {
		if record.BootRequested {
			return fmt.Errorf("native boot has no bound shared worker")
		}
		if err := d.resolveRetiringSharedClaim(ctx, cm, record); err != nil {
			return err
		}
	}
	if record.BootRequested && !record.WorkloadAbsent {
		if actor != nil && actor.GetStatus().GetWorkerAssignment() != nil && (record.Worker == nil || !sharedAssignmentMatches(actor.GetStatus().GetWorkerAssignment(), record.Worker)) {
			if err := d.captureSharedReplacement(ctx, cm, record, actor); err != nil {
				if apierrors.IsConflict(err) {
					return err
				}
				return errors.Join(err, d.sharedDoubt(ctx, cm, record, "the Actor's replacement worker could not be recorded"))
			}
		}
		// A persisted strong marker always wins, even over a journaled interim
		// proof (for example one saved just before a crash): pool deletion and
		// the absence proof must finish. Otherwise a journaled interim proof is
		// sufficient on its own, and later doubt about the pool does not reopen it.
		reason := shared.Strong
		if reason == "" && shared.Proof == nil {
			var err error
			if reason, err = d.sharedStrongReason(ctx, cm, record); err != nil {
				// Unverifiable before the proof is doubt: persist it and start
				// the pool's deletion (under the detached cleanup context)
				// rather than leave no intent for the bounds loop to recover.
				reason = "the shared worker could not be verified during retirement: " + err.Error()
			}
		}
		if reason == "" && shared.Proof == nil {
			proven, doubt, err := d.advanceSharedInterim(ctx, cm, record, actor)
			if err != nil && !apierrors.IsConflict(err) {
				// Any failure after the evidence suspend's intent (including
				// a journal write after the suspend took effect) is doubt; a
				// write conflict is retried.
				doubt, err = "the evidence-only retirement could not be recorded: "+err.Error(), nil
			}
			if err != nil || (!proven && doubt == "") {
				return err
			}
			reason = doubt
		}
		if reason != "" {
			// Deletion is driven even if the marker cannot be saved; the
			// persistence error is still returned.
			persistErr := d.markSharedStrong(ctx, cm, record, reason)
			gone, err := d.strongSharedAbsence(ctx, record)
			if persistErr != nil || err != nil || !gone {
				return errors.Join(persistErr, err)
			}
		}
		record.WorkloadAbsent = true
		stamp(&record.Timing.WorkerAbsentAt)
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	if actor != nil {
		if !record.DeleteIssued {
			record.DeleteIssued = true
			stamp(&record.Timing.ActorDeleteIssuedAt)
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		_, err := d.control.DeleteActor(ctx, &pb.DeleteActorRequest{Actor: actorRef(record), Options: &pb.DeleteOptions{Uid: actor.GetMetadata().GetUid()}})
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		return nil
	}
	if shared.Strong != "" && !shared.Released {
		// Finish a persisted strong retirement before the slot (and with it
		// the marker the bounds loop reads) is released.
		gone, err := d.strongSharedAbsence(ctx, record)
		if err != nil || !gone {
			return err
		}
	}
	if err := d.releaseSharedSlot(ctx, cm, record); err != nil {
		return err
	}
	return d.finishStop(ctx, cm, record)
}

// advanceSharedInterim moves the interim path forward by one step. It
// returns proven when the proof is journaled, a doubt for the strong path, or
// neither while waiting.
func (d *Lifecycle) advanceSharedInterim(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, actor *pb.Actor) (bool, string, error) {
	shared := record.SharedWorker
	if actor == nil {
		return false, "the Actor disappeared before its termination was proven", nil
	}
	switch actor.GetStatus().GetState() {
	case pb.ActorState_ACTOR_STATE_RUNNING:
		if shared.Suspend != nil && shared.Suspend.Issued {
			return false, "the Actor kept running after its suspend was issued", nil
		}
		// A Stop issues the same synchronous VOLUMES suspend, only as
		// termination evidence; no Tag is kept.
		ready, err := d.prepareSharedSuspend(ctx, cm, record, actor)
		if err != nil {
			return false, "", err
		}
		if shared.Strong != "" {
			return false, shared.Strong, nil
		}
		if !ready {
			return false, "", nil
		}
		stamp(&record.Timing.SuspendIssuedAt)
		if err := d.save(ctx, cm, record); err != nil {
			return false, "", err
		}
		// A rejected or timed-out evidence suspend may or may not have taken
		// effect, so it is doubt: the strong path is persisted and the pool's
		// retirement starts before returning, and it is never reissued.
		if _, err = d.control.SuspendActor(ctx, &pb.SuspendActorRequest{Actor: actorRef(record), Options: &pb.SuspendOptions{Mode: pb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}}); err != nil {
			return false, "the evidence-only suspend failed: " + status.Code(err).String(), nil
		}
		return false, "", nil
	case pb.ActorState_ACTOR_STATE_SUSPENDING:
		// A synchronous evidence suspend that succeeded returns SUSPENDED, and
		// one that failed is already doubt, so a SUSPENDING Actor seen on a
		// later pass (for example after a crash between the RPC and its
		// result) is unresolved: a failed upstream workflow can leave it
		// SUSPENDING for good. A Stop needs no data, so it never waits for or
		// continues that suspension; it takes the strong path.
		return false, "the evidence-only suspend left the Actor SUSPENDING, unresolved", nil
	case pb.ActorState_ACTOR_STATE_SUSPENDED:
	default:
		return false, "the Actor is " + actor.GetStatus().GetState().String(), nil
	}
	if shared.Suspend == nil || !shared.Suspend.Issued || !shared.EgressRevoked {
		return false, "the Actor was suspended outside the recorded retirement", nil
	}
	if shared.Suspend.SnapshotDigest == "" {
		snapshot := latestSnapshot(actor)
		if verifyDataSnapshot(snapshot, pb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR) != nil || snapshot.GetActorTemplateUid() != record.Template.UID || snapshot.GetGeneration() <= shared.Suspend.PriorGeneration || snapshotDigest(snapshot) == shared.Suspend.PriorSnapshotDigest {
			return false, "the suspension exposed no new completed data-only snapshot", nil
		}
		shared.Suspend.SnapshotDigest = snapshotDigest(snapshot)
		shared.Suspend.ObservedVersion = actor.GetMetadata().GetVersion()
		stamp(&record.Timing.SuspendedObservedAt)
		if err := d.save(ctx, cm, record); err != nil {
			return false, "", err
		}
	}
	proof, doubt := d.proveSharedTermination(ctx, cm, record, actor)
	if proof == nil {
		return false, doubt, nil
	}
	shared.Proof = proof
	if err := d.save(ctx, cm, record); err != nil {
		return false, "", err
	}
	return true, "", nil
}

func sharedAssignmentMatches(assignment *pb.WorkerAssignment, f *workerFence) bool {
	return assignment.GetWorker().GetName() == f.Name && assignment.GetWorkerNamespace() == f.Namespace && assignment.GetWorkerPool() == f.Pool && assignment.GetWorkerPod() == f.Pod && assignment.GetWorkerPodUid() == f.PodUID
}

// captureSharedReplacement records a different worker the Actor is assigned
// to in the same pool. That is doubt, so retirement takes the strong path and
// waits for that Pod's absence too.
func (d *Lifecycle) captureSharedReplacement(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, actor *pb.Actor) error {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment.GetWorkerNamespace() != record.Placement.Namespace || assignment.GetWorkerPool() != record.RuntimePool.Name || assignment.GetWorkerPodUid() == "" || assignment.GetWorkerPod() == "" {
		return sdk.ErrStaleIdentity
	}
	for i := range record.RetirementWorkers {
		if sharedAssignmentMatches(assignment, &record.RetirementWorkers[i]) {
			return nil
		}
	}
	if len(record.RetirementWorkers) == 16 {
		return fmt.Errorf("native replacement retirement evidence exceeded bound")
	}
	record.RetirementWorkers = append(record.RetirementWorkers, workerFence{Name: assignment.GetWorker().GetName(), Namespace: assignment.GetWorkerNamespace(), Pool: assignment.GetWorkerPool(), Pod: assignment.GetWorkerPod(), PodUID: assignment.GetWorkerPodUid(), AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.InstanceID})
	if record.SharedWorker.Strong == "" {
		record.SharedWorker.Strong = "the Actor was assigned to a replacement worker"
	}
	return d.save(ctx, cm, record)
}

// strongSharedAbsence retires the record's pool and reports true once the
// pool is gone and every fenced worker Pod UID is absent.
func (d *Lifecycle) strongSharedAbsence(ctx context.Context, record *journalRecord) (bool, error) {
	ctx, cancel := strongCleanupContext(ctx)
	defer cancel()
	shared := record.SharedWorker
	pool, err := d.getSharedPool(ctx, record)
	poolGone := apierrors.IsNotFound(err)
	if err != nil && !poolGone {
		return false, err
	}
	if !poolGone {
		if string(pool.GetUID()) != shared.UID {
			poolGone = true
		} else if err := d.hardRetireSharedPool(ctx, pool, sharedRetiring, shared.Strong); err != nil {
			return false, err
		}
	}
	fences := append([]workerFence{}, record.RetirementWorkers...)
	if record.Worker != nil {
		fences = append(fences, *record.Worker)
	}
	allAbsent := true
	for i := range fences {
		f := &fences[i]
		absent, err := d.workerFenceAbsent(ctx, f)
		if err != nil {
			return false, err
		}
		if absent {
			continue
		}
		allAbsent = false
		pod := &corev1.Pod{}
		if err := d.client.Get(ctx, types.NamespacedName{Namespace: f.Namespace, Name: f.Pod}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, err
		}
		if string(pod.UID) != f.PodUID || pod.Labels[workerPoolLabel] != f.Pool || pod.Labels[sharedPoolLabel] != shared.Label {
			return false, sdk.ErrStaleIdentity
		}
		if pod.DeletionTimestamp == nil && poolGone {
			if err := d.client.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
		}
	}
	return poolGone && allAbsent, nil
}

// resolveRetiringSharedClaim binds an issued but unbound claim or creation
// only through this record's own ledger entry; otherwise the record releases
// it and holds nothing to retire.
func (d *Lifecycle) resolveRetiringSharedClaim(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	shared := record.SharedWorker
	if shared.ClaimIssued || shared.CreateIssued {
		pool, err := d.getSharedPool(ctx, record)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil {
			if ledger, matchErr := sharedPoolMatches(pool, record); matchErr == nil && ledgerEntry(ledger, claimMarker(cm, record)) != nil {
				shared.UID = string(pool.GetUID())
				record.RuntimePool.UID = shared.UID
				return d.save(ctx, cm, record)
			}
		}
	}
	shared.Released = true
	return d.save(ctx, cm, record)
}

// releaseSharedSlot removes this record's ledger entry after its Actor is
// gone. A missing, replaced or deleting pool holds nothing to release.
func (d *Lifecycle) releaseSharedSlot(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	shared := record.SharedWorker
	if shared.Released {
		return nil
	}
	if shared.Strong != "" && shared.UID != "" {
		// The entry keeps the strong marker visible to the bounds loop, so it
		// is released only once the pool is gone and every fenced worker Pod
		// is proven absent.
		gone, err := d.strongSharedAbsence(ctx, record)
		if err != nil {
			return err
		}
		if !gone {
			return fmt.Errorf("waiting for the strongly retired shared worker to be proven absent before releasing its slot")
		}
	}
	if shared.UID != "" {
		marker := claimMarker(cm, record)
		_, err := d.updateSharedLedger(ctx, record.Placement.Namespace, shared.Pool, shared.UID, func(ledger *sharedLedger) (bool, error) {
			before := len(ledger.Entries)
			ledger.Entries = slices.DeleteFunc(ledger.Entries, func(entry sharedLedgerEntry) bool { return entry.Journal == marker })
			return len(ledger.Entries) != before, nil
		})
		// An unreadable ledger admits nobody, so the Actor's already-deleted
		// slot needs no release there; API errors and conflicts are retried.
		if err != nil && !errors.Is(err, errSharedLedgerInvalid) {
			return err
		}
	}
	shared.Released = true
	return d.save(ctx, cm, record)
}

// finishStop reports Stopped, with retained data for a suspension.
func (d *Lifecycle) finishStop(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	first := record.Observation.State != sdk.AllocationStopped && record.Timing.StoppedAt == nil
	record.Observation.State = sdk.AllocationStopped
	record.Observation.Startup = nil
	if first {
		stamp(&record.Timing.StoppedAt)
	}
	if record.Operation == "suspend" {
		if record.Checkpoint == nil {
			return fmt.Errorf("native suspension has no verified Data Tag")
		}
		if err := d.verifyCheckpoint(ctx, record.Checkpoint); err != nil {
			return err
		}
		data, err := json.Marshal(record.Checkpoint)
		if err != nil {
			return err
		}
		record.Observation.RetainedData = &sdk.RetainedDataReference{ID: record.Checkpoint.Tag.Name, SourceInstance: record.Observation.Identity, ProofSHA256: digest(data)}
	}
	if err := d.save(ctx, cm, record); err != nil {
		return err
	}
	if first {
		logRuntimeStopped(ctx, record)
	}
	return nil
}

// terminationProof names the evidence a stopped record used, for timing.
func terminationProof(record *journalRecord) string {
	shared := record.SharedWorker
	switch {
	case shared == nil || !record.BootRequested:
		return ""
	case shared.Strong != "":
		return "strong"
	case shared.Proof != nil:
		return "interim"
	default:
		return ""
	}
}

// abandonSharedToPrivate moves a record whose template does not exist yet to
// the private single-Actor pool it would have used without sharing. A claim
// that still lands later is an orphan the leader collects.
func (d *Lifecycle) abandonSharedToPrivate(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, reason string) error {
	if record.Template.CreateIssued || record.SharedWorker.CreateIssued || record.Actor.CreateIssued {
		return fmt.Errorf("a shared placement cannot move to a private worker after native creation")
	}
	operator := record.SharedWorker.OperatorLabels
	record.SharedWorker = nil
	if err := bindPrivatePool(record, record.Actor.Name+"-workers", operator); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("Substrate allocation takes a private worker instead of a shared one", "workspaceNamespace", record.Request.Key.Namespace, "workspaceName", record.Request.Key.Name, "sequence", record.Request.Sequence, "reason", reason)
	if err := d.save(ctx, cm, record); err != nil {
		return err
	}
	return d.ensureRuntimePool(ctx, cm, record)
}

// sharedPoolHostsSuspect reports whether the pool must be strongly retired
// because of a hosted allocation, reading only Kubernetes: a hosted journal
// that recorded the strong path or an instance failure, or a hosted
// workspace that is tainted, quarantined or failed. Admission, the leader
// sweep (before any native read) and the bounds loop all use it.
func (d *Lifecycle) sharedPoolHostsSuspect(ctx context.Context, pool *unstructured.Unstructured, ledger *sharedLedger) (bool, error) {
	for _, entry := range ledger.Entries {
		record, found, err := d.sharedEntryRecordConfirmed(ctx, pool, entry)
		if errors.Is(err, errSharedJournalUndecodable) {
			// Its Strong and Failure markers and its workspace cannot be
			// checked, so it blocks admission and retires the pool.
			return true, nil
		}
		if err != nil {
			return false, err
		}
		held := found && record != nil && sharedEntryHeld(record, pool, entry)
		if !held {
			// An entry that names an Actor UID is a booted slot. If its exact
			// journal does not hold it, under that Actor's name and UID, the
			// entry hides whose markers apply: doubt. A claim without an Actor
			// UID can be an abandoned orphan and is neutral.
			if entry.ActorUID == "" {
				continue
			}
			// A release removes the entry before its journal records
			// Released, so a concurrent release can make an older snapshot's
			// entry look disowned. Doubt holds only if the entry is still in a
			// pool snapshot read after the journal.
			persists, err := d.sharedEntryPersists(ctx, pool, entry)
			if err != nil {
				return false, err
			}
			if persists {
				return true, nil
			}
			continue
		}
		// A recorded strong path or instance failure is suspect before the
		// workspace itself reports Failed.
		if record.SharedWorker.Strong != "" || record.Failure != "" {
			return true, nil
		}
		workspace := &api.ExecutionWorkspace{}
		if err := d.sharedWorkspace(ctx, clientKey(record.Request.Key), workspace); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, err
		}
		if workspace.UID == record.Request.Key.WorkspaceUID && workspaceSuspect(workspace) {
			return true, nil
		}
	}
	return false, nil
}

// errForeignTemplate reports an ActorTemplate outside the shared pool's
// Atespace that could be placed on it.
var errForeignTemplate = errors.New("an ActorTemplate this provider did not create could be placed on the shared worker; shared admission is closed")

// foreignTemplateMatches lists every ActorTemplate on the install (the
// provider holds global viewer) and reports whether one outside atespace has
// an empty worker selector or one the pool's labels satisfy. Upstream
// placement matches only labels (cmd/ateapi/internal/scheduling), so such a
// template could land a foreign Actor on the shared worker.
func (d *Lifecycle) foreignTemplateMatches(ctx context.Context, atespace string, poolLabels map[string]string) (bool, error) {
	templates, err := d.listAllActorTemplates(ctx)
	if err != nil {
		return false, err
	}
	// The hot path consults the in-process record of provider templates. A
	// stale record can only miss, never wrongly vouch: entries come only
	// from data this provider wrote, template UIDs are immutable, a refresh
	// never replaces an entry recorded after it began, and an entry whose
	// UID is still unknown is a miss. A miss rereads Kubernetes before a
	// template is accepted or declared foreign.
	ours := recordedTemplates.snapshot()
	if !recordedTemplateMiss(templates, atespace, poolLabels, ours) {
		// Every same-Atespace template that reaches the pool is recorded
		// with its exact UID, so only one outside the Atespace can be foreign.
		return templatesReach(templates, atespace, poolLabels, ours), nil
	}
	began := recordedTemplates.begin()
	fresh, err := d.providerTemplates(ctx)
	if err != nil {
		return false, err
	}
	// The authoritative reread, merged with anything recorded since it began.
	return templatesReach(templates, atespace, poolLabels, recordedTemplates.replace(fresh, began)), nil
}

// recordedTemplateMiss reports a template inside the pool's Atespace that
// reaches the pool and is absent from, disagrees with, or has no known UID
// in the record. An unknown-UID entry is accepted only after a reread.
func recordedTemplateMiss(templates []*pb.ActorTemplate, atespace string, poolLabels map[string]string, ours map[string]string) bool {
	return slices.ContainsFunc(templates, func(template *pb.ActorTemplate) bool {
		meta := template.GetMetadata()
		if meta.GetAtespace() != atespace || !selectorMatches(template.GetWorkerSelector().GetMatchLabels(), poolLabels) {
			return false
		}
		uid, recorded := ours[meta.GetAtespace()+"/"+meta.GetName()]
		return !recorded || uid == "" || uid != meta.GetUid()
	})
}

// recordedTemplateCache is the in-process record of provider templates.
// Every add is stamped from a counter, so a refresh that began before an add
// (and may have read that journal before its UID was written) never replaces
// it.
type recordedTemplateCache struct {
	mu      sync.Mutex
	entries map[string]string
	stamps  map[string]uint64
	clock   uint64
}

var recordedTemplates = &recordedTemplateCache{}

// recordedTemplateCacheMax bounds the in-process template record.
const recordedTemplateCacheMax = 4096

func (c *recordedTemplateCache) snapshot() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.entries)
}

// begin returns the stamp a refresh must pass to replace, read before the
// refresh lists anything.
func (c *recordedTemplateCache) begin() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clock
}

// replace installs a full inventory read from Kubernetes by a refresh that
// began at stamp began, and returns the merged record. Every entry keeps the
// stamp of the information it holds: a save stamps its entry with a new
// counter value, and a refresh stamps what it read with began. A refresh
// overrides or drops (for example a deleted journal's entry) only entries
// stamped at or before began, so an entry saved after it began survives it,
// however overlapping refreshes interleave or finish.
func (c *recordedTemplateCache) replace(entries map[string]string, began uint64) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	merged := map[string]string{}
	stamps := map[string]uint64{}
	for key, uid := range entries {
		merged[key], stamps[key] = uid, began
	}
	for key, stamp := range c.stamps {
		if stamp > began {
			merged[key], stamps[key] = c.entries[key], stamp
		}
	}
	c.entries, c.stamps = merged, stamps
	return maps.Clone(merged)
}

// add records a template this process just wrote to a journal.
func (c *recordedTemplateCache) add(atespace string, ref nativeReference) {
	if atespace == "" || ref.Name == "" || ref.UID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]string{}
	}
	if c.stamps == nil {
		c.stamps = map[string]uint64{}
	}
	if len(c.entries) >= recordedTemplateCacheMax {
		// Bounded: a full record is dropped. Every missing or unknown entry
		// is reread from Kubernetes before it counts, so this costs reads,
		// never a wrong match.
		c.entries, c.stamps = map[string]string{}, map[string]uint64{}
	}
	c.clock++
	key := atespace + "/" + ref.Name
	c.entries[key] = ref.UID
	c.stamps[key] = c.clock
}

// providerTemplates is every derived template this provider recorded: each
// journal or archived record's template and create template, and each
// checkpoint catalog's template (which may outlive its workspace). Keys are
// <atespace>/<name>; values are the recorded UID, or "" before it is known.
func (d *Lifecycle) providerTemplates(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	add := func(atespace string, ref nativeReference) {
		if atespace == "" || ref.Name == "" {
			return
		}
		key := atespace + "/" + ref.Name
		if existing, found := out[key]; !found || existing == "" {
			out[key] = ref.UID
		}
	}
	journals := &corev1.ConfigMapList{}
	if err := d.client.List(ctx, journals, client.HasLabels{ownershipLabel, providerLabel}); err != nil {
		return nil, err
	}
	d.sharedPass.noteJournals(journals.Items)
	for i := range journals.Items {
		if record, ok := decodeClaimRecord(&journals.Items[i]); ok {
			add(record.Atespace, record.Template)
			add(record.Atespace, record.CreateTemplate)
		}
	}
	catalogs, err := d.listCatalogs(ctx, "")
	if err != nil {
		return nil, err
	}
	for i := range catalogs {
		// The List already returned the full objects: no per-catalog Get.
		artifact, err := decodeCatalog(&catalogs[i], client.ObjectKeyFromObject(&catalogs[i]))
		if err != nil {
			return nil, err
		}
		add(artifact.Checkpoint.Atespace, artifact.Checkpoint.Template)
	}
	return out, nil
}

// listAllActorTemplates lists ActorTemplates in every Atespace.
func (d *Lifecycle) listAllActorTemplates(ctx context.Context) ([]*pb.ActorTemplate, error) {
	var out []*pb.ActorTemplate
	page := ""
	seen := map[string]bool{}
	for {
		response, err := d.control.ListActorTemplates(ctx, &pb.ListActorTemplatesRequest{Options: &pb.ListOptions{PageSize: 1000, PageToken: page}})
		if err != nil {
			return nil, err
		}
		out = append(out, response.GetActorTemplates()...)
		page = response.GetNextPageToken()
		if page == "" {
			return out, nil
		}
		if seen[page] || len(seen) >= 100 {
			return nil, fmt.Errorf("native template pagination repeated a token or exceeded its bound")
		}
		seen[page] = true
	}
}

// templatesReach reports a foreign template whose worker selector, empty or
// not, a worker with these labels satisfies. Outside the pool's Atespace
// every such template is foreign. Inside it, so is any that is not a derived
// template this provider recorded (ours), with the recorded UID once known.
func templatesReach(templates []*pb.ActorTemplate, atespace string, poolLabels map[string]string, ours map[string]string) bool {
	return slices.ContainsFunc(templates, func(template *pb.ActorTemplate) bool {
		if !selectorMatches(template.GetWorkerSelector().GetMatchLabels(), poolLabels) {
			return false
		}
		meta := template.GetMetadata()
		if meta.GetAtespace() != atespace {
			return true
		}
		uid, recorded := ours[meta.GetAtespace()+"/"+meta.GetName()]
		return !recorded || uid != "" && uid != meta.GetUid()
	})
}

// selectorMatches reports whether a label selector, including an empty one,
// selects a worker with these labels.
//
// Like upstream's labels.SelectorFromSet, a selected key must be present, so
// a selector for `key: ""` does not match a worker without that key. An empty
// selector still matches every worker. Upstream selectors have only
// matchLabels.
func selectorMatches(selector, workerLabels map[string]string) bool {
	for key, value := range selector {
		if actual, present := workerLabels[key]; !present || actual != value {
			return false
		}
	}
	return true
}

// refuseForeignTemplates fails closed when a foreign template could reach
// the pool, quarantining and strongly retiring an existing pool.
func (d *Lifecycle) refuseForeignTemplates(ctx context.Context, atespace string, poolLabels map[string]string, pool *unstructured.Unstructured) error {
	match, err := d.foreignTemplateMatches(ctx, atespace, poolLabels)
	if err != nil {
		return err
	}
	if !match {
		return nil
	}
	if pool != nil {
		if err := d.retireSharedPool(ctx, pool, sharedQuarantined, errForeignTemplate.Error()); err != nil {
			return errors.Join(errForeignTemplate, err)
		}
	}
	return errForeignTemplate
}

// initiateSharedStrongRetirement starts the record pool's strong retirement
// using Kubernetes alone, so no native read can postpone it.
func (d *Lifecycle) initiateSharedStrongRetirement(ctx context.Context, record *journalRecord) error {
	shared := record.SharedWorker
	if shared == nil || shared.UID == "" {
		return nil
	}
	ctx, cancel := strongCleanupContext(ctx)
	defer cancel()
	pool, err := d.getSharedPool(ctx, record)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(pool.GetUID()) != shared.UID {
		return nil
	}
	return d.hardRetireSharedPool(ctx, pool, sharedRetiring, shared.Strong)
}

// sharedDoubt records the strong path for this record and starts retiring
// its pool at once. Nothing uncertain is ever replayed.
//
// A strong trigger often follows a native call that ran past the request's
// deadline, so it is persisted, and the retirement started, under a fresh
// bounded context detached from the caller's cancellation.
func (d *Lifecycle) sharedDoubt(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, reason string) error {
	ctx, cancel := strongCleanupContext(ctx)
	defer cancel()
	// The deletion is attempted whether or not the marker is durable: a
	// rejected journal write must never gate it. A deleting pool or an absent
	// Pod is itself strong evidence on every later pass.
	persistErr := d.markSharedStrong(ctx, cm, record, reason)
	return errors.Join(persistErr, d.initiateSharedStrongRetirement(ctx, record))
}

// egressSettle is how long deny-all egress must be stored before
// SuspendActor: longer than both the egress gateway's declared policy cache
// (--substrate-egress-policy-cache-ttl, which must equal the gateway's
// --egress-policy-cache-ttl) and the worker DNS relay's 5s cache. Upstream
// reports no enforcement acknowledgement to wait for instead. Cache expiry
// only governs new requests; already-admitted streams end when the checkpoint
// deactivates the tunnel.
func (c Config) egressSettle() time.Duration {
	return max(c.gatewayPolicyCacheTTL(), dnsRelayPolicyTTL) + sharedEgressSettleMargin
}

// sharedCleanupTimeout bounds persisting a strong trigger and starting the
// pool's retirement after the caller's own context has ended.
const sharedCleanupTimeout = 30 * time.Second

// strongCleanupContext detaches a strong-retirement step from the caller's
// cancellation (keeping its values) and bounds it on its own. It uses only
// Kubernetes, so it cannot be postponed by the native API either.
func strongCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), sharedCleanupTimeout)
}

// sharedEntryHeld reports whether an entry's exact journal record holds this
// pool's slot for the same Actor: name, and UID once the entry records one.
func sharedEntryHeld(record *journalRecord, pool *unstructured.Unstructured, entry sharedLedgerEntry) bool {
	shared := record.SharedWorker
	return shared != nil && !shared.Released && shared.Pool == pool.GetName() &&
		(shared.UID == "" || shared.UID == string(pool.GetUID())) && record.Placement.Namespace == pool.GetNamespace() &&
		actorKey(record) == entry.Actor && (entry.ActorUID == "" || entry.ActorUID == record.Actor.UID)
}

// sharedEntryPersists reports whether a fresh read of the same pool still
// holds exactly this entry. It confirms a disowned-entry suspicion read from
// an older snapshot, so a concurrent slot release is never mistaken for doubt.
func (d *Lifecycle) sharedEntryPersists(ctx context.Context, pool *unstructured.Unstructured, entry sharedLedgerEntry) (bool, error) {
	current := poolObject()
	if err := d.client.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if current.GetUID() != pool.GetUID() {
		return false, nil
	}
	ledger, err := readSharedLedger(current)
	if err != nil {
		// An unreadable ledger is doubt in its own right.
		return true, nil
	}
	return slices.Contains(ledger.Entries, entry), nil
}

// hardRetireSharedPool deletes a pool whose retirement is unconditional:
// a reached lifetime bound, unreadable or inconsistent lifetime evidence, a
// persisted retirement state, missing shared labels, or a withdrawn
// dedicated-install assertion. It records the state when it can, but no
// failed ledger write (including a conflict) can block the UID-preconditioned
// Delete.
func (d *Lifecycle) hardRetireSharedPool(ctx context.Context, pool *unstructured.Unstructured, state, reason string) error {
	if err := d.deleteSharedPool(ctx, pool); err != nil {
		// The decision must survive a failed Delete: persist the intent and
		// the state on its own context, so admission refuses the pool and
		// the next pass retries the Delete.
		d.persistSharedRetireDecision(ctx, pool, state, reason, err)
		return err
	}
	ctrl.LoggerFrom(ctx).Info("Substrate shared worker retired", "state", state, "reason", reason)
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedLedgerWriteTimeout)
	defer cancel()
	d.recordSharedRetirement(writeCtx, pool, state, reason)
	return nil
}

// deleteSharedPool is the mandatory step of every strong retirement: the
// UID-preconditioned foreground Delete, on its own fresh bounded context. A
// deletionTimestamp already stops admission and every startup verification.
func (d *Lifecycle) deleteSharedPool(ctx context.Context, pool *unstructured.Unstructured) error {
	if pool.GetDeletionTimestamp() != nil {
		return nil
	}
	deleteCtx, cancel := strongCleanupContext(ctx)
	defer cancel()
	uid := pool.GetUID()
	if err := d.client.Delete(deleteCtx, pool, client.Preconditions{UID: &uid}, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// recordSharedRetirement is best-effort bookkeeping after a pool's Delete:
// the retirement state and reason, written under the caller's bounded ctx.
// It never gates deletion or escalation.
func (d *Lifecycle) recordSharedRetirement(ctx context.Context, pool *unstructured.Unstructured, state, reason string) {
	if len(reason) > sharedLedgerReasonMax {
		reason = reason[:sharedLedgerReasonMax]
	}
	ledger, err := readSharedLedger(pool)
	if err != nil || ledger.State == state || ledger.State == sharedQuarantined {
		return
	}
	ledger.State, ledger.Reason = state, reason
	recorded := pool.DeepCopy()
	if err := setSharedLedger(recorded, ledger); err != nil {
		return
	}
	if err := d.client.Update(ctx, recorded); err != nil && !apierrors.IsNotFound(err) {
		ctrl.LoggerFrom(ctx).Info("Substrate shared worker retirement state not recorded; the pool is already deleting", "reason", err.Error())
	}
}

// sharedLedgerWriteTimeout bounds the best-effort retirement state write that
// follows a strong retirement's Delete.
const sharedLedgerWriteTimeout = 5 * time.Second

// sharedRetireIntended reports a decided strong retirement not yet deleted.
func sharedRetireIntended(pool *unstructured.Unstructured) bool {
	_, intended := pool.GetAnnotations()[sharedRetireIntentAnnotation]
	return intended
}

// persistSharedRetireDecision records, best effort and on its own short
// context, that the pool's strong retirement was decided after its Delete
// failed with deleteErr: the retirement-intent annotation, and the ledger's
// retirement state when it can be written, in one Update.
//
// The write is bound to the decided pool's UID. It reads the pool, does
// nothing if the live object has another UID (a replacement under the same
// name), and updates with the read's resourceVersion as its precondition, so
// it never lands on a pool replaced after the read. A Delete refused by its
// UID precondition (Conflict) means the decided pool is already gone or
// replaced, and nothing is written. It needs only get and update, the verbs
// the provider already holds on WorkerPools.
func (d *Lifecycle) persistSharedRetireDecision(ctx context.Context, pool *unstructured.Unstructured, state, reason string, deleteErr error) {
	uid := pool.GetUID()
	if uid == "" || apierrors.IsConflict(deleteErr) {
		return
	}
	if len(reason) > sharedLedgerReasonMax {
		reason = reason[:sharedLedgerReasonMax]
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedLedgerWriteTimeout)
	defer cancel()
	var err error
	for range sharedRetireDecisionAttempts {
		current := poolObject()
		if err = d.client.Get(writeCtx, client.ObjectKeyFromObject(pool), current); err != nil {
			break
		}
		if current.GetUID() != uid || current.GetDeletionTimestamp() != nil {
			return
		}
		changed := false
		if !sharedRetireIntended(current) {
			annotations := current.GetAnnotations()
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[sharedRetireIntentAnnotation] = reason
			current.SetAnnotations(annotations)
			changed = true
		}
		if ledger, readErr := readSharedLedger(current); readErr == nil && ledger.State != state && ledger.State != sharedQuarantined {
			ledger.State, ledger.Reason = state, reason
			// An oversized ledger keeps its state; the intent alone suffices.
			if setSharedLedger(current, ledger) == nil {
				changed = true
			}
		}
		if !changed {
			return
		}
		if err = d.client.Update(writeCtx, current); !apierrors.IsConflict(err) {
			break
		}
	}
	if err != nil && !apierrors.IsNotFound(err) {
		ctrl.LoggerFrom(ctx).Info("Substrate shared worker retirement decision not recorded", "reason", err.Error())
	}
}

// sharedRetireDecisionAttempts bounds the conflict retries of a retirement
// decision's write.
const sharedRetireDecisionAttempts = 3
