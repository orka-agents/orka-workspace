package substrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func clientKey(key sdk.AllocationKey) types.NamespacedName {
	return types.NamespacedName{Namespace: key.Namespace, Name: key.Name}
}

// verifyRetiringWorker proves a live fenced worker is this instance's private,
// single-Actor worker and hosts no other Actor before its Pod is terminated.
// DrainWorker needs global ownership and is not used: deleting the exact
// private Pod and pool terminates the sandbox, and exact Pod UID absence is
// the termination proof.
func (d *Lifecycle) verifyRetiringWorker(ctx context.Context, record *journalRecord) error {
	f := record.Worker
	if f == nil {
		return fmt.Errorf("native worker fence is missing")
	}
	worker, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: &pb.ObjectRef{Name: f.Name}})
	if err != nil {
		return err
	}
	if worker.GetMetadata().GetName() != f.Name || worker.GetMetadata().GetUid() != f.UID || worker.GetWorkerPodUid() != f.PodUID || worker.GetWorkerNamespace() != f.Namespace || worker.GetWorkerPool() != f.Pool || worker.GetWorkerPod() != f.Pod {
		return sdk.ErrStaleIdentity
	}
	if err := verifySingleActorWorker(record, worker); err != nil {
		return err
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: f.Namespace, Name: f.Pod}, pod); err != nil {
		return err
	}
	if string(pod.UID) != f.PodUID || pod.Labels[workerPoolLabel] != f.Pool {
		return sdk.ErrStaleIdentity
	}
	for key, expected := range workerLabels(record) {
		if value := pod.Labels[key]; (value != "" && value != expected) || (record.ChallengeSHA256 != "" && value != expected) {
			return sdk.ErrStaleIdentity
		}
	}
	return d.verifyWorkerExclusive(ctx, record, f.Name, false)
}

func (d *Lifecycle) workerAbsent(ctx context.Context, record *journalRecord) (bool, error) {
	if record.Worker == nil {
		return !record.BootRequested, nil
	}
	return d.workerFenceAbsent(ctx, record.Worker)
}

func (d *Lifecycle) workerFenceAbsent(ctx context.Context, f *workerFence) (bool, error) {
	pod := &corev1.Pod{}
	err := d.client.Get(ctx, types.NamespacedName{Namespace: f.Namespace, Name: f.Pod}, pod)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if string(pod.UID) != f.PodUID {
		return true, nil
	}
	return false, nil
}

// Native scheduling cannot pin a Pod UID atomically with Resume. Keep startup
// bound to the original worker, but permit exact authorized retirement of a
// replacement in this instance's independently owned private pool.
func (d *Lifecycle) captureRetirementWorker(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, actor *pb.Actor) error {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		return nil
	}
	matches := func(worker *workerFence) bool {
		return worker != nil && assignment.GetWorker().GetName() == worker.Name && assignment.GetWorkerNamespace() == worker.Namespace && assignment.GetWorkerPool() == worker.Pool && assignment.GetWorkerPod() == worker.Pod && assignment.GetWorkerPodUid() == worker.PodUID
	}
	if matches(record.Worker) {
		return nil
	}
	for i := range record.RetirementWorkers {
		if matches(&record.RetirementWorkers[i]) {
			return nil
		}
	}
	if record.Worker != nil {
		absent, err := d.workerAbsent(ctx, record)
		if err != nil || !absent {
			if err != nil {
				return err
			}
			return sdk.ErrStaleIdentity
		}
	}
	pool := poolObject()
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool); err != nil {
		return err
	}
	if string(pool.GetUID()) != record.RuntimePool.UID || record.RuntimePool.UID == "" || !runtimePoolMatches(pool, record) || assignment.GetWorkerNamespace() != record.Placement.Namespace || assignment.GetWorkerPool() != record.RuntimePool.Name || assignment.GetWorkerPodUid() == "" || assignment.GetWorkerPod() == "" {
		return sdk.ErrStaleIdentity
	}
	worker, err := d.worker(ctx, record, actor)
	if apierrors.IsNotFound(err) || status.Code(err) == codes.NotFound {
		// The exact assigned Pod is already absent. Preserve its UID as
		// retirement evidence; it can never become startup evidence.
		worker = &workerFence{Name: assignment.GetWorker().GetName(), Namespace: assignment.GetWorkerNamespace(), Pool: assignment.GetWorkerPool(), Pod: assignment.GetWorkerPod(), PodUID: assignment.GetWorkerPodUid(), AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.InstanceID}
		if absent, absenceErr := d.workerFenceAbsent(ctx, worker); absenceErr != nil || !absent {
			if absenceErr != nil {
				return absenceErr
			}
			return err
		}
	} else if err != nil {
		return err
	}
	if record.Worker == nil {
		record.Worker = worker
	} else {
		if len(record.RetirementWorkers) == 16 {
			return fmt.Errorf("native replacement retirement evidence exceeded bound")
		}
		record.RetirementWorkers = append(record.RetirementWorkers, *worker)
	}
	return d.save(ctx, cm, record)
}

func (d *Lifecycle) stop(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	actor, err := d.actor(ctx, record)
	if err != nil {
		return err
	}
	if actor == nil && record.Actor.UID == "" && record.Actor.CreateIssued {
		return fmt.Errorf("native Actor creation outcome is unresolved; preserving the journal")
	}
	if actor != nil && record.Actor.UID == "" {
		record.Actor.UID = actor.GetMetadata().GetUid()
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	if actor != nil {
		if err := d.captureRetirementWorker(ctx, cm, record, actor); err != nil {
			return err
		}
	}
	fences := append([]workerFence{}, record.RetirementWorkers...)
	if record.Worker != nil {
		fences = append(fences, *record.Worker)
	}
	allAbsent := true
	for i := range fences {
		absent, err := d.workerFenceAbsent(ctx, &fences[i])
		if err != nil {
			return err
		}
		if !absent {
			allAbsent = false
			retiring := *record
			retiring.Worker = &fences[i]
			if err := d.verifyRetiringWorker(ctx, &retiring); err != nil {
				return err
			}
		}
	}
	poolGone, err := d.deleteRuntimePool(ctx, cm, record)
	if err != nil {
		return err
	}
	if !allAbsent {
		if !record.WorkerRetiring {
			record.WorkerRetiring = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		for i := range fences {
			f := &fences[i]
			absent, err := d.workerFenceAbsent(ctx, f)
			if err != nil {
				return err
			}
			if absent {
				continue
			}
			pod := &corev1.Pod{}
			if err := d.client.Get(ctx, types.NamespacedName{Namespace: f.Namespace, Name: f.Pod}, pod); apierrors.IsNotFound(err) {
				continue
			} else if err != nil {
				return err
			}
			if string(pod.UID) != f.PodUID || pod.Labels[workerPoolLabel] != f.Pool {
				return sdk.ErrStaleIdentity
			}
			for key, expected := range workerLabels(record) {
				if value := pod.Labels[key]; value != expected {
					return sdk.ErrStaleIdentity
				}
			}
			if pod.DeletionTimestamp == nil {
				if err := d.client.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
		}
		return nil
	}
	if !poolGone {
		return nil
	}
	if record.BootRequested && record.Worker == nil {
		return fmt.Errorf("native boot has no independent exact worker termination proof")
	}
	if !record.WorkloadAbsent {
		record.WorkloadAbsent = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	if actor != nil {
		if !record.DeleteIssued {
			record.DeleteIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		// v1 deletes an Actor in every state; the UID precondition keeps a
		// recreated Actor under the same name out of reach.
		_, err := d.control.DeleteActor(ctx, &pb.DeleteActorRequest{Actor: actorRef(record), Options: &pb.DeleteOptions{Uid: actor.GetMetadata().GetUid()}})
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		return nil
	}
	record.Observation.State = sdk.AllocationStopped
	record.Observation.Startup = nil
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
	return d.save(ctx, cm, record)
}

// latestSnapshot returns the Actor's highest-generation snapshot.
func latestSnapshot(actor *pb.Actor) *pb.Snapshot {
	var latest *pb.Snapshot
	for _, snapshot := range actor.GetStatus().GetSnapshots() {
		if latest == nil || snapshot.GetGeneration() > latest.GetGeneration() {
			latest = snapshot
		}
	}
	return latest
}

// snapshotDigest pins a snapshot's immutable identity. Locality is a node
// cache binding that detach or revert clears, so it is excluded.
func snapshotDigest(snapshot *pb.Snapshot) string {
	if snapshot == nil {
		return ""
	}
	stable := proto.Clone(snapshot).(*pb.Snapshot)
	stable.Locality = ""
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(stable)
	if err != nil {
		return ""
	}
	return digest(data)
}

// verifyDataSnapshot accepts only a completed, durable, data-only snapshot.
func verifyDataSnapshot(snapshot *pb.Snapshot, owner pb.SnapshotOwner) error {
	if snapshot.GetUuid() == "" || snapshot.GetGeneration() < 1 || snapshot.GetOwner() != owner || snapshot.GetFidelity() != pb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES || snapshot.GetDurableSnapshot().GetState() != pb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED || snapshot.GetDurableSnapshot().GetObject().GetSnapshotUri() == "" {
		return fmt.Errorf("native snapshot is not a completed durable data-only snapshot")
	}
	return nil
}

// tagDigest pins a completed data-only Tag. v1 Tags report the template the
// snapshot was captured under but not their source Actor UID.
func tagDigest(tag *pb.Tag) (string, error) {
	if tag.GetMetadata().GetUid() == "" || tag.GetScope() != pb.TagScope_TAG_SCOPE_ATESPACE || tag.GetStatus().GetActorTemplateUid() == "" || verifyDataSnapshot(tag.GetStatus().GetSnapshot(), pb.SnapshotOwner_SNAPSHOT_OWNER_TAG) != nil {
		return "", fmt.Errorf("native Tag has no complete immutable data-only provenance")
	}
	status := proto.Clone(tag.GetStatus()).(*pb.TagStatus)
	status.Snapshot.Locality = ""
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&pb.Tag{Metadata: &pb.ResourceMetadata{Atespace: tag.GetMetadata().GetAtespace(), Name: tag.GetMetadata().GetName(), Uid: tag.GetMetadata().GetUid()}, Scope: tag.GetScope(), SourceActor: tag.GetSourceActor(), Status: status})
	return digest(data), err
}

func (d *Lifecycle) verifyCheckpoint(ctx context.Context, checkpoint *checkpointRecord) error {
	if checkpoint == nil || checkpoint.Tag.UID == "" || checkpoint.Tag.Digest == "" {
		return fmt.Errorf("native checkpoint record is incomplete")
	}
	tag, err := d.control.GetTag(ctx, &pb.GetTagRequest{Tag: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Tag.Name}})
	if err != nil {
		return err
	}
	hash, err := tagDigest(tag)
	if err != nil {
		return err
	}
	if tag.GetMetadata().GetUid() != checkpoint.Tag.UID || tag.GetMetadata().GetAtespace() != checkpoint.Atespace || tag.GetMetadata().GetName() != checkpoint.Tag.Name || tag.GetScope() != pb.TagScope_TAG_SCOPE_ATESPACE || tag.GetSourceActor().GetAtespace() != checkpoint.Atespace || tag.GetSourceActor().GetName() != checkpoint.SourceName || checkpoint.SourceUID == "" || tag.GetStatus().GetActorTemplateUid() != checkpoint.Template.UID || checkpoint.SnapshotURI == "" || tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetObject().GetSnapshotUri() != checkpoint.SnapshotURI || hash != checkpoint.Tag.Digest {
		return sdk.ErrStaleIdentity
	}
	return d.verifyTemplate(ctx, checkpoint.Atespace, checkpoint.Template)
}

func (d *Lifecycle) SuspendInstance(ctx context.Context, key sdk.AllocationKey, identity sdk.InstanceIdentity) (sdk.AllocationObservation, error) {
	var observed sdk.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, key)
		if err != nil {
			return err
		}
		if record.Observation.Identity != identity {
			return sdk.ErrStaleIdentity
		}
		observed = record.Observation
		if record.Failure != "" {
			return fmt.Errorf("a failed native instance has no data to suspend; Stop retires it")
		}
		if record.Operation == "suspend" {
			if err := d.reserveSuspended(ctx, record); err != nil {
				return err
			}
		}
		if record.Operation == "suspend" && record.Observation.State == sdk.AllocationStopped {
			return d.verifyCheckpoint(ctx, record.Checkpoint)
		}
		if !record.SuspendEnabled || record.Operation == "delete" || record.Operation == "stop" {
			return fmt.Errorf("native suspension is unsupported or already retired")
		}
		if err := d.requireJournal(ctx, cm, record); err != nil {
			return err
		}
		if record.Operation != "suspend" {
			if record.ChallengeSHA256 == "" || record.Worker == nil {
				return fmt.Errorf("native suspension requires the exact process and worker identities")
			}
			actor, err := d.actor(ctx, record)
			if err != nil {
				return err
			}
			if actor == nil || actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING {
				return fmt.Errorf("native checkpoint requires the admitted running Actor")
			}
			name, err := randomName("orka-data-")
			if err != nil {
				return err
			}
			if err := d.reserveSuspended(ctx, record); err != nil {
				return err
			}
			record.Pending = &checkpointIntent{Name: name, Tag: nativeReference{Name: name}, PriorGeneration: actor.GetStatus().GetLastAssignedGeneration(), PriorSnapshotDigest: snapshotDigest(latestSnapshot(actor))}
			record.Operation = "suspend"
			record.Observation = pendingObservation(record)
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		if record.Pending != nil {
			err := d.capture(ctx, cm, record)
			if lost := (*nativeInstanceLostError)(nil); errors.As(err, &lost) {
				// The capture source is gone, so this capture can never finish.
				// The failed instance retires through the exact Stop path.
				observed, err = d.fail(ctx, cm, record, lost.reason)
				return err
			}
			if err != nil {
				return err
			}
			if record.Pending != nil {
				observed = record.Observation
				return nil
			}
		}
		if err := d.stop(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		return nil
	})
	return observed, err
}

func (d *Lifecycle) capture(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	operation := record.Pending
	actor, err := d.actor(ctx, record)
	if err != nil {
		return err
	}
	if actor == nil && record.Actor.UID != "" {
		return lostInstance(record, nativeActorDisappeared)
	}
	if actor.GetStatus().GetState() == pb.ActorState_ACTOR_STATE_CRASHED {
		return lostInstance(record, nativeActorStopped)
	}
	if actor == nil || record.Worker == nil || actor.GetActorTemplate().GetName() != record.Template.Name {
		return fmt.Errorf("native checkpoint lost its source lifetime or template")
	}
	// The template is immutable and pinned by UID; its snapshots are data-only.
	if err := d.verifyTemplate(ctx, record.Atespace, record.Template); err != nil {
		return err
	}
	if !operation.SuspendIssued {
		if actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING {
			return fmt.Errorf("native Actor stopped before checkpoint intent was issued")
		}
		if err := d.verifyRetiringWorker(ctx, record); err != nil {
			return err
		}
		operation.SuspendIssued = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		// Synchronous durability returns only after the snapshot is uploaded,
		// so CreateTag copies it without changing the Actor.
		_, err := d.control.SuspendActor(ctx, &pb.SuspendActorRequest{Actor: actorRef(record), Options: &pb.SuspendOptions{Mode: pb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}})
		return err
	}
	state := actor.GetStatus().GetState()
	if state == pb.ActorState_ACTOR_STATE_SUSPENDING {
		return nil
	}
	if state != pb.ActorState_ACTOR_STATE_SUSPENDED {
		return fmt.Errorf("native suspension outcome is unresolved for Actor state %s after request issuance; refusing replay", state)
	}
	snapshot := latestSnapshot(actor)
	hash := snapshotDigest(snapshot)
	if verifyDataSnapshot(snapshot, pb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR) != nil || snapshot.GetActorTemplateUid() != record.Template.UID || snapshot.GetGeneration() <= operation.PriorGeneration || hash == operation.PriorSnapshotDigest {
		return fmt.Errorf("native suspension exposed no new completed data-only snapshot under the recorded template")
	}
	if operation.SourceSnapshotDigest == "" {
		operation.SourceSnapshotDigest = hash
		operation.SourceVersion = actor.GetMetadata().GetVersion()
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	if hash != operation.SourceSnapshotDigest || actor.GetMetadata().GetVersion() != operation.SourceVersion {
		return fmt.Errorf("native source changed during Tag capture")
	}
	ref := &pb.ObjectRef{Atespace: record.Atespace, Name: operation.Name}
	tag, err := d.control.GetTag(ctx, &pb.GetTagRequest{Tag: ref})
	if status.Code(err) == codes.NotFound {
		if operation.Tag.UID != "" {
			return sdk.ErrStaleIdentity
		}
		if !operation.Tag.CreateIssued {
			operation.Tag.CreateIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		tag, err = d.control.CreateTag(ctx, &pb.CreateTagRequest{Tag: &pb.Tag{Metadata: &pb.ResourceMetadata{Atespace: record.Atespace, Name: operation.Name}, Scope: pb.TagScope_TAG_SCOPE_ATESPACE, SourceActor: actorRef(record)}})
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
	}
	if err != nil {
		return err
	}
	if tag.GetMetadata().GetUid() == "" || tag.GetMetadata().GetAtespace() != record.Atespace || tag.GetMetadata().GetName() != operation.Name || !proto.Equal(tag.GetSourceActor(), actorRef(record)) || tag.GetScope() != pb.TagScope_TAG_SCOPE_ATESPACE {
		return sdk.ErrStaleIdentity
	}
	if operation.Tag.UID == "" {
		operation.Tag.UID = tag.GetMetadata().GetUid()
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	} else if operation.Tag.UID != tag.GetMetadata().GetUid() {
		return sdk.ErrStaleIdentity
	}
	if tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState() != pb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
		return nil
	}
	if tag.GetStatus().GetActorTemplateUid() != record.Template.UID {
		return fmt.Errorf("native Tag has incorrect template provenance")
	}
	tagHash, err := tagDigest(tag)
	if err != nil {
		return err
	}
	current, err := d.actor(ctx, record)
	if err != nil {
		return err
	}
	// Before/after fence: the same Actor UID at the same version still holds
	// the same snapshot, so the Tag copied exactly this capture.
	if current == nil || current.GetMetadata().GetUid() != record.Actor.UID || current.GetMetadata().GetVersion() != operation.SourceVersion || current.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_SUSPENDED || snapshotDigest(latestSnapshot(current)) != operation.SourceSnapshotDigest {
		return fmt.Errorf("native source changed during immutable Tag copy")
	}
	operation.Tag.Digest = tagHash
	checkpoint := &checkpointRecord{Tag: operation.Tag, Atespace: record.Atespace, SourceName: record.Actor.Name, SourceUID: record.Actor.UID, Template: record.Template, SnapshotURI: tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetObject().GetSnapshotUri(), CreatedAt: metav1.Now()}
	reference, err := d.registerCheckpoint(ctx, record, checkpoint)
	if err != nil {
		return err
	}
	record.Checkpoint = checkpoint
	record.CheckpointCatalog = reference
	record.Pending = nil
	return d.save(ctx, cm, record)
}

func deletedDisposition(data bool) *api.ExecutionWorkspaceDisposition {
	disposition := &api.ExecutionWorkspaceDisposition{Compute: api.DispositionDeleted, AccessCredentials: api.DispositionNotApplicable, EphemeralSecrets: api.DispositionNotApplicable, WorkspaceData: api.DispositionNotApplicable, PersistentVolumes: api.DispositionNotApplicable, Checkpoints: api.DispositionNotApplicable, ProviderResources: api.DispositionDeleted}
	if data {
		disposition.WorkspaceData = api.DispositionDeleted
		disposition.Checkpoints = api.DispositionDeleted
	}
	return disposition
}

func (d *Lifecycle) DeleteAllocation(ctx context.Context, key sdk.AllocationKey, identity sdk.InstanceIdentity, policy api.ExecutionWorkspaceDeletionPolicy) (sdk.AllocationObservation, error) {
	var observed sdk.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, key)
		if err != nil {
			return err
		}
		if record.Observation.Identity != identity {
			return sdk.ErrStaleIdentity
		}
		observed = record.Observation
		if record.Observation.State != sdk.AllocationStopped && record.Observation.State != sdk.AllocationDeleted {
			return sdk.ErrInstanceRunning
		}
		if record.DeletionPolicy != nil && !samePolicy(*record.DeletionPolicy, policy) {
			return sdk.ErrRequestConflict
		}
		if record.Observation.State == sdk.AllocationDeleted {
			return nil
		}
		if policy.ProviderResources != api.WorkspaceDeletionActionDelete || policy.Checkpoints != api.WorkspaceDeletionActionDelete || policy.PersistentVolumes != api.WorkspaceDeletionActionDelete {
			return fmt.Errorf("native runtime currently requires delete policy")
		}
		if err := d.requireJournal(ctx, cm, record); err != nil {
			return err
		}
		if record.Operation != "delete" {
			record.Operation = "delete"
			record.DeletionPolicy = &policy
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		for sequence := int64(1); sequence <= record.Request.Sequence; sequence++ {
			entry := record
			entryCM := cm
			if sequence < record.Request.Sequence {
				entryCM, entry, err = d.readAt(ctx, key, historyKey(key, sequence))
				if err != nil {
					return err
				}
			}
			complete, err := d.cleanupRecord(ctx, entryCM, entry)
			if err != nil {
				return err
			}
			if !complete {
				return nil
			}
		}
		if err := d.releaseSuspended(ctx, record); err != nil {
			return err
		}
		record.SuspendedReservation = false
		record.Observation.State = sdk.AllocationDeleted
		record.Observation.Startup = nil
		record.Observation.RetainedData = nil
		record.Observation.Disposition = deletedDisposition(record.SuspendEnabled)
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		return nil
	})
	return observed, err
}

func (d *Lifecycle) cleanupRecord(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) (bool, error) {
	actor, err := d.actor(ctx, record)
	if err != nil {
		return false, err
	}
	if actor != nil {
		return false, sdk.ErrInstanceRunning
	}
	complete, err := d.releaseRecordArtifacts(ctx, record)
	if err != nil || !complete {
		return complete, err
	}
	for _, checkpoint := range []*checkpointRecord{record.Checkpoint, record.InheritedCheckpoint} {
		if checkpoint == nil {
			continue
		}
		referenced, err := d.catalogTagReferenced(ctx, record.Request.Key.Namespace, checkpoint.Atespace, checkpoint.Tag.UID)
		if err != nil {
			return false, err
		}
		if referenced {
			continue
		}
		tag, err := d.control.GetTag(ctx, &pb.GetTagRequest{Tag: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Tag.Name}})
		if status.Code(err) == codes.NotFound {
			continue
		}
		if err != nil {
			return false, err
		}
		if tag.GetMetadata().GetUid() != checkpoint.Tag.UID {
			return false, sdk.ErrStaleIdentity
		}
		if _, err := d.control.DeleteTag(ctx, &pb.DeleteTagRequest{Tag: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Tag.Name}, Options: &pb.DeleteOptions{Uid: checkpoint.Tag.UID}}); err != nil && status.Code(err) != codes.NotFound {
			return false, err
		}
		return false, nil
	}
	if record.Pending != nil && record.Pending.Tag.CreateIssued {
		tag, err := d.control.GetTag(ctx, &pb.GetTagRequest{Tag: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Pending.Name}})
		if status.Code(err) == codes.NotFound && record.Pending.Tag.UID == "" {
			return false, fmt.Errorf("native Tag creation outcome is unresolved")
		}
		if err != nil && status.Code(err) != codes.NotFound {
			return false, err
		}
		if tag != nil {
			if record.Pending.Tag.UID == "" {
				record.Pending.Tag.UID = tag.GetMetadata().GetUid()
				if err := d.save(ctx, cm, record); err != nil {
					return false, err
				}
			}
			if tag.GetMetadata().GetUid() != record.Pending.Tag.UID {
				return false, sdk.ErrStaleIdentity
			}
			if _, err := d.control.DeleteTag(ctx, &pb.DeleteTagRequest{Tag: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Pending.Name}, Options: &pb.DeleteOptions{Uid: record.Pending.Tag.UID}}); err != nil {
				return false, err
			}
			return false, nil
		}
	}
	if record.Template.CreateIssued {
		// Infrastructure creation can be accepted before either its response
		// or journaled UID survives. Cancellation must recover that lifetime
		// before checking references or attempting deletion.
		if record.Template.UID == "" {
			template, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name}})
			if status.Code(err) == codes.NotFound {
				return false, fmt.Errorf("native template creation outcome is unresolved")
			}
			if err != nil {
				return false, err
			}
			hash, err := templateDigest(template)
			if err != nil {
				return false, err
			}
			if template.GetMetadata().GetUid() == "" || hash != record.Template.Digest {
				return false, sdk.ErrStaleIdentity
			}
			record.Template.UID = template.GetMetadata().GetUid()
			if err := d.save(ctx, cm, record); err != nil {
				return false, err
			}
		}
		referenced, err := d.catalogTemplateReferenced(ctx, record.Request.Key.Namespace, record.Atespace, record.Template.UID)
		if err != nil {
			return false, err
		}
		if !referenced {
			template, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name}})
			if status.Code(err) == codes.NotFound && record.Template.UID == "" {
				return false, fmt.Errorf("native template creation outcome is unresolved")
			}
			if err != nil && status.Code(err) != codes.NotFound {
				return false, err
			}
			if template != nil {
				if template.GetMetadata().GetUid() != record.Template.UID {
					return false, sdk.ErrStaleIdentity
				}
				if _, err := d.control.DeleteActorTemplate(ctx, &pb.DeleteActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name}, Options: &pb.DeleteOptions{Uid: record.Template.UID}}); err != nil {
					return false, err
				}
				return false, nil
			}
		}
	}
	for _, entry := range []struct {
		ref    *nativeReference
		object client.Object
	}{{&record.Anchor, &corev1.ConfigMap{}}} {
		if !entry.ref.CreateIssued {
			continue
		}
		err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Placement.Namespace, Name: entry.ref.Name}, entry.object)
		if apierrors.IsNotFound(err) {
			if entry.ref.UID == "" {
				return false, fmt.Errorf("native infrastructure creation outcome is unresolved")
			}
			continue
		}
		if err != nil {
			return false, err
		}
		if entry.object.GetUID() == "" || entry.ref.UID != "" && string(entry.object.GetUID()) != entry.ref.UID || !reflect.DeepEqual(entry.object.GetLabels(), labels(record)) {
			return false, sdk.ErrStaleIdentity
		}
		if entry.ref.UID == "" {
			switch object := entry.object.(type) {
			case *corev1.ConfigMap:
				expected := map[string]string{"journalName": cm.Name, "journalNamespace": cm.Namespace, "journalUID": string(cm.UID)}
				if !reflect.DeepEqual(object.Data, expected) || len(object.BinaryData) != 0 || len(object.OwnerReferences) != 0 {
					return false, sdk.ErrStaleIdentity
				}
			}
			entry.ref.UID = string(entry.object.GetUID())
			if err := d.save(ctx, cm, record); err != nil {
				return false, err
			}
		}
		uid := entry.object.GetUID()
		if err := d.client.Delete(ctx, entry.object, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}
