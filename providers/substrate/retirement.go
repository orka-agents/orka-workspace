package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func clientKey(key sdk.AllocationKey) types.NamespacedName {
	return types.NamespacedName{Namespace: key.Namespace, Name: key.Name}
}

func (d *Lifecycle) drainWorker(ctx context.Context, record *journalRecord) error {
	f := record.Worker
	if f == nil {
		return fmt.Errorf("native worker fence is missing")
	}
	ref := &pb.ObjectRef{Name: f.Name}
	worker, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: ref})
	if err != nil {
		return err
	}
	verify := func(w *pb.Worker) bool {
		return w.GetMetadata().GetName() == f.Name && w.GetMetadata().GetUid() == f.UID && w.GetWorkerPodUid() == f.PodUID && w.GetWorkerNamespace() == f.Namespace && w.GetWorkerPool() == f.Pool && w.GetWorkerPod() == f.Pod && w.GetStatus().GetCapacity().GetActors() == 1
	}
	if !verify(worker) {
		return sdk.ErrStaleIdentity
	}
	pod := &corev1.Pod{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: f.Namespace, Name: f.Pod}, pod); err != nil {
		return err
	}
	if string(pod.UID) != f.PodUID || pod.DeletionTimestamp != nil || pod.Labels[workerPoolLabel] != f.Pool {
		return sdk.ErrStaleIdentity
	}
	for key, expected := range workerLabels(record) {
		if value := pod.Labels[key]; (value != "" && value != expected) || (record.ChallengeSHA256 != "" && value != expected) {
			return sdk.ErrStaleIdentity
		}
	}
	if worker.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_DRAINING {
		worker, err = d.control.DrainWorker(ctx, &pb.DrainWorkerRequest{Worker: ref})
		if err != nil {
			return err
		}
	}
	if !verify(worker) || worker.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_DRAINING {
		return sdk.ErrStaleIdentity
	}
	page := ""
	seen := map[string]bool{}
	for {
		assignments, err := d.control.ListWorkerActorAssignments(ctx, &pb.ListWorkerActorAssignmentsRequest{Worker: ref, PageSize: 1000, PageToken: page})
		if err != nil {
			return err
		}
		for _, assignment := range assignments.GetActorAssignments() {
			if assignment.GetActorUid() != record.Actor.UID || !proto.Equal(assignment.GetActor(), actorRef(record)) {
				return fmt.Errorf("native worker has another Actor assignment; shared teardown refused")
			}
		}
		page = assignments.GetNextPageToken()
		if page == "" {
			return nil
		}
		if seen[page] {
			return fmt.Errorf("native assignment pagination repeated a token")
		}
		seen[page] = true
	}
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
			if err := d.drainWorker(ctx, &retiring); err != nil {
				return err
			}
		}
	}
	poolGone, err := d.deleteRuntimePool(ctx, cm, record)
	if err != nil {
		return err
	}
	if !allAbsent {
		if !record.WorkerDrained {
			record.WorkerDrained = true
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
		_, err := d.control.DeleteActor(ctx, &pb.DeleteActorRequest{Actor: actorRef(record), AnyState: true})
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

func snapshotDigest(snapshot *pb.ExternalSnapshot) string {
	if snapshot == nil {
		return ""
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
	if err != nil {
		return ""
	}
	return digest(data)
}
func tagDigest(tag *pb.Tag) (string, error) {
	if tag.GetMetadata().GetUid() == "" || tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" || tag.GetStatus().GetSnapshot().GetContentScope() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || tag.GetStatus().GetSourceActorUid() == "" || tag.GetStatus().GetActorTemplateUid() == "" {
		return "", fmt.Errorf("native Tag has no complete immutable Data provenance")
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&pb.Tag{Metadata: &pb.ResourceMetadata{Atespace: tag.GetMetadata().GetAtespace(), Name: tag.GetMetadata().GetName(), Uid: tag.GetMetadata().GetUid()}, SourceActor: tag.GetSourceActor(), Status: tag.GetStatus()})
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
	if tag.GetMetadata().GetUid() != checkpoint.Tag.UID || tag.GetMetadata().GetAtespace() != checkpoint.Atespace || tag.GetMetadata().GetName() != checkpoint.Tag.Name || tag.GetScope() != pb.TagScope_TAG_SCOPE_ATESPACE || tag.GetSourceActor().GetAtespace() != checkpoint.Atespace || tag.GetSourceActor().GetName() != checkpoint.SourceName || tag.GetStatus().GetSourceActorUid() != checkpoint.SourceUID || tag.GetStatus().GetActorTemplateUid() != checkpoint.Template.UID || hash != checkpoint.Tag.Digest {
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
			record.Pending = &checkpointIntent{Name: name, Tag: nativeReference{Name: name}, PriorSnapshotDigest: snapshotDigest(actor.GetStatus().GetExternalSnapshot())}
			record.Operation = "suspend"
			record.Observation = pendingObservation(record)
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		if record.Pending != nil {
			if err := d.capture(ctx, cm, record); err != nil {
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
	if actor == nil || record.Worker == nil || actor.GetStatus().GetCurrentActorTemplateUid() != record.Template.UID {
		return fmt.Errorf("native checkpoint lost its source lifetime or template")
	}
	if err := d.verifyTemplate(ctx, record.Atespace, record.Template); err != nil {
		return err
	}
	if !record.WorkerDrained {
		if err := d.drainWorker(ctx, record); err != nil {
			return err
		}
		record.WorkerDrained = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	if !operation.SuspendIssued {
		if actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING {
			return fmt.Errorf("native Actor stopped before checkpoint intent was issued")
		}
		operation.SuspendIssued = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		_, err := d.control.SuspendActor(ctx, &pb.SuspendActorRequest{Actor: actorRef(record)})
		return err
	}
	state := actor.GetStatus().GetState()
	if state == pb.ActorState_ACTOR_STATE_SUSPENDING {
		return nil
	}
	if state != pb.ActorState_ACTOR_STATE_SUSPENDED {
		return fmt.Errorf("native suspension outcome is unresolved for Actor state %s after request issuance; refusing replay", state)
	}
	snapshot := actor.GetStatus().GetExternalSnapshot()
	hash := snapshotDigest(snapshot)
	if snapshot.GetContentScope() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA || snapshot.GetSnapshotUri() == "" || hash == operation.PriorSnapshotDigest {
		return fmt.Errorf("native suspension exposed no new complete Data snapshot")
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
	if tag.GetStatus().GetSnapshot() == nil {
		return nil
	}
	if tag.GetStatus().GetSourceActorUid() != record.Actor.UID || tag.GetStatus().GetActorTemplateUid() != record.Template.UID {
		return fmt.Errorf("native Tag has incorrect source provenance")
	}
	tagHash, err := tagDigest(tag)
	if err != nil {
		return err
	}
	current, err := d.actor(ctx, record)
	if err != nil {
		return err
	}
	if current == nil || current.GetMetadata().GetVersion() != operation.SourceVersion || current.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_SUSPENDED || snapshotDigest(current.GetStatus().GetExternalSnapshot()) != operation.SourceSnapshotDigest {
		return fmt.Errorf("native source changed during immutable Tag copy")
	}
	operation.Tag.Digest = tagHash
	checkpoint := &checkpointRecord{Tag: operation.Tag, Atespace: record.Atespace, SourceName: record.Actor.Name, SourceUID: record.Actor.UID, Template: record.Template, CreatedAt: metav1.Now()}
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
		if _, err := d.control.DeleteTag(ctx, &pb.DeleteTagRequest{Tag: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Tag.Name}}); err != nil && status.Code(err) != codes.NotFound {
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
			if _, err := d.control.DeleteTag(ctx, &pb.DeleteTagRequest{Tag: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Pending.Name}}); err != nil {
				return false, err
			}
			return false, nil
		}
	}
	if record.Template.CreateIssued {
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
				if _, err := d.control.DeleteActorTemplate(ctx, &pb.DeleteActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name}}); err != nil {
					return false, err
				}
				return false, nil
			}
		}
	}
	for _, entry := range []struct {
		ref    nativeReference
		object client.Object
	}{{record.NetworkPolicy, &networkingv1.NetworkPolicy{}}, {record.Anchor, &corev1.ConfigMap{}}} {
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
		if string(entry.object.GetUID()) != entry.ref.UID || !reflect.DeepEqual(entry.object.GetLabels(), labels(record)) {
			return false, sdk.ErrStaleIdentity
		}
		uid := entry.object.GetUID()
		if err := d.client.Delete(ctx, entry.object, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}
