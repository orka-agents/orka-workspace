package substrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

func newRecord(request sdk.WorkloadRequest) (*journalRecord, error) {
	id, err := randomName("ate-")
	if err != nil {
		return nil, err
	}
	return &journalRecord{Version: journalVersion, Request: request, Operation: "ensure", Actor: nativeReference{Name: id}, Template: nativeReference{Name: id + "-template"}, Anchor: nativeReference{Name: id + "-owner"}, NetworkPolicy: nativeReference{Name: id + "-network"}, Observation: sdk.AllocationObservation{Key: request.Key, Sequence: request.Sequence, Identity: sdk.InstanceIdentity{AllocationID: id, InstanceID: id, RequestRevision: request.Revision}, State: sdk.AllocationPending}}, nil
}

func (d *Lifecycle) EnsureAllocation(ctx context.Context, request sdk.WorkloadRequest) (sdk.AllocationObservation, error) {
	if err := validateRequest(request); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if d.control == nil || d.config.ActorDNSSuffix == "" || !d.config.DirectEgressEnabled {
		return sdk.AllocationObservation{}, fmt.Errorf("native control, Actor routing, and direct egress are required")
	}
	var observed sdk.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, request.Key)
		if err != nil && !errors.Is(err, sdk.ErrNotFound) {
			return err
		}
		if record != nil && request.Sequence < record.Request.Sequence {
			_, retired, err := d.readAt(ctx, request.Key, historyKey(request.Key, request.Sequence))
			if err != nil {
				return err
			}
			if retired.Request.Revision != request.Revision {
				return sdk.ErrRequestConflict
			}
			observed = retired.Observation
			return nil
		}
		if record != nil && request.Sequence == record.Request.Sequence {
			if request.Revision != record.Request.Revision {
				return sdk.ErrRequestConflict
			}
			observed = record.Observation
			if record.Operation != "ensure" {
				return nil
			}
		}
		if err := d.admitted(ctx, request); err != nil {
			return err
		}
		if record == nil {
			// New journals carry protection atomically at creation.
			if err := d.proveNoAllocation(ctx, request.Key); err != nil {
				return err
			}
			if err := sdk.ValidateWorkloadTransition(nil, request, nil, false); err != nil {
				return err
			}
			record, err = newRecord(request)
			if err != nil {
				return err
			}
			if err := d.resolveProfile(ctx, record); err != nil {
				return err
			}
			if slices.Contains(request.Runtime.RequiredFeatures, api.WorkspaceFeatureSuspend) && !record.SuspendEnabled {
				return fmt.Errorf("suspend feature requires a data-only profile")
			}
			data, err := encode(record)
			if err != nil {
				return err
			}
			key := journalKey(request.Key)
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: labels(record), Finalizers: []string{journalProtectionFinalizer}, OwnerReferences: []metav1.OwnerReference{workspaceOwner(request.Key)}}, Data: map[string]string{journalDataKey: data}}
			if err := d.client.Create(ctx, cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, err)
				}
				return err
			}
		} else if request.Sequence != record.Request.Sequence {
			if record.Operation == "delete" || record.Observation.State == sdk.AllocationDeleted {
				return sdk.ErrRequestConflict
			}
			if err := sdk.ValidateWorkloadTransition(&record.Request, request, &record.Observation, record.SuspendEnabled); err != nil {
				return err
			}
			if err := d.requireJournal(ctx, cm, record); err != nil {
				return err
			}
			if record.Checkpoint != nil {
				if err := d.reserveSuspended(ctx, record); err != nil {
					return err
				}
				if err := d.verifyCheckpoint(ctx, record.Checkpoint); err != nil {
					return err
				}
			}
			if actor, err := d.actor(ctx, record); err != nil {
				return err
			} else if actor != nil {
				return sdk.ErrInstanceRunning
			}
			fresh, err := newRecord(request)
			if err != nil {
				return err
			}
			fresh.Observation.Identity.AllocationID = record.Observation.Identity.AllocationID
			if err := d.resolveProfile(ctx, fresh); err != nil {
				return err
			}
			if record.SuspendEnabled && (!fresh.SuspendEnabled || !compatibleRestore(record.TemplateSpec, fresh.TemplateSpec) || record.Placement != fresh.Placement || !compatibleRuntimePool(record.RuntimePoolSpec, fresh.RuntimePoolSpec)) {
				return fmt.Errorf("native resume changed immutable infrastructure or durable layout")
			}
			fresh.InheritedCheckpoint = record.Checkpoint
			fresh.InheritedCatalog = record.CheckpointCatalog
			if record.Checkpoint != nil {
				fresh.CreateTemplate = record.Checkpoint.Template
			}
			if err := d.archive(ctx, cm, record); err != nil {
				return err
			}
			if err := d.save(ctx, cm, fresh); err != nil {
				return err
			}
			record = fresh
		}
		observed = pendingObservation(record)
		if err := d.requireJournal(ctx, cm, record); err != nil {
			return err
		}
		if err := d.importCheckpoint(ctx, cm, record); err != nil {
			return err
		}
		if err := d.verifyInheritedCheckpoint(ctx, record); err != nil {
			return err
		}
		if err := d.ensureInfrastructure(ctx, cm, record); err != nil {
			return err
		}
		if err := d.ensureNative(ctx, cm, record); err != nil {
			return err
		}
		actor, err := d.actor(ctx, record)
		if err != nil {
			return err
		}
		if actor == nil || actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING {
			return nil
		}
		if record.Worker != nil {
			if err := d.bindWorker(ctx, record); err != nil {
				return err
			}
		}
		worker, err := d.worker(ctx, record, actor)
		if err != nil {
			return err
		}
		if record.Worker == nil {
			record.Worker = worker
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
			if err := d.bindWorker(ctx, record); err != nil {
				return err
			}
		} else if *record.Worker != *worker {
			return sdk.ErrStaleIdentity
		}
		hash, err := d.challenge(ctx, record, actor)
		if err != nil {
			return err
		}
		if record.ChallengeSHA256 == "" {
			record.ChallengeSHA256 = hash
			record.ChallengeVersion = actor.GetMetadata().GetVersion()
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		observed, err = d.observeReady(ctx, record)
		if err != nil {
			return err
		}
		record.Observation = observed
		if observed.State == sdk.AllocationReady && record.Request.Sequence > 1 {
			if err := d.releaseSuspended(ctx, record); err != nil {
				return err
			}
		}
		return d.save(ctx, cm, record)
	})
	return observed, err
}

func (d *Lifecycle) ensureNative(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	template, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name}})
	if status.Code(err) == codes.NotFound {
		if record.Template.UID != "" {
			return sdk.ErrStaleIdentity
		}
		if !record.Template.CreateIssued {
			record.Template.CreateIssued = true
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		template, err = d.control.CreateActorTemplate(ctx, &pb.CreateActorTemplateRequest{ActorTemplate: record.TemplateSpec})
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
	}
	if err != nil {
		return err
	}
	hash, err := templateDigest(template)
	if err != nil {
		return err
	}
	if hash != record.Template.Digest || template.GetMetadata().GetUid() == "" || record.Template.UID != "" && record.Template.UID != template.GetMetadata().GetUid() {
		return sdk.ErrStaleIdentity
	}
	if record.Template.UID == "" {
		record.Template.UID = template.GetMetadata().GetUid()
		if record.CreateTemplate.UID == "" {
			record.CreateTemplate = record.Template
		}
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	for _, ref := range []nativeReference{record.Template, record.CreateTemplate} {
		if err := d.verifyTemplate(ctx, record.Atespace, ref); err != nil {
			return err
		}
	}
	actor, err := d.actor(ctx, record)
	if err != nil {
		return err
	}
	if actor == nil {
		if record.Actor.UID != "" {
			return fmt.Errorf("recorded native Actor disappeared; uncertain work is never replayed")
		}
		if record.Actor.CreateIssued {
			return fmt.Errorf("native Actor creation was already issued without an observed lifetime; uncertain work is never replayed")
		}
		if record.InheritedCheckpoint != nil {
			if err := d.verifyCheckpoint(ctx, record.InheritedCheckpoint); err != nil {
				return err
			}
		}
		record.Actor.CreateIssued = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		create := &pb.Actor{Metadata: &pb.ResourceMetadata{Atespace: record.Atespace, Name: record.Actor.Name}, ActorTemplate: &pb.ObjectRef{Atespace: record.Atespace, Name: record.CreateTemplate.Name}}
		if record.InheritedCheckpoint != nil {
			create.SourceTag = &pb.ObjectRef{Atespace: record.InheritedCheckpoint.Atespace, Name: record.InheritedCheckpoint.Tag.Name}
		}
		actor, err = d.control.CreateActor(ctx, &pb.CreateActorRequest{Actor: create})
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		if err != nil {
			return err
		}
	}
	if record.Actor.UID == "" {
		if actor.GetMetadata().GetUid() == "" || actor.GetMetadata().GetAtespace() != record.Atespace || actor.GetMetadata().GetName() != record.Actor.Name || actor.GetActorTemplate().GetName() != record.CreateTemplate.Name {
			return sdk.ErrStaleIdentity
		}
		if record.InheritedCheckpoint != nil && (!proto.Equal(actor.GetSourceTag(), &pb.ObjectRef{Atespace: record.InheritedCheckpoint.Atespace, Name: record.InheritedCheckpoint.Tag.Name}) || actor.GetStatus().GetCurrentActorTemplateUid() != record.CreateTemplate.UID || actor.GetStatus().GetExternalSnapshot().GetContentScope() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA) {
			return fmt.Errorf("native restore does not identify the expected Data Tag and template")
		}
		record.Actor.UID = actor.GetMetadata().GetUid()
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	if actor.GetActorTemplate().GetName() != record.Template.Name {
		if actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_SUSPENDED || record.BootRequested {
			return fmt.Errorf("restored native Actor left suspension before bootstrap rotation")
		}
		update := proto.Clone(actor).(*pb.Actor)
		update.ActorTemplate = &pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name}
		// The upstream UpdateActor checks the observed UID/version. That CAS
		// belongs only to this operation, never to Suspend/Resume/Delete.
		if _, err := d.control.UpdateActor(ctx, &pb.UpdateActorRequest{Actor: update}); err != nil {
			return err
		}
		return nil
	}
	if !record.BootRequested {
		if actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_SUSPENDED {
			return fmt.Errorf("native Actor boot changed outside the recorded intent")
		}
		ready, err := d.prepareWorker(ctx, cm, record)
		if err != nil || !ready {
			return err
		}
		record.BootRequested = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		_, err = d.control.ResumeActor(ctx, &pb.ResumeActorRequest{Actor: actorRef(record), Boot: record.InheritedCheckpoint == nil})
		return err
	}
	if actor.GetStatus().GetState() == pb.ActorState_ACTOR_STATE_CRASHED || record.ChallengeSHA256 != "" && actor.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_RUNNING {
		return fmt.Errorf("native boot stopped outside the lifecycle; admission is closed")
	}
	return nil
}

func (d *Lifecycle) Observe(ctx context.Context, key sdk.AllocationKey) (sdk.AllocationObservation, error) {
	cm, record, err := d.read(ctx, key)
	if err != nil {
		return sdk.AllocationObservation{}, err
	}
	if err := d.requireJournalReadOnly(ctx, cm, record); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if record.Operation == "ensure" {
		return d.observeReady(ctx, record)
	}
	return record.Observation, nil
}

func (d *Lifecycle) StopInstance(ctx context.Context, key sdk.AllocationKey, identity sdk.InstanceIdentity) (sdk.AllocationObservation, error) {
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
		if record.Observation.State == sdk.AllocationDeleted || record.Observation.State == sdk.AllocationStopped {
			return nil
		}
		if err := d.requireJournal(ctx, cm, record); err != nil {
			return err
		}
		if record.Operation != "stop" {
			record.Operation = "stop"
			record.Observation = pendingObservation(record)
			if err := d.save(ctx, cm, record); err != nil {
				return err
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

func (d *Lifecycle) requireJournalReadOnly(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, clientKey(record.Request.Key), workspace); err != nil {
		return err
	}
	if workspace.UID != record.Request.Key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != record.Request.Key.ProviderUID {
		return sdk.ErrStaleIdentity
	}
	if workspace.Annotations[journalRequiredAnnotation] != cm.Name+"/"+string(cm.UID) {
		return fmt.Errorf("native journal marker is missing or changed")
	}
	return nil
}

func samePolicy(a, b api.ExecutionWorkspaceDeletionPolicy) bool { return reflect.DeepEqual(a, b) }
