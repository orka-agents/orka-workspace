package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

func (d *Lifecycle) EnsureAllocation(ctx context.Context, request workspaceprovider.WorkloadRequest) (workspaceprovider.AllocationObservation, error) {
	if err := validateRequest(request); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	var observed workspaceprovider.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, request.Key)
		if err != nil && !errors.Is(err, workspaceprovider.ErrNotFound) {
			return err
		}
		if record != nil && request.Sequence < record.Request.Sequence {
			_, retired, err := d.readAt(ctx, request.Key, historyKey(request.Key, request.Sequence))
			if err != nil {
				return err
			}
			if retired.Request.Revision != request.Revision {
				return workspaceprovider.ErrRequestConflict
			}
			observed = retired.Observation
			return nil
		}
		if record != nil && request.Sequence == record.Request.Sequence {
			if request.Revision != record.Request.Revision {
				return workspaceprovider.ErrRequestConflict
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
			if err := d.proveNoAllocation(ctx, request.Key); err != nil {
				return err
			}
			if err := workspaceprovider.ValidateWorkloadTransition(nil, request, nil, false); err != nil {
				return err
			}
			record, err = newRecord(request)
			if err != nil {
				return err
			}
			if err := d.resolveProfile(ctx, record); err != nil {
				return err
			}
			if slices.Contains(request.Runtime.RequiredFeatures, workspacev1alpha1.WorkspaceFeatureSuspend) && record.Volume == nil {
				return fmt.Errorf("suspend feature requires a data-only SandboxWorkspaceProfile")
			}
			data, err := encode(record)
			if err != nil {
				return err
			}
			key := journalKey(request.Key)
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: labels(record), OwnerReferences: []metav1.OwnerReference{workspaceOwner(request.Key)}}, Data: map[string]string{journalDataKey: data}}
			if err := d.client.Create(ctx, cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, err)
				}
				return err
			}
		} else if request.Sequence != record.Request.Sequence {
			if record.Volume != nil && (record.Operation != "suspend" || record.Storage == nil) {
				return fmt.Errorf("persistent Sandbox replacement requires exact retained data from suspension")
			}
			if err := workspaceprovider.ValidateWorkloadTransition(&record.Request, request, &record.Observation, record.Volume != nil); err != nil {
				return err
			}
			if request.Runtime.Template.Namespace != record.Request.Runtime.Template.Namespace {
				return workspaceprovider.ErrRequestConflict
			}
			next := *record
			next.Request = request
			if record.Volume != nil {
				if err := validateDurableRuntime(&next); err != nil {
					return err
				}
			}
			if record.Sandbox.UID == "" && record.Volume == nil {
				if err := d.requireJournal(ctx, cm, record); err != nil {
					return err
				}
				complete, err := d.removeUnmaterializedAllocation(ctx, record)
				if err != nil {
					return err
				}
				if !complete {
					observed = record.Observation
					return nil
				}
				if err := d.archive(ctx, cm, record); err != nil {
					return err
				}
				fresh, err := newRecord(request)
				if err != nil {
					return err
				}
				fresh.ResumePrepared = true
				cm.Labels = labels(fresh)
				if err := d.save(ctx, cm, fresh); err != nil {
					return err
				}
				record = fresh
			} else {
				sb, err := d.sandbox(ctx, record)
				if err != nil {
					return err
				}
				if _, err := d.verifyStorage(ctx, record, sb); err != nil {
					return err
				}
				if err := d.confirmSuspended(ctx, record, sb); err != nil {
					return err
				}
				if err := d.archive(ctx, cm, record); err != nil {
					return err
				}
				record.Request = request
				record.Operation = "ensure"
				record.PreviousPod = record.Pod
				record.Pod = nil
				record.ResumePrepared = false
				record.DeletionPolicy = nil
				record.Observation = workspaceprovider.AllocationObservation{Key: request.Key, Sequence: request.Sequence, Identity: workspaceprovider.InstanceIdentity{AllocationID: record.Observation.Identity.AllocationID, InstanceID: record.Observation.Identity.AllocationID + "-" + strconv.FormatInt(request.Sequence, 10), RequestRevision: request.Revision}, State: workspaceprovider.AllocationPending}
				if err := d.save(ctx, cm, record); err != nil {
					return err
				}
			}
		}
		if err := d.requireJournal(ctx, cm, record); err != nil {
			return err
		}
		if request.Sequence > 1 && record.Sandbox.UID != "" {
			if err := d.resume(ctx, cm, record); err != nil {
				return err
			}
		}
		if err := d.ensureNative(ctx, cm, record); err != nil {
			return err
		}
		sb, err := d.sandbox(ctx, record)
		if apierrors.IsNotFound(err) && record.Sandbox.UID == "" {
			observed = pendingObservation(record)
			return nil
		}
		if err != nil {
			return err
		}
		if err := attestBlueprint(record, sb); err != nil {
			return err
		}
		if record.Sandbox.UID == "" {
			record.Sandbox = objectReference{Name: sb.Name, UID: sb.UID}
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		pod, err := d.pod(ctx, record, sb)
		if apierrors.IsNotFound(err) {
			observed = pendingObservation(record)
			return nil
		}
		if err != nil {
			return err
		}
		if err := d.attestPod(ctx, record, sb, pod); err != nil {
			return err
		}
		if record.Pod == nil {
			record.Pod = &workspaceprovider.PodReference{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}
			if record.Volume != nil {
				record.Storage, err = d.verifyStorage(ctx, record, sb)
				if err != nil {
					return err
				}
			}
			if err := d.save(ctx, cm, record); err != nil {
				return err
			}
		}
		observed, err = d.observeReady(ctx, record)
		if err != nil {
			return err
		}
		record.Observation = observed
		return d.save(ctx, cm, record)
	})
	return observed, err
}

func pendingObservation(record *journalRecord) workspaceprovider.AllocationObservation {
	observed := record.Observation
	observed.State = workspaceprovider.AllocationPending
	observed.Startup = nil
	return observed
}

func (d *Lifecycle) attestPod(ctx context.Context, record *journalRecord, sb *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
	if err := attestBlueprint(record, sb); err != nil {
		return err
	}
	if pod.UID == "" || pod.Name != sb.Name || !podLabelsMatch(sb, pod) || !podAnnotationsMatch(sb, pod) {
		return fmt.Errorf("Pod metadata differs from the exact Sandbox")
	}
	claimName := ""
	if record.Volume != nil {
		claimName = durableVolumeName + "-" + sb.Name
	}
	if !podSpecsMatch(sb.Spec.PodTemplate.Spec, pod.Spec, claimName) {
		return fmt.Errorf("Pod specification differs from the admitted Sandbox")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.RestartCount > 0 {
			return fmt.Errorf("runtime process restarted within the same Pod: %w", workspaceprovider.ErrStaleIdentity)
		}
	}
	if _, err := d.verifyStorage(ctx, record, sb); err != nil {
		return err
	}
	return nil
}

func (d *Lifecycle) observeReady(ctx context.Context, record *journalRecord) (workspaceprovider.AllocationObservation, error) {
	sb, err := d.sandbox(ctx, record)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return pendingObservation(record), nil
		}
		return workspaceprovider.AllocationObservation{}, err
	}
	if sb.DeletionTimestamp != nil || sb.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended {
		return pendingObservation(record), nil
	}
	pod, err := d.pod(ctx, record, sb)
	if apierrors.IsNotFound(err) {
		return pendingObservation(record), nil
	}
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if err := d.attestPod(ctx, record, sb, pod); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if record.Pod == nil || pod.DeletionTimestamp != nil || net.ParseIP(pod.Status.PodIP) == nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return pendingObservation(record), nil
	}
	observed := record.Observation
	observed.State = workspaceprovider.AllocationReady
	observed.Startup = &workspaceprovider.StartupEvidence{ContractVersion: workspaceprovider.LifecycleContractV1, Identity: observed.Identity, Endpoint: "http://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(record.Request.Runtime.BootstrapPort))), Pod: record.Pod}
	if record.Storage != nil {
		observed.Startup.PersistentVolumes = []workspacev1alpha1.PersistentVolumeEvidence{{VolumeName: durableVolumeName, Claim: workspacev1alpha1.PodReference{Namespace: record.Namespace, Name: record.Storage.ClaimName, UID: record.Storage.ClaimUID}, Volume: workspacev1alpha1.ObjectIdentityReference{Name: record.Storage.VolumeName, UID: record.Storage.VolumeUID}}}
	}
	return observed, nil
}

func (d *Lifecycle) Observe(ctx context.Context, key workspaceprovider.AllocationKey) (workspaceprovider.AllocationObservation, error) {
	_, record, err := d.read(ctx, key)
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if record.Operation == "ensure" {
		return d.observeReady(ctx, record)
	}
	return record.Observation, nil
}

func (d *Lifecycle) confirmSuspended(ctx context.Context, record *journalRecord, sb *sandboxv1beta1.Sandbox) error {
	condition := apimeta.FindStatusCondition(sb.Status.Conditions, string(sandboxv1beta1.SandboxConditionSuspended))
	if sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended || condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != sb.Generation || condition.Reason != sandboxv1beta1.SandboxReasonSuspendedPodTerminated {
		return workspaceprovider.ErrInstanceRunning
	}
	if _, err := d.pod(ctx, record, sb); err == nil {
		return workspaceprovider.ErrInstanceRunning
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	// Checking the exact retired Pod independently protects against changed owner
	// references hiding a still-running process from the Sandbox owner scan.
	if record.Pod != nil {
		pod := &corev1.Pod{}
		err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Pod.Namespace, Name: record.Pod.Name}, pod)
		if err == nil {
			return workspaceprovider.ErrInstanceRunning
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (d *Lifecycle) resume(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	sb, err := d.sandbox(ctx, record)
	if err != nil {
		return err
	}
	if _, err := d.verifyStorage(ctx, record, sb); err != nil {
		return err
	}
	if record.ResumePrepared && sb.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeRunning {
		return attestBlueprint(record, sb)
	}
	if err := d.confirmSuspended(ctx, record, sb); err != nil {
		return err
	}
	_, previous, err := d.readAt(ctx, record.Request.Key, historyKey(record.Request.Key, record.Request.Sequence-1))
	if err != nil {
		return err
	}
	if err := attestBlueprint(previous, sb); err != nil {
		return err
	}
	if !record.ResumePrepared {
		record.ResumePrepared = true
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
	}
	template := &extv1beta1.SandboxTemplate{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: record.Template.Name}, template); err != nil {
		return err
	}
	if !nativeOwned(template, record, record.Template) {
		return workspaceprovider.ErrStaleIdentity
	}
	desired := desiredTemplate(record)
	if !apiequality.Semantic.DeepEqual(template.Spec, desired.Spec) {
		if !apiequality.Semantic.DeepEqual(template.Spec, desiredTemplate(previous).Spec) {
			return fmt.Errorf("suspended template changed outside the journal")
		}
		template.Spec = desired.Spec
		if err := d.client.Update(ctx, template); err != nil {
			return err
		}
	}
	// Re-read after template update. The blueprint can change only while this
	// exact Sandbox still has no Pod, and its resourceVersion fences the update.
	sb, err = d.sandbox(ctx, record)
	if err != nil {
		return err
	}
	if err := d.confirmSuspended(ctx, record, sb); err != nil {
		return err
	}
	if err := attestBlueprint(previous, sb); err != nil {
		return err
	}
	// Read intent after reading the Sandbox version. Retirement always changes
	// its native operation marker, so either this check or the native CAS loses
	// when a concurrent stop has won the journal.
	if err := d.verifyEnsureIntent(ctx, record); err != nil {
		return err
	}
	sb.Spec.PodTemplate = desired.Spec.PodTemplate
	sb.Spec.PodTemplate.ObjectMeta.Labels = maps.Clone(desired.Spec.PodTemplate.ObjectMeta.Labels)
	if sb.Spec.PodTemplate.ObjectMeta.Labels == nil {
		sb.Spec.PodTemplate.ObjectMeta.Labels = map[string]string{}
	}
	sb.Spec.PodTemplate.ObjectMeta.Labels[extv1beta1.SandboxIDLabel] = string(record.Claim.UID)
	sb.Spec.PodTemplate.ObjectMeta.Labels[sandboxv1beta1.SandboxTemplateRefHashLabel] = sandboxcontrollers.NameHash(record.Template.Name)
	sb.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
	if sb.Annotations == nil {
		sb.Annotations = map[string]string{}
	}
	sb.Annotations[nativeOperationAnnotation] = strconv.FormatInt(record.Request.Sequence, 10) + ":ensure"
	return d.client.Update(ctx, sb)
}

func retainedData(record *journalRecord) *workspaceprovider.RetainedDataReference {
	data, _ := json.Marshal(struct {
		Key      workspaceprovider.AllocationKey
		Identity workspaceprovider.InstanceIdentity
		Claim    objectReference
		Sandbox  objectReference
		Storage  *storageIdentity
	}{record.Request.Key, record.Observation.Identity, record.Claim, record.Sandbox, record.Storage})
	sum := sha256.Sum256(data)
	return &workspaceprovider.RetainedDataReference{ID: record.Observation.Identity.AllocationID, SourceInstance: record.Observation.Identity, ProofSHA256: "sha256:" + hex.EncodeToString(sum[:])}
}
