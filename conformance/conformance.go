// Package conformance checks the required lifecycle contract through public
// provider operations. Providers supply an admitted fixture and a factory that
// reconnects to the same durable store. No testing.T is needed by provider CLIs.
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Check consumes one fresh admitted fixture through deletion. Each factory call
// represents a provider restart: it must reconnect to the same durable backend.
// Callers must set a deadline appropriate for that backend. The suite does not
// advertise optional capabilities or certify a provider's startup attestation.
func Check(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	if err := request.Validate(); err != nil {
		return fmt.Errorf("fixture: %w", err)
	}
	driver := factory()
	if driver == nil {
		return fmt.Errorf("factory returned no provider")
	}
	if _, err := driver.Observe(ctx, request.Key); !errors.Is(err, workspaceprovider.ErrNotFound) {
		return fmt.Errorf("fresh fixture must not exist: %v", err)
	}
	// Discard a successful create response, as if it were lost in transit, and
	// retry with a newly constructed driver. This must recover the same instance.
	created, err := driver.EnsureAllocation(ctx, request)
	if err != nil {
		return fmt.Errorf("ensure allocation: %w", err)
	}
	if created.Identity.AllocationID == "" || created.Identity.InstanceID == "" {
		return fmt.Errorf("ensure returned no allocation/instance identity")
	}
	driver = factory()
	observed, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		return driver.EnsureAllocation(ctx, request)
	}, workspaceprovider.AllocationReady)
	if err != nil {
		return fmt.Errorf("retry ensure after lost response: %w", err)
	}
	if observed.Identity != created.Identity {
		return fmt.Errorf("ensure created a different instance after a lost response")
	}
	if err := workspaceprovider.ValidateStartup(request, observed); err != nil {
		return fmt.Errorf("startup: %w", err)
	}
	if err := sameReady(ctx, driver, request, observed.Identity); err != nil {
		return err
	}

	changed := copyRequest(request)
	changed.Args = append(append([]string(nil), request.Args...), "different-request")
	if changed.Runtime != nil {
		for i := range changed.Runtime.Template.Spec.Containers {
			if changed.Runtime.Template.Spec.Containers[i].Name == changed.Runtime.ContainerName {
				changed.Runtime.Template.Spec.Containers[i].Args = changed.Args
			}
		}
	}
	changed.Revision, err = workspaceprovider.WorkloadRevision(changed)
	if err != nil {
		return err
	}
	if _, err := driver.EnsureAllocation(ctx, changed); !errors.Is(err, workspaceprovider.ErrRequestConflict) {
		return fmt.Errorf("changed immutable request was not rejected: %v", err)
	}
	if err := sameReady(ctx, driver, request, observed.Identity); err != nil {
		return err
	}

	foreign := request.Key
	foreign.WorkspaceUID += "-replacement"
	if _, err := driver.Observe(ctx, foreign); err == nil {
		return fmt.Errorf("foreign workspace UID observed an allocation")
	}
	if _, err := driver.StopInstance(ctx, foreign, observed.Identity); err == nil {
		return fmt.Errorf("foreign workspace UID stopped an allocation")
	}
	foreign = request.Key
	foreign.ProviderUID += "-replacement"
	if _, err := driver.StopInstance(ctx, foreign, observed.Identity); err == nil {
		return fmt.Errorf("foreign provider UID stopped an allocation")
	}
	policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{
		ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete,
		PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete,
		Checkpoints:       workspacev1alpha1.WorkspaceDeletionActionDelete,
	}
	for _, stale := range []workspaceprovider.InstanceIdentity{
		{AllocationID: observed.Identity.AllocationID, InstanceID: "stale", RequestRevision: observed.Identity.RequestRevision},
		{AllocationID: "stale", InstanceID: observed.Identity.InstanceID, RequestRevision: observed.Identity.RequestRevision},
		{AllocationID: observed.Identity.AllocationID, InstanceID: observed.Identity.InstanceID, RequestRevision: "stale"},
	} {
		if _, err := driver.StopInstance(ctx, request.Key, stale); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
			return fmt.Errorf("stale stop was not rejected: %v", err)
		}
		if _, err := driver.DeleteAllocation(ctx, request.Key, stale, policy); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
			return fmt.Errorf("stale deletion was not rejected: %v", err)
		}
	}
	if _, err := driver.DeleteAllocation(ctx, request.Key, observed.Identity, policy); !errors.Is(err, workspaceprovider.ErrInstanceRunning) {
		return fmt.Errorf("deletion before observed stop was not rejected: %v", err)
	}
	if err := sameReady(ctx, driver, request, observed.Identity); err != nil {
		return err
	}

	if _, err := driver.StopInstance(ctx, request.Key, observed.Identity); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	driver = factory()
	stopped, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		return driver.StopInstance(ctx, request.Key, observed.Identity)
	}, workspaceprovider.AllocationStopped)
	if err != nil {
		return fmt.Errorf("retry stop after lost response: %w", err)
	}
	if stopped.Sequence != request.Sequence || stopped.Identity != observed.Identity || stopped.Key != request.Key || stopped.Startup != nil {
		return fmt.Errorf("stop did not preserve fence and withdraw startup evidence")
	}
	if err := noResurrection(ctx, driver, request, observed.Identity, workspaceprovider.AllocationStopped); err != nil {
		return err
	}

	if _, err := driver.DeleteAllocation(ctx, request.Key, observed.Identity, policy); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	driver = factory()
	deleted, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		return driver.DeleteAllocation(ctx, request.Key, observed.Identity, policy)
	}, workspaceprovider.AllocationDeleted)
	if err != nil {
		return fmt.Errorf("retry delete after lost response: %w", err)
	}
	if deleted.Sequence != request.Sequence || deleted.Identity != observed.Identity || deleted.Key != request.Key || deleted.Startup != nil {
		return fmt.Errorf("deletion did not preserve fence and withdraw startup evidence")
	}
	if err := workspaceprovider.ValidateDeletedDisposition(deleted.Disposition, policy); err != nil {
		return fmt.Errorf("deleted disposition: %w", err)
	}
	if err := noResurrection(ctx, driver, request, observed.Identity, workspaceprovider.AllocationDeleted); err != nil {
		return err
	}
	next := nextRequest(request, observed.Identity, nil)
	if _, err := driver.EnsureAllocation(ctx, next); err == nil {
		return fmt.Errorf("deleted allocation accepted another sequence")
	}
	return nil
}

func sameReady(ctx context.Context, driver workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest, identity workspaceprovider.InstanceIdentity) error {
	observation, err := driver.Observe(ctx, request.Key)
	if err != nil {
		return fmt.Errorf("observe after restart or rejected operation: %w", err)
	}
	if observation.Identity != identity {
		return fmt.Errorf("read-only observation changed instance identity")
	}
	return workspaceprovider.ValidateStartup(request, observation)
}

func noResurrection(ctx context.Context, driver workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest, identity workspaceprovider.InstanceIdentity, state workspaceprovider.AllocationState) error {
	_, err := driver.EnsureAllocation(ctx, request)
	if err != nil && !errors.Is(err, workspaceprovider.ErrRequestConflict) {
		return fmt.Errorf("retry retired request: %w", err)
	}
	observed, err := driver.Observe(ctx, request.Key)
	if err != nil {
		return fmt.Errorf("retirement tombstone disappeared: %w", err)
	}
	if observed.Sequence != request.Sequence || observed.Identity != identity || observed.Key != request.Key || observed.State != state || observed.Startup != nil {
		return fmt.Errorf("retired request was resurrected or lost its identity")
	}
	return nil
}

func until(ctx context.Context, operation func() (workspaceprovider.AllocationObservation, error), state workspaceprovider.AllocationState) (workspaceprovider.AllocationObservation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
		observed, err := operation()
		if err != nil || observed.State == state {
			return observed, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return observed, ctx.Err()
		case <-timer.C:
		}
	}
}

// AdmitRequest updates the isolated test fixture's core-owned admitted request.
// Production providers must never grant this admission themselves.
type AdmitRequest func(context.Context, workspaceprovider.WorkloadRequest) error

func copyRequest(request workspaceprovider.WorkloadRequest) workspaceprovider.WorkloadRequest {
	data, _ := json.Marshal(request)
	var copied workspaceprovider.WorkloadRequest
	_ = json.Unmarshal(data, &copied)
	return copied
}
func nextRequest(request workspaceprovider.WorkloadRequest, previous workspaceprovider.InstanceIdentity, retained *workspaceprovider.RetainedDataReference) workspaceprovider.WorkloadRequest {
	next := copyRequest(request)
	next.Sequence++
	next.PreviousInstance = &previous
	next.RetainedData = retained
	next.Revision, _ = workspaceprovider.WorkloadRevision(next)
	return next
}

// CheckReplacement consumes a fresh fixture through two sequences. Supply
// admit when the fixture stores each requested workload on an admitted object.
func CheckReplacement(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest, admit AdmitRequest) error {
	return checkContinuation(ctx, factory, request, admit, false)
}

// CheckSuspension separately tests the optional data-suspension interface.
// It must not be used to certify persistence for a provider's simulation mode.
func CheckSuspension(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest, admit AdmitRequest) error {
	return checkContinuation(ctx, factory, request, admit, true)
}

func checkContinuation(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest, admit AdmitRequest, suspend bool) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	first, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		return factory().EnsureAllocation(ctx, request)
	}, workspaceprovider.AllocationReady)
	if err != nil {
		return err
	}
	if err := workspaceprovider.ValidateStartup(request, first); err != nil {
		return err
	}
	next := nextRequest(request, first.Identity, nil)
	if _, err := factory().EnsureAllocation(ctx, next); err == nil {
		return fmt.Errorf("running predecessor accepted another sequence")
	}
	var retired workspaceprovider.AllocationObservation
	if suspend {
		driver, ok := factory().(workspaceprovider.SuspensionController)
		if !ok {
			return fmt.Errorf("provider has no suspension capability")
		}
		// Start the suspension inside its window, then keep retrying after the
		// window closed: a suspension started in time must still finish.
		window := metav1.NewTime(time.Now().Add(time.Hour))
		retired, err = until(ctx, func() (workspaceprovider.AllocationObservation, error) {
			observed, err := driver.SuspendInstance(ctx, request.Key, first.Identity, &window)
			window = expiredSuspendWindow()
			return observed, err
		}, workspaceprovider.AllocationStopped)
	} else {
		retired, err = until(ctx, func() (workspaceprovider.AllocationObservation, error) {
			return factory().StopInstance(ctx, request.Key, first.Identity)
		}, workspaceprovider.AllocationStopped)
	}
	if err != nil {
		return err
	}
	if retired.Key != request.Key || retired.Identity != first.Identity || retired.Sequence != request.Sequence || retired.Startup != nil {
		return fmt.Errorf("retirement changed the predecessor fence")
	}
	if suspend {
		if retired.RetainedData == nil || !retired.RetainedData.Valid() || retired.RetainedData.SourceInstance != first.Identity {
			return fmt.Errorf("suspension lacks verified retained lineage")
		}
		expired := expiredSuspendWindow()
		again, err := factory().(workspaceprovider.SuspensionController).SuspendInstance(ctx, request.Key, first.Identity, &expired)
		if err != nil || again.State != workspaceprovider.AllocationStopped || again.RetainedData == nil || *again.RetainedData != *retired.RetainedData {
			return fmt.Errorf("a suspension completed in time was not stable after its window closed: %v", err)
		}
	}
	next = nextRequest(request, first.Identity, retired.RetainedData)
	if admit != nil {
		if err := admit(ctx, next); err != nil {
			return err
		}
	}
	second, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) { return factory().EnsureAllocation(ctx, next) }, workspaceprovider.AllocationReady)
	if err != nil {
		return err
	}
	if err := workspaceprovider.ValidateStartup(next, second); err != nil {
		return err
	}
	if second.Identity.InstanceID == first.Identity.InstanceID {
		return fmt.Errorf("new sequence reused retired instance identity")
	}
	if _, err := factory().StopInstance(ctx, request.Key, first.Identity); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
		return fmt.Errorf("old instance could stop its successor: %v", err)
	}
	old, err := factory().EnsureAllocation(ctx, request)
	if err != nil || old.Sequence != request.Sequence || old.Identity != first.Identity || old.State != workspaceprovider.AllocationStopped || old.Startup != nil {
		return fmt.Errorf("old request lost its retirement tombstone: %v", err)
	}
	if err := sameReady(ctx, factory(), next, second.Identity); err != nil {
		return err
	}
	retired, err = until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		if suspend {
			driver, ok := factory().(workspaceprovider.SuspensionController)
			if !ok {
				return workspaceprovider.AllocationObservation{}, fmt.Errorf("provider has no suspension capability")
			}
			return driver.SuspendInstance(ctx, next.Key, second.Identity, nil)
		}
		return factory().StopInstance(ctx, next.Key, second.Identity)
	}, workspaceprovider.AllocationStopped)
	if err != nil {
		return err
	}
	if retired.Key != next.Key || retired.Identity != second.Identity || retired.Sequence != next.Sequence || retired.Startup != nil {
		return fmt.Errorf("successor termination changed its exact fence")
	}
	if suspend {
		if retired.RetainedData == nil || !retired.RetainedData.Valid() || retired.RetainedData.SourceInstance != second.Identity {
			return fmt.Errorf("successor suspension lacks verified retained lineage")
		}
		// Run the admitted adversarial request last. Persisting it consumes the
		// next immutable sequence, so the successful resume fixture cannot be
		// repaired by admitting a different request at that same sequence.
		forged := *retired.RetainedData
		forged.ID += "-foreign"
		forgedRequest := nextRequest(next, second.Identity, &forged)
		if admit != nil {
			if err := admit(ctx, forgedRequest); err != nil {
				return err
			}
		}
		if _, err := factory().EnsureAllocation(ctx, forgedRequest); err == nil {
			return fmt.Errorf("forged retained lineage was accepted")
		}
		observed, err := factory().Observe(ctx, next.Key)
		if err != nil || observed.Key != next.Key || observed.Sequence != next.Sequence || observed.Identity != second.Identity || observed.State != workspaceprovider.AllocationStopped ||
			observed.Startup != nil || observed.RetainedData == nil || *observed.RetainedData != *retired.RetainedData {
			return fmt.Errorf("rejected retained lineage changed its predecessor: %v", err)
		}
	}
	policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
	deleted, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		return factory().DeleteAllocation(ctx, next.Key, second.Identity, policy)
	}, workspaceprovider.AllocationDeleted)
	if err != nil {
		return err
	}
	if deleted.Key != next.Key || deleted.Sequence != next.Sequence || deleted.Identity != second.Identity || deleted.Startup != nil {
		return fmt.Errorf("continuation cleanup changed its exact predecessor fence")
	}
	return workspaceprovider.ValidateDeletedDisposition(deleted.Disposition, policy)
}

// expiredSuspendWindow is well past any provider's clock skew allowance.
func expiredSuspendWindow() metav1.Time {
	return metav1.NewTime(time.Now().Add(-time.Hour))
}

// CheckSuspendDeadline consumes a fresh fixture whose provider observes core's
// Suspend authorization only after its NotAfter. The provider must refuse to
// start the suspension with ErrSuspendDeadlineExceeded, leave the exact
// instance untouched, and keep refusing after the fallback Stop. The instance
// must end stopped with no retained data, so it is never reported suspended.
func CheckSuspendDeadline(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	suspendable := func() (workspaceprovider.SuspendableLifecycle, error) {
		driver, ok := factory().(workspaceprovider.SuspendableLifecycle)
		if !ok {
			return nil, fmt.Errorf("provider has no suspension capability")
		}
		return driver, nil
	}
	ready, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		return factory().EnsureAllocation(ctx, request)
	}, workspaceprovider.AllocationReady)
	if err != nil {
		return err
	}
	if err := workspaceprovider.ValidateStartup(request, ready); err != nil {
		return err
	}
	driver, err := suspendable()
	if err != nil {
		return err
	}
	expired := expiredSuspendWindow()
	if _, err := driver.SuspendInstance(ctx, request.Key, ready.Identity, &expired); !errors.Is(err, workspaceprovider.ErrSuspendDeadlineExceeded) {
		return fmt.Errorf("suspension started after its authorization expired: %v", err)
	}
	// Refusal must not have begun any retirement: the instance still serves.
	if err := sameReady(ctx, factory(), request, ready.Identity); err != nil {
		return fmt.Errorf("refused late suspension changed the instance: %w", err)
	}
	authorization := &workspaceprovider.WorkloadRetirement{Sequence: request.Sequence, Identity: ready.Identity, Action: workspacev1alpha1.WorkloadRetirementSuspend, NotAfter: &expired}
	var exceeded bool
	stopped, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		driver, err := suspendable()
		if err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
		observed, deadline, err := workspaceprovider.SuspendOrStop(ctx, driver, request.Key, ready.Identity, authorization)
		exceeded = deadline
		return observed, err
	}, workspaceprovider.AllocationStopped)
	if err != nil {
		return fmt.Errorf("fallback stop after an expired suspend authorization: %w", err)
	}
	if !exceeded {
		return fmt.Errorf("an expired suspend authorization was not reported as exceeded")
	}
	if stopped.Key != request.Key || stopped.Sequence != request.Sequence || stopped.Identity != ready.Identity || stopped.Startup != nil || stopped.RetainedData != nil {
		return fmt.Errorf("fallback stop changed the exact fence or published retained data")
	}
	// A restarted provider still refuses and reports the same stopped instance.
	driver, err = suspendable()
	if err != nil {
		return err
	}
	if _, err := driver.SuspendInstance(ctx, request.Key, ready.Identity, &expired); !errors.Is(err, workspaceprovider.ErrSuspendDeadlineExceeded) {
		return fmt.Errorf("expired suspend authorization was not refused after the fallback stop: %v", err)
	}
	observed, err := factory().Observe(ctx, request.Key)
	if err != nil || observed.Key != request.Key || observed.Sequence != request.Sequence || observed.Identity != ready.Identity ||
		observed.State != workspaceprovider.AllocationStopped || observed.RetainedData != nil || observed.Startup != nil {
		return fmt.Errorf("stopped instance after an expired suspend authorization was reported suspended or changed: %v", err)
	}
	policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
	deleted, err := until(ctx, func() (workspaceprovider.AllocationObservation, error) {
		return factory().DeleteAllocation(ctx, request.Key, ready.Identity, policy)
	}, workspaceprovider.AllocationDeleted)
	if err != nil {
		return err
	}
	return workspaceprovider.ValidateDeletedDisposition(deleted.Disposition, policy)
}
