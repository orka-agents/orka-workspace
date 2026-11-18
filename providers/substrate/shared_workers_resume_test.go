package substrate

import (
	"maps"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
)

// A suspended Actor's next sequence resumes from its own Tag onto the shared
// worker its neighbour keeps running: a new Actor on the same fenced worker,
// with the checkpoint's provenance carried forward.
func TestSharedResumeOfANewSequenceReusesTheRunningWorker(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	_, recordB := sharedReady(t, c, native, second)
	if recordB.SharedWorker.Pool != recordA.SharedWorker.Pool {
		t.Fatal("the neighbour did not share the first worker")
	}
	stopped, suspended := sharedRetired(t, c, native, first, a.Identity, true)
	if stopped.RetainedData == nil || suspended.Checkpoint == nil {
		t.Fatal("the suspension retained no data")
	}
	if !podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("the interim suspension removed the neighbour's worker")
	}

	next := *first.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &a.Identity
	next.RetainedData = stopped.RetainedData
	next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	resumedObs, resumed := sharedReady(t, c, native, next)

	if resumed.SharedWorker.Pool != recordA.SharedWorker.Pool || resumed.SharedWorker.UID != recordA.SharedWorker.UID {
		t.Fatalf("the resume left the neighbour's running shared pool: %#v", resumed.SharedWorker)
	}
	if resumed.Worker == nil || resumed.Worker.PodUID != recordA.Worker.PodUID || resumed.Worker.UID != recordA.Worker.UID {
		t.Fatal("the resume did not land on the same fenced worker")
	}
	if len(sharedPools(t, c)) != 1 {
		t.Fatal("the resume created another shared pool")
	}
	if resumedObs.Identity.InstanceID == a.Identity.InstanceID || resumed.Actor.UID == "" || resumed.Actor.UID == recordA.Actor.UID {
		t.Fatal("the resume reused the suspended Actor's identity")
	}
	inherited := resumed.InheritedCheckpoint
	if inherited == nil || inherited.Tag != suspended.Checkpoint.Tag || inherited.SnapshotURI != suspended.Checkpoint.SnapshotURI || inherited.SourceUID != suspended.Checkpoint.SourceUID {
		t.Fatalf("the resume did not restore the suspended sequence's own Tag: %#v", inherited)
	}
	if resumed.CreateTemplate.Name == resumed.Template.Name {
		t.Fatal("the restored Actor did not move to the rotated bootstrap template")
	}
	if !maps.Equal(resumed.SharedWorker.OperatorLabels, recordA.SharedWorker.OperatorLabels) {
		t.Fatal("the resume lost the operator pool provenance")
	}
	_, ledger := poolLedger(t, c, resumed.SharedWorker.Pool)
	held := false
	for _, entry := range ledger.Entries {
		held = held || entry.ActorUID == resumed.Actor.UID
	}
	if len(ledger.Entries) != 2 || !held {
		t.Fatalf("the ledger does not hold exactly the neighbour and the resumed sequence: %#v", ledger.Entries)
	}
}
