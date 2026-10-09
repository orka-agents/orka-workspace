package substrate

import (
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMissingRetentionLedgerRejectsUnreadableJournalBeforeCapture(t *testing.T) {
	for _, corruption := range []string{"missing record", "invalid JSON", "oversized record", "unknown version", "invalid identity", "record at other name"} {
		t.Run(corruption, func(t *testing.T) {
			c, native, request := fixture(t, true)
			request = cappedRequest(t, c, request, 1)
			first := ready(t, c, native, request)
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: "substrate-workspace-unrecoverable", Labels: map[string]string{providerLabel: string(request.Key.ProviderUID)}}, Data: map[string]string{journalDataKey: "{"}}
			switch corruption {
			case "missing record":
				object.Name = "substrate-history-unrecoverable"
				object.Data = nil
			case "oversized record":
				object.Data[journalDataKey] = strings.Repeat(" ", MaxJournalBytes+1)
			case "unknown version", "invalid identity":
				copy := *record
				copy.Operation = "suspend"
				if corruption == "unknown version" {
					copy.Version = "substrate.workspace.journal.future"
				}
				raw, err := json.Marshal(&copy)
				if err != nil {
					t.Fatal(err)
				}
				object.Data[journalDataKey] = string(raw)
			case "record at other name":
				object.Name = "unrecoverable-provider-record"
			}
			if err := c.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil || !strings.Contains(err.Error(), "cannot account for native journal") {
				t.Fatalf("unreadable journal allowed capacity reset: %v", err)
			}
			if native.suspends != 0 || native.boots != 1 || native.deletes != 0 || len(native.tags) != 0 {
				t.Fatal("failed occupancy accounting changed the native runtime")
			}
			if err := c.Get(t.Context(), retentionKey(record), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Fatalf("failed occupancy accounting created a new ledger: %v", err)
			}
			_, after, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if after.Operation != "ensure" || after.Observation.State != sdk.AllocationReady || after.Pending != nil || after.Observation.Identity != first.Identity {
				t.Fatal("failed occupancy accounting changed the running journal")
			}
		})
	}
}

func TestMissingRetentionLedgerIgnoresProviderNonJournalConfigMaps(t *testing.T) {
	c, native, request := fixture(t, true)
	request = cappedRequest(t, c, request, 1)
	first := ready(t, c, native, request)
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: "provider-bookkeeping", Labels: map[string]string{providerLabel: string(request.Key.ProviderUID)}}, Data: map[string]string{"journalName": "bookkeeping-reference"}}
	if err := c.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	stopped := retired(t, c, native, request, first.Identity, true)
	if stopped.RetainedData == nil || native.suspends != 1 {
		t.Fatal("non-journal provider metadata prevented an admitted suspension")
	}
}

func TestResetRetentionLedgerCannotAdmitPastRetainedOccupants(t *testing.T) {
	for _, reset := range []string{"recreated", "edited"} {
		t.Run(reset, func(t *testing.T) {
			c, native, request := fixture(t, true)
			request = cappedRequest(t, c, request, 1)
			first := ready(t, c, native, request)
			retired(t, c, native, request, first.Identity, true)
			second := restoreRequest(t, c, request, nil)
			secondReady := ready(t, c, native, second)
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			ledger := &corev1.ConfigMap{}
			if err := c.Get(t.Context(), retentionKey(record), ledger); err != nil {
				t.Fatal(err)
			}
			switch reset {
			case "recreated":
				if err := c.Delete(t.Context(), ledger); err != nil {
					t.Fatal(err)
				}
				replacement := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ledger.Namespace, Name: ledger.Name, Labels: ledger.Labels}, Data: map[string]string{"limit": ledger.Data["limit"]}}
				if err := c.Create(t.Context(), replacement); err != nil {
					t.Fatal(err)
				}
			case "edited":
				ledger.Data = map[string]string{"limit": ledger.Data["limit"]}
				if err := c.Update(t.Context(), ledger); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := driver(c, native).SuspendInstance(t.Context(), second.Key, secondReady.Identity); err == nil || !strings.Contains(err.Error(), "missing retained workspace") {
				t.Fatalf("reset ledger admitted another suspended workspace: %v", err)
			}
			if native.suspends != 1 {
				t.Fatal("reset ledger reached native suspension")
			}
		})
	}
}

func TestResumedWorkspaceHistoryDoesNotHoldSuspendedCapacity(t *testing.T) {
	c, native, request := fixture(t, true)
	request = cappedRequest(t, c, request, 1)
	first := ready(t, c, native, request)
	stopped := retired(t, c, native, request, first.Identity, true)
	next := *request.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &first.Identity
	next.RetainedData = stopped.RetainedData
	next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, next)
	second := restoreRequest(t, c, request, nil)
	secondReady := ready(t, c, native, second)
	if out := retired(t, c, native, second, secondReady.Identity, true); out.RetainedData == nil || native.suspends != 2 {
		t.Fatal("a resumed workspace's archived suspend record kept its released capacity")
	}
}

func TestResetRetentionLedgerKeepsUnfinishedResumeReservation(t *testing.T) {
	c, native, request := fixture(t, true)
	request = cappedRequest(t, c, request, 1)
	first := ready(t, c, native, request)
	stopped := retired(t, c, native, request, first.Identity, true)
	next := *request.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &first.Identity
	next.RetainedData = stopped.RetainedData
	next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if observed, err := driver(c, native).EnsureAllocation(t.Context(), next); err != nil || observed.State == sdk.AllocationReady {
		t.Fatalf("resume unexpectedly finished in one step: state=%q err=%v", observed.State, err)
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Request.Sequence != next.Sequence || !record.SuspendedReservation {
		t.Fatal("unfinished resume did not retain its suspended reservation")
	}
	second := restoreRequest(t, c, request, nil)
	secondReady := ready(t, c, native, second)
	ledger := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), retentionKey(record), ledger); err != nil {
		t.Fatal(err)
	}
	ledger.Data = map[string]string{"limit": ledger.Data["limit"]}
	if err := c.Update(t.Context(), ledger); err != nil {
		t.Fatal(err)
	}
	if _, err := driver(c, native).SuspendInstance(t.Context(), second.Key, secondReady.Identity); err == nil || !strings.Contains(err.Error(), "missing retained workspace") {
		t.Fatalf("reset ledger ignored an unfinished resume reservation: %v", err)
	}
	ready(t, c, native, next)
	_, record, err = driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SuspendedReservation {
		t.Fatal("ready successor kept its released reservation")
	}
	if out := retired(t, c, native, second, secondReady.Identity, true); out.RetainedData == nil {
		t.Fatal("released reservation still blocked suspension")
	}
}
