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
