package policy_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func attachment(epoch int64) map[string]any {
	return map[string]any{"epoch": epoch, "taskRef": map[string]any{"name": "task", "uid": "task-uid"}, "tokenSHA256": "digest", "expiresAt": "2026-10-06T12:00:00Z"}
}

func TestAttachmentIntentRequiresMatchingAdvancingEpoch(t *testing.T) {
	policy := compilePolicy(t)
	for _, test := range []struct {
		name    string
		mutate  func(*unstructured.Unstructured)
		allowed bool
	}{
		{"idempotent", func(*unstructured.Unstructured) {}, true},
		{"counter mismatch", func(w *unstructured.Unstructured) { set(t, w, int64(4), "spec", "attachment", "epoch") }, false},
		{"same epoch task replacement", func(w *unstructured.Unstructured) { set(t, w, "other-uid", "spec", "attachment", "taskRef", "uid") }, false},
		{"same epoch token replacement", func(w *unstructured.Unstructured) { set(t, w, "other-digest", "spec", "attachment", "tokenSHA256") }, false},
		{"same epoch expiry replacement", func(w *unstructured.Unstructured) {
			set(t, w, "2026-10-07T12:00:00Z", "spec", "attachment", "expiresAt")
		}, false},
		{"advanced intent", func(w *unstructured.Unstructured) {
			set(t, w, attachment(6), "spec", "attachment")
			set(t, w, int64(6), "spec", "attachmentEpoch")
		}, true},
		{"missing counter", func(w *unstructured.Unstructured) {
			unstructured.RemoveNestedField(w.Object, "spec", "attachmentEpoch")
		}, false},
		{"clear intent", func(w *unstructured.Unstructured) { unstructured.RemoveNestedField(w.Object, "spec", "attachment") }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			old := fixture("executionworkspaces")
			set(t, old, attachment(5), "spec", "attachment")
			set(t, old, int64(5), "spec", "attachmentEpoch")
			updated := old.DeepCopy()
			test.mutate(updated)
			if got := policy.allows(t, "executionworkspaces", coreUser, "", updated, old); got != test.allowed {
				t.Fatalf("allowed=%t, want %t", got, test.allowed)
			}
		})
	}
	for _, epoch := range []int64{-1, 0, 1} {
		w := fixture("executionworkspaces")
		set(t, w, attachment(epoch), "spec", "attachment")
		set(t, w, epoch, "spec", "attachmentEpoch")
		if got := policy.allows(t, "executionworkspaces", coreUser, "", w, nil); got != (epoch > 0) {
			t.Fatalf("created epoch %d allowed=%t", epoch, got)
		}
	}
}

func TestAttachmentHighWaterAndLegacyCleanup(t *testing.T) {
	policy := compilePolicy(t)
	old := fixture("executionworkspaces")
	set(t, old, int64(5), "spec", "attachmentEpoch")
	for _, epoch := range []int64{4, 5, 6} {
		updated := old.DeepCopy()
		set(t, updated, attachment(epoch), "spec", "attachment")
		set(t, updated, epoch, "spec", "attachmentEpoch")
		if got := policy.allows(t, "executionworkspaces", coreUser, "", updated, old); got != (epoch > 5) {
			t.Fatalf("reattachment epoch %d allowed=%t", epoch, got)
		}
	}
	// A stored mismatch cannot authorize positive acknowledgement, but must not
	// block status revocation, desired deletion, or clearing the old intent.
	set(t, old, attachment(7), "spec", "attachment")
	updated := old.DeepCopy()
	set(t, updated, int64(0), "status", "attachedEpoch")
	if !policy.allows(t, "executionworkspaces", providerUser, "status", updated, old) {
		t.Fatal("legacy mismatch blocked status revocation")
	}
	updated = old.DeepCopy()
	set(t, updated, "Deleted", "spec", "desiredState")
	if !policy.allows(t, "executionworkspaces", coreUser, "", updated, old) {
		t.Fatal("legacy mismatch blocked deletion")
	}
	updated = old.DeepCopy()
	unstructured.RemoveNestedField(updated.Object, "spec", "attachment")
	if policy.allows(t, "executionworkspaces", coreUser, "", updated, old) {
		t.Fatal("clearing legacy intent lost its high-water mark")
	}
	set(t, updated, int64(7), "spec", "attachmentEpoch")
	if !policy.allows(t, "executionworkspaces", coreUser, "", updated, old) {
		t.Fatal("legacy mismatch blocked intent revocation with preserved high-water")
	}
	updated = old.DeepCopy()
	set(t, updated, attachment(6), "spec", "attachment")
	set(t, updated, int64(6), "spec", "attachmentEpoch")
	if policy.allows(t, "executionworkspaces", coreUser, "", updated, old) {
		t.Fatal("replacement regressed the legacy intent high-water")
	}
}
