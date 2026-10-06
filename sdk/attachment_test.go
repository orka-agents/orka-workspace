package workspaceprovider

import (
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
)

func TestAttachmentEpochAcknowledgement(t *testing.T) {
	for _, test := range []struct{ epoch, counter, want int64 }{{5, 5, 5}, {4, 5, 0}, {5, 4, 0}, {0, 0, 0}, {-1, -1, 0}} {
		workspace := &api.ExecutionWorkspace{Spec: api.ExecutionWorkspaceSpec{AttachmentEpoch: test.counter, Attachment: &api.ExecutionWorkspaceAttachment{Epoch: test.epoch}}}
		if got := AttachmentEpochAcknowledgement(workspace); got != test.want {
			t.Fatalf("epoch=%d counter=%d acknowledged=%d, want %d", test.epoch, test.counter, got, test.want)
		}
	}
	if AttachmentEpochAcknowledgement(nil) != 0 || AttachmentEpochAcknowledgement(&api.ExecutionWorkspace{}) != 0 {
		t.Fatal("acknowledged absent attachment intent")
	}
}
