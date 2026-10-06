// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNativeWorkingDirectoryPreservesLiteralCommandAndArguments(t *testing.T) {
	_, _, request := fixture(t, false)
	workingDir := filepath.Join(t.TempDir(), "literal $(touch injected); `touch injected` directory")
	if err := os.Mkdir(workingDir, 0o755); err != nil {
		t.Fatal(err)
	}
	container := &request.Runtime.Template.Spec.Containers[0]
	container.WorkingDir = workingDir
	container.Command = []string{"/bin/sh", "-c", `printf '%s\n' "$PWD"; printf '%s\n' "$@"`, "working-directory-fixture"}
	container.Args = []string{"literal argument", "$(touch injected)", "--option=value"}
	record, err := newRecord(request)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compileContainer(record)
	if err != nil {
		t.Fatal(err)
	}
	output, err := runCompiledContainer(t, compiled)
	if err != nil {
		t.Fatalf("compiled command failed: %v: %s", err, output)
	}
	expected := workingDir + "\n" + strings.Join(container.Args, "\n") + "\n"
	if string(output) != expected {
		t.Fatalf("compiled command changed directory or arguments: got %q, want %q", output, expected)
	}
	if _, err := os.Stat(filepath.Join(workingDir, "injected")); !os.IsNotExist(err) {
		t.Fatal("working directory or command argument was interpreted as shell code")
	}
}

func runCompiledContainer(t *testing.T, compiled *pb.Container) ([]byte, error) {
	t.Helper()
	// The initialization chmod is native-only. Shadow it locally so executing
	// the compiled command exercises cd/exec without changing host permissions.
	shimDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(shimDir, "chmod"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	args := append(append([]string(nil), compiled.Command[1:]...), compiled.Args...)
	command := exec.CommandContext(t.Context(), compiled.Command[0], args...)
	command.Env = append(os.Environ(), "PATH="+shimDir+":/usr/bin:/bin")
	return command.CombinedOutput()
}

func TestNativeMissingWorkingDirectoryDoesNotExecuteCommand(t *testing.T) {
	_, _, request := fixture(t, false)
	container := &request.Runtime.Template.Spec.Containers[0]
	container.WorkingDir = filepath.Join(t.TempDir(), "missing")
	container.Command = []string{"/bin/sh", "-c", "printf 'unexpected-command-ran\\n'"}
	record, err := newRecord(request)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compileContainer(record)
	if err != nil {
		t.Fatal(err)
	}
	output, err := runCompiledContainer(t, compiled)
	if err == nil || strings.Contains(string(output), "unexpected-command-ran") {
		t.Fatalf("missing working directory executed the command: %v: %s", err, output)
	}
}

func TestNativeRejectsUnrepresentableRuntimeResourcesBeforeCompute(t *testing.T) {
	for _, invalid := range []string{"request only", "request exceeds limit", "unsupported request", "negative request", "relative working directory", "read-only root filesystem"} {
		t.Run(invalid, func(t *testing.T) {
			c, native, request := fixture(t, false)
			container := &request.Runtime.Template.Spec.Containers[0]
			container.Resources = corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
			}
			switch invalid {
			case "request only":
				delete(container.Resources.Limits, corev1.ResourceCPU)
			case "request exceeds limit":
				container.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("3")
			case "unsupported request":
				container.Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("1Gi")
			case "negative request":
				container.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("-1")
			case "relative working directory":
				container.WorkingDir = "relative"
			case "read-only root filesystem":
				readOnly := true
				container.SecurityContext = &corev1.SecurityContext{ReadOnlyRootFilesystem: &readOnly}
			}
			request.Resources = *container.Resources.DeepCopy()
			request.Revision, _ = sdk.WorkloadRevision(request)
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("unrepresentable workload was accepted")
			}
			if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
				t.Fatal("unsupported workload created native compute or a derived template")
			}
		})
	}
}

func TestNativeRequestsCoveredByBookedActorLimits(t *testing.T) {
	for _, requests := range []corev1.ResourceList{
		{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
		{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
		{corev1.ResourceCPU: resource.MustParse("0"), corev1.ResourceMemory: resource.MustParse("0")},
	} {
		c, native, request := fixture(t, false)
		container := &request.Runtime.Template.Spec.Containers[0]
		container.Resources = corev1.ResourceRequirements{Requests: requests, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}}
		request.Runtime.RequiredFeatures = []api.ExecutionWorkspaceFeature{api.WorkspaceFeatureACPRuntime, api.WorkspaceFeatureNativeProcess}
		request.Resources = *container.Resources.DeepCopy()
		request.Revision, _ = sdk.WorkloadRevision(request)
		if err := admit(c)(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		ready(t, c, native, request)
		_, record, err := driver(c, native).read(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		limits := record.TemplateSpec.GetResources().GetLimits()
		if len(limits) != 2 || limits[0].Name != "cpu" || limits[0].Quantity != "2" || limits[1].Name != "memory" || limits[1].Quantity != "4Gi" || record.TemplateSpec.Containers[0].Resources != nil {
			t.Fatalf("native Actor does not book/enforce the limits covering Core requests: %v", limits)
		}
	}
}

func TestNativeProcessIntentPreservesDurableMountWithoutPodScratch(t *testing.T) {
	c, native, request := fixture(t, true)
	request.Runtime.RequiredFeatures = []api.ExecutionWorkspaceFeature{api.WorkspaceFeatureACPRuntime, api.WorkspaceFeatureNativeProcess, api.WorkspaceFeatureSuspend}
	container := &request.Runtime.Template.Spec.Containers[0]
	container.SecurityContext = &corev1.SecurityContext{ReadOnlyRootFilesystem: new(false)}
	container.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
	}
	request.Resources = *container.Resources.DeepCopy()
	request.Revision, _ = sdk.WorkloadRevision(request)
	if len(request.Runtime.Template.Spec.Volumes) != 0 {
		t.Fatal("native intent unexpectedly declares Kubernetes scratch")
	}
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	observed := ready(t, c, native, request)
	if observed.Startup.Process == nil || observed.Startup.Pod != nil {
		t.Fatal("native-process workload did not produce exact process evidence")
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	durable, identity := false, false
	for _, volume := range record.TemplateSpec.Volumes {
		if volume.Name == durableVolumeName && volume.DurableDir != nil {
			durable = true
		}
		if volume.Name == identityVolumeName && volume.SystemInfo != nil {
			identity = true
		}
	}
	if !durable || !identity || len(record.TemplateSpec.Containers[0].VolumeMounts) != 2 {
		t.Fatal("native process intent lost exact durable or identity mounts")
	}
	if revision, err := sdk.WorkloadRevision(request); err != nil || revision != request.Revision {
		t.Fatal("provider rewrote the admitted native intent")
	}
}
