// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"testing"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestCompileResourcesAcceptsExactFractionalMemoryNotations(t *testing.T) {
	for notation, want := range map[string]int64{"1.5Gi": 1610612736, "1Gi": 1 << 30, "1500M": 1500000000} {
		requirements := corev1.ResourceRequirements{Limits: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse(notation),
		}}
		compiled, err := compileResources(nil, requirements)
		if err != nil || compiled.GetLimits().GetMemoryBytes() != want {
			t.Fatalf("%s: compiled %v, %v; want %d bytes", notation, compiled, err, want)
		}
	}
	fractional := corev1.ResourceRequirements{Limits: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1.5"),
	}}
	if _, err := compileResources(nil, fractional); err == nil {
		t.Fatal("a fractional byte count was accepted")
	}
}

func TestRetiringWorkerToleratesOnlyRegistrationWindow(t *testing.T) {
	record := &journalRecord{TemplateSpec: &pb.ActorTemplate{Resources: &pb.ResourceRequirements{
		Limits: &pb.Resources{CpuMillicores: 1000, MemoryBytes: 1 << 30},
	}}}
	worker := func(classes ...string) *pb.Worker {
		status := &pb.WorkerStatus{Capacity: &pb.WorkerResources{Actors: 1000, CpuMillicores: 1000, MemoryBytes: 1 << 30}}
		for _, class := range classes {
			status.SandboxClasses = append(status.SandboxClasses, &pb.WorkerSandboxClass{SandboxClass: class})
		}
		return &pb.Worker{Status: status}
	}
	if err := verifySingleActorWorkerShape(record, worker(), true); err != nil {
		t.Fatalf("re-registering worker blocked retirement: %v", err)
	}
	if err := verifySingleActorWorker(record, worker()); err == nil {
		t.Fatal("startup accepted a worker with no registered sandbox class")
	}
	if err := verifySingleActorWorkerShape(record, worker("microvm"), true); err == nil {
		t.Fatal("retirement accepted a worker of another sandbox class")
	}
	resized := worker()
	resized.Status.Capacity.MemoryBytes = 2 << 30
	if err := verifySingleActorWorkerShape(record, resized, true); err == nil {
		t.Fatal("retirement accepted a worker that can host another Actor")
	}
}
