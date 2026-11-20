// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

// nativeWatch (NATIVE_WATCH=1) prints one JSON line whenever a native Worker
// of the run namespace, its Actor assignments, an Actor of the run Atespace,
// or an Actor's default egress policy changes. It is a read-only observer for
// live validation and reports only identities, states, epochs, capacities,
// labels and egress host lists, never credentials.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
)

func watchEmit(kind, key string, view map[string]any) {
	view["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	view["kind"], view["key"] = kind, key
	encoded, _ := json.Marshal(view)
	fmt.Println("NATIVE_WATCH", string(encoded))
}

func egressHosts(policy *pb.EgressPolicy) []string {
	var hosts []string
	for _, rule := range policy.GetRules() {
		hosts = append(hosts, rule.GetHttp().GetHostnames()...)
		hosts = append(hosts, rule.GetHttps().GetHostnames()...)
		hosts = append(hosts, rule.GetTlsPassthrough().GetHostnames()...)
	}
	sort.Strings(hosts)
	return slices.Compact(hosts)
}

func (p *proof) nativeWatch(ctx context.Context) (map[string]any, error) {
	interval := 250 * time.Millisecond
	seen := map[string]string{}
	changed := func(kind, key string, view map[string]any) {
		encoded, _ := json.Marshal(view)
		if seen[kind+"/"+key] != string(encoded) {
			seen[kind+"/"+key] = string(encoded)
			watchEmit(kind, key, view)
		}
	}
	errors := 0
	for ctx.Err() == nil {
		pass, cancel := context.WithTimeout(ctx, 10*time.Second)
		current := map[string]bool{}
		workers, err := p.native.ListWorkers(pass, &pb.ListWorkersRequest{Options: &pb.ListOptions{PageSize: 1000}})
		if err != nil {
			errors++
			watchEmit("error", "ListWorkers", map[string]any{"error": err.Error()})
		}
		for _, w := range workers.GetWorkers() {
			if w.GetWorkerNamespace() != p.namespace {
				continue
			}
			key := w.GetMetadata().GetName()
			current["worker/"+key] = true
			st := w.GetStatus()
			view := map[string]any{"uid": w.GetMetadata().GetUid(), "pool": w.GetWorkerPool(), "pod": w.GetWorkerPod(), "podUID": w.GetWorkerPodUid(),
				"labels": w.GetLabels(), "state": st.GetState().String(), "capacityMilliCPU": st.GetCapacity().GetCpuMillicores(),
				"capacityMemory": st.GetCapacity().GetMemoryBytes(), "allocatedMilliCPU": st.GetAllocated().GetCpuMillicores(),
				"allocatedMemory": st.GetAllocated().GetMemoryBytes(), "observedEpoch": st.GetObservedEpoch(),
				"registeredEpoch": st.GetRegisteredEpoch(), "issuedEpoch": st.GetIssuedEpoch(), "version": w.GetMetadata().GetVersion()}
			assignments, err := p.native.ListWorkerActorAssignments(pass, &pb.ListWorkerActorAssignmentsRequest{Worker: &pb.ObjectRef{Name: key}, Options: &pb.ListOptions{PageSize: 1000}})
			if err != nil {
				view["assignmentsError"] = err.Error()
			} else {
				var names []string
				for _, a := range assignments.GetActorAssignments() {
					names = append(names, a.GetActor().GetName())
				}
				sort.Strings(names)
				view["assignments"] = names
			}
			changed("worker", key, view)
		}
		actors, err := p.native.ListActors(pass, &pb.ListActorsRequest{Atespace: p.namespace, Options: &pb.ListOptions{PageSize: 1000}})
		if err != nil {
			errors++
			watchEmit("error", "ListActors", map[string]any{"error": err.Error()})
		}
		for _, a := range actors.GetActors() {
			key := a.GetMetadata().GetName()
			current["actor/"+key] = true
			as := a.GetStatus().GetWorkerAssignment()
			view := map[string]any{"uid": a.GetMetadata().GetUid(), "version": a.GetMetadata().GetVersion(), "state": a.GetStatus().GetState().String(),
				"worker": as.GetWorker().GetName(), "workerPod": as.GetWorkerPod(), "workerPool": as.GetWorkerPool()}
			if policy, err := p.native.GetActorEgressPolicy(pass, &pb.GetActorEgressPolicyRequest{Actor: &pb.ObjectRef{Atespace: p.namespace, Name: key}, Name: "default"}); err != nil {
				view["egressError"] = strings.SplitN(err.Error(), ":", 2)[0]
			} else {
				view["egressHosts"] = egressHosts(policy)
				view["egressVersion"] = policy.GetMetadata().GetVersion()
			}
			changed("actor", key, view)
		}
		for key := range seen {
			if !current[key] && !strings.HasPrefix(seen[key], `{"gone"`) {
				kind, name, _ := strings.Cut(key, "/")
				seen[key] = `{"gone":true}`
				watchEmit(kind, name, map[string]any{"gone": true})
			}
		}
		cancel()
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
	return map[string]any{"errors": errors}, nil
}

func nativeWatchDuration() time.Duration {
	if raw := os.Getenv("NATIVE_WATCH_DURATION"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			return d
		}
	}
	return 3 * time.Hour
}
