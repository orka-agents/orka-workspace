package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Retire retained failed-run allocations through the provider. This mode never
// calls Ensure or Resume and refuses changed namespace/workspace lifetimes.
func (p *proof) cleanup(ctx context.Context) error {
	if !strings.HasPrefix(p.namespace, "external-substrate-proof-") {
		return fmt.Errorf("cleanup namespace is not a scoped native proof")
	}
	namespace := &corev1.Namespace{}
	if err := p.c.Get(ctx, client.ObjectKey{Name: p.namespace}, namespace); err != nil {
		return err
	}
	if string(namespace.UID) != os.Getenv("SUBSTRATE_E2E_NAMESPACE_UID") || namespace.DeletionTimestamp != nil {
		return fmt.Errorf("cleanup namespace lifetime changed")
	}
	expected := map[string]string{}
	if err := json.Unmarshal([]byte(os.Getenv("SUBSTRATE_E2E_WORKSPACE_UIDS")), &expected); err != nil {
		return err
	}
	workspaces := &api.ExecutionWorkspaceList{}
	if err := p.c.List(ctx, workspaces, client.InNamespace(p.namespace)); err != nil {
		return err
	}
	if len(workspaces.Items) != len(expected) {
		return fmt.Errorf("cleanup workspace inventory changed")
	}
	for i := range workspaces.Items {
		w := &workspaces.Items[i]
		if expected[w.Name] != string(w.UID) || w.Status.Allocation == nil || w.Spec.ProviderBinding.Name != "substrate" {
			return fmt.Errorf("cleanup workspace exact identity is missing")
		}
		if err := p.retire(ctx, w, api.WorkloadRetirementDelete); err != nil {
			actor, readErr := p.native.GetActor(ctx, &pb.GetActorRequest{Actor: &pb.ObjectRef{Atespace: p.namespace, Name: w.Status.ExternalID}})
			if readErr == nil {
				fmt.Printf("CLEANUP_TERMINAL_DIAGNOSTIC actorUID=%s state=%s assignment=%s\n", actor.GetMetadata().GetUid(), actor.GetStatus().GetState(), actor.GetStatus().GetWorkerAssignment())
			}
			return err
		}
		p.passed("failed-run allocation retired by exact provider identity without boot replay: " + w.Name)
	}
	checkpoints := &api.ExecutionWorkspaceCheckpointList{}
	if err := p.c.List(ctx, checkpoints, client.InNamespace(p.namespace)); err != nil {
		return err
	}
	controller := &provider.CheckpointReconciler{Client: p.c, Control: p.native, Config: p.config}
	for i := range checkpoints.Items {
		checkpoint := &checkpoints.Items[i]
		if err := p.c.Delete(ctx, checkpoint, client.Preconditions{UID: &checkpoint.UID}); err != nil {
			return err
		}
		if err := poll(ctx, func() (bool, error) {
			if _, err := controller.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(checkpoint)}); err != nil {
				return false, err
			}
			err := p.c.Get(ctx, client.ObjectKeyFromObject(checkpoint), &api.ExecutionWorkspaceCheckpoint{})
			return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
		}); err != nil {
			return err
		}
	}
	for i := range workspaces.Items {
		w := &workspaces.Items[i]
		if err := p.c.Delete(ctx, w, client.Preconditions{UID: &w.UID}); err != nil {
			return err
		}
		if err := poll(ctx, func() (bool, error) {
			err := p.c.Get(ctx, client.ObjectKeyFromObject(w), &api.ExecutionWorkspace{})
			return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
		}); err != nil {
			return err
		}
	}
	catalogs := &corev1.ConfigMapList{}
	if err := p.c.List(ctx, catalogs, client.InNamespace(p.namespace), client.MatchingLabels{"substrate.workspace.orka.ai/checkpoint-catalog": "substrate.workspace.checkpoint.v1"}); err != nil {
		return err
	}
	for _, cm := range catalogs.Items {
		if err := p.collectCatalog(ctx, p.namespace, cm.Name, string(cm.UID)); err != nil {
			return err
		}
	}
	actors, err := p.native.ListActors(ctx, &pb.ListActorsRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil || len(actors.GetActors()) != 0 || actors.GetNextPageToken() != "" {
		return fmt.Errorf("cleanup retains native Actors: %v", err)
	}
	tags, err := p.native.ListTags(ctx, &pb.ListTagsRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil || len(tags.GetTags()) != 0 || tags.GetNextPageToken() != "" {
		return fmt.Errorf("cleanup retains native Data Tags: %v", err)
	}
	templates, err := p.native.ListActorTemplates(ctx, &pb.ListActorTemplatesRequest{Atespace: p.namespace, PageSize: 1000})
	if err != nil || templates.GetNextPageToken() != "" {
		return fmt.Errorf("cleanup template inventory failed: %v", err)
	}
	for _, template := range templates.GetActorTemplates() {
		if template.GetMetadata().GetAtespace() != p.namespace || template.GetMetadata().GetName() != "infrastructure" || template.GetMetadata().GetUid() == "" || len(template.GetContainers()) != 1 || template.GetContainers()[0].GetImage() != p.image || template.GetWorkerSelector().GetMatchLabels()["orka.workspace.e2e/pool"] != p.namespace {
			return fmt.Errorf("cleanup has an unexpected native template")
		}
		ref := &pb.ObjectRef{Atespace: p.namespace, Name: "infrastructure"}
		again, err := p.native.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: ref})
		if err != nil || again.GetMetadata().GetUid() != template.GetMetadata().GetUid() || again.GetMetadata().GetVersion() != template.GetMetadata().GetVersion() {
			return fmt.Errorf("cleanup infrastructure template lifetime changed: %v", err)
		}
		if _, err := p.native.DeleteActorTemplate(ctx, &pb.DeleteActorTemplateRequest{ActorTemplate: ref}); err != nil {
			return err
		}
		if _, err := p.native.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: ref}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("cleanup infrastructure template remains: %v", err)
		}
	}
	space, err := p.native.GetAtespace(ctx, &pb.GetAtespaceRequest{Atespace: &pb.ObjectRef{Name: p.namespace}})
	if err != nil || space.GetMetadata().GetUid() == "" || space.GetMetadata().GetName() != p.namespace {
		return fmt.Errorf("cleanup native Atespace identity missing: %v", err)
	}
	if _, err := p.native.DeleteAtespace(ctx, &pb.DeleteAtespaceRequest{Atespace: &pb.ObjectRef{Name: p.namespace}}); err != nil {
		return err
	}
	if _, err := p.native.GetAtespace(ctx, &pb.GetAtespaceRequest{Atespace: &pb.ObjectRef{Name: p.namespace}}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("cleanup native Atespace remains: %v", err)
	}
	p.passed("failed-run native Actors, Tags, templates and Atespace are absent")
	report, err := json.Marshal(map[string]any{"namespace": p.namespace, "namespaceUID": namespace.UID, "workspaceUIDs": expected, "checks": p.checks, "nativeCommit": provider.UpstreamCommit})
	if err != nil {
		return err
	}
	fmt.Println("NATIVE_CLEANUP_PROOF", string(report))
	return nil
}

func (p *proof) collectCatalog(ctx context.Context, namespace, name, uid string) error {
	if !strings.HasPrefix(namespace, "external-substrate-proof-") || !strings.HasPrefix(name, "substrate-data-") || uid == "" {
		return fmt.Errorf("catalog sweep scope is invalid")
	}
	target := &corev1.Namespace{}
	if err := p.c.Get(ctx, client.ObjectKey{Name: namespace}, target); err != nil {
		return err
	}
	if string(target.UID) != os.Getenv("SUBSTRATE_E2E_NAMESPACE_UID") {
		return fmt.Errorf("catalog sweep namespace lifetime changed")
	}
	controller := &provider.CheckpointReconciler{Client: p.c, Control: p.native, Config: p.config}
	if err := poll(ctx, func() (bool, error) {
		cm := &corev1.ConfigMap{}
		err := p.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cm)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if string(cm.UID) != uid {
			return false, fmt.Errorf("catalog sweep UID changed")
		}
		_, err = controller.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: namespace, Name: "catalog/" + name}})
		return false, err
	}); err != nil {
		return err
	}
	p.passed("provider catalog controller finalized exact collected catalog: " + namespace + "/" + name)
	return nil
}
