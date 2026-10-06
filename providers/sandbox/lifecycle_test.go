package sandbox

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/sandbox/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type uidClient struct {
	client.Client
	next int
}

func (c *uidClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if object.GetUID() == "" {
		c.next++
		object.SetUID(types.UID(fmt.Sprintf("native-%d", c.next)))
	}
	if object.GetGeneration() == 0 {
		object.SetGeneration(1)
	}
	return c.Client.Create(ctx, object, options...)
}

func fixture(t *testing.T, persistent bool) (client.Client, workspaceprovider.WorkloadRequest) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, storagev1.AddToScheme, workspacev1alpha1.AddToScheme, profilev1alpha1.AddToScheme, sandboxv1beta1.AddToScheme, extv1beta1.AddToScheme} {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := &uidClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}, &workspacev1alpha1.ExecutionWorkspaceProvider{}, &sandboxv1beta1.Sandbox{}, &corev1.Pod{}).Build()}
	hash := "sha256:" + strings.Repeat("1", 64)
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{ObjectMeta: metav1.ObjectMeta{Name: "sandbox", UID: "provider", Generation: 1}, Spec: workspacev1alpha1.ExecutionWorkspaceProviderSpec{ControllerName: ControllerName, LifecycleState: workspacev1alpha1.ExecutionWorkspaceProviderActive}}
	workspace := &workspacev1alpha1.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: "tasks", Name: "workspace", UID: "workspace", Generation: 1}, Spec: workspacev1alpha1.ExecutionWorkspaceSpec{ClassBinding: workspacev1alpha1.ImmutableObjectBinding{Name: "class", UID: "class", Generation: 1, ProfileHash: hash}, ProviderBinding: workspacev1alpha1.ImmutableObjectBinding{Name: provider.Name, UID: provider.UID, Generation: 1, ProfileHash: hash}, DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady}}
	workspace.Spec.CoreAdmission = &workspacev1alpha1.ExecutionWorkspaceCoreAdmission{AdmittedGeneration: 1, ClassBinding: workspace.Spec.ClassBinding, ProviderBinding: workspace.Spec.ProviderBinding}
	workspace.Status.Conditions = []metav1.Condition{{Type: string(workspacev1alpha1.ConditionWorkspaceAdmitted), Status: metav1.ConditionTrue, Reason: string(workspacev1alpha1.ReasonReady), ObservedGeneration: 1}}
	image := "example.invalid/runtime@sha256:" + strings.Repeat("2", 64)
	automount := false
	request := workspaceprovider.WorkloadRequest{Sequence: 1, Key: workspaceprovider.AllocationKey{Namespace: workspace.Namespace, Name: workspace.Name, WorkspaceUID: workspace.UID, ProviderUID: provider.UID}, Image: image, Runtime: &workspaceprovider.RuntimeWorkload{BootstrapPort: 8080, Protocol: "orka.harness.v2", ContainerName: "supervisor", PoolBinding: workspacev1alpha1.ImmutableObjectBinding{Name: "pool", UID: "pool", Generation: 1, ProfileHash: hash}, ClassBinding: workspace.Spec.ClassBinding, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Namespace: "runtimes", Labels: map[string]string{"orka.ai/pool": "pool"}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &automount, Containers: []corev1.Container{{Name: "supervisor", Image: image}}}}}}
	if persistent {
		profile := &profilev1alpha1.SandboxWorkspaceProfile{TypeMeta: metav1.TypeMeta{APIVersion: profilev1alpha1.GroupVersion.String(), Kind: "SandboxWorkspaceProfile"}, ObjectMeta: metav1.ObjectMeta{Namespace: workspace.Namespace, Name: "data", UID: "profile", Generation: 1}, Spec: profilev1alpha1.SandboxWorkspaceProfileSpec{Suspend: &profilev1alpha1.SandboxSuspendPolicy{Mode: profilev1alpha1.SandboxSuspendModeDataOnly, Volume: profilev1alpha1.SandboxDurableVolume{StorageClassName: "dynamic", Capacity: "1Gi"}}}}
		if err := c.Create(t.Context(), profile); err != nil {
			t.Fatal(err)
		}
		raw := &unstructured.Unstructured{}
		raw.SetGroupVersionKind(profilev1alpha1.GroupVersion.WithKind("SandboxWorkspaceProfile"))
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(profile), raw); err != nil {
			t.Fatal(err)
		}
		profileHash, err := workspaceprovider.ParametersProfileHash(raw)
		if err != nil {
			t.Fatal(err)
		}
		request.ParametersRef = &workspacev1alpha1.TypedObjectReference{Group: profilev1alpha1.GroupVersion.Group, Kind: "SandboxWorkspaceProfile", Name: profile.Name}
		request.ParametersBinding = &workspacev1alpha1.ImmutableObjectBinding{Name: profile.Name, UID: profile.UID, Generation: 1, ProfileHash: profileHash}
		container := &request.Runtime.Template.Spec.Containers[0]
		container.VolumeMounts = []corev1.VolumeMount{{Name: durableVolumeName, MountPath: durableMountPath}}
		container.Env = []corev1.EnvVar{{Name: "ORKA_ACP_DURABLE_WORKSPACE_DIR", Value: durableMountPath}}
		deletePolicy := corev1.PersistentVolumeReclaimDelete
		if err := c.Create(t.Context(), &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "dynamic", UID: "storage-class"}, Provisioner: "example.test/csi", ReclaimPolicy: &deletePolicy}); err != nil {
			t.Fatal(err)
		}
	}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	workspace.Spec.Workload = &request
	for _, object := range []client.Object{provider, workspace} {
		if err := c.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	return c, request
}

func admit(t *testing.T, c client.Client) conformance.AdmitRequest {
	t.Helper()
	return func(ctx context.Context, request workspaceprovider.WorkloadRequest) error {
		workspace := &workspacev1alpha1.ExecutionWorkspace{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}, workspace); err != nil {
			return err
		}
		workspace.Spec.Workload = &request
		workspace.Spec.Retirement = nil
		return c.Update(ctx, workspace)
	}
}

// nativeTick simulates only upstream object materialization and garbage
// collection. It is not evidence that a live backend preserves filesystem data.
func nativeTick(ctx context.Context, c client.Client) error {
	var templates extv1beta1.SandboxTemplateList
	if err := c.List(ctx, &templates); err != nil {
		return err
	}
	for i := range templates.Items {
		template := &templates.Items[i]
		hash := sandboxcontrollers.NameHash(template.Name)
		if template.Labels[sandboxv1beta1.SandboxTemplateRefHashLabel] == hash {
			continue
		}
		if template.Labels == nil {
			template.Labels = map[string]string{}
		}
		template.Labels[sandboxv1beta1.SandboxTemplateRefHashLabel] = hash
		if err := c.Update(ctx, template); err != nil {
			return err
		}
	}
	var claims extv1beta1.SandboxClaimList
	if err := c.List(ctx, &claims); err != nil {
		return err
	}
	for i := range claims.Items {
		claim := &claims.Items[i]
		sb := &sandboxv1beta1.Sandbox{}
		key := client.ObjectKeyFromObject(claim)
		if err := c.Get(ctx, key, sb); apierrors.IsNotFound(err) {
			warm := &extv1beta1.SandboxWarmPool{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: claim.Spec.WarmPoolRef.Name}, warm); err != nil {
				return err
			}
			template := &extv1beta1.SandboxTemplate{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: warm.Spec.TemplateRef.Name}, template); err != nil {
				return err
			}
			controller := true
			sb = &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Namespace: claim.Namespace, Name: claim.Name, Labels: map[string]string{extv1beta1.SandboxIDLabel: string(claim.UID)}, Annotations: map[string]string{sandboxv1beta1.SandboxTemplateRefAnnotation: template.Name}, OwnerReferences: []metav1.OwnerReference{{APIVersion: extv1beta1.GroupVersion.String(), Kind: "SandboxClaim", Name: claim.Name, UID: claim.UID, Controller: &controller}}}, Spec: sandboxv1beta1.SandboxSpec{SandboxBlueprint: *template.Spec.SandboxBlueprint.DeepCopy(), OperatingMode: sandboxv1beta1.SandboxOperatingModeRunning}}
			sb.Spec.VolumeClaimTemplates = claim.Spec.VolumeClaimTemplates
			sb.Spec.PodTemplate.ObjectMeta.Labels = maps.Clone(template.Spec.PodTemplate.ObjectMeta.Labels)
			if sb.Spec.PodTemplate.ObjectMeta.Labels == nil {
				sb.Spec.PodTemplate.ObjectMeta.Labels = map[string]string{}
			}
			sb.Spec.PodTemplate.ObjectMeta.Labels[extv1beta1.SandboxIDLabel] = string(claim.UID)
			sb.Spec.PodTemplate.ObjectMeta.Labels[sandboxv1beta1.SandboxTemplateRefHashLabel] = sandboxcontrollers.NameHash(template.Name)
			if err := c.Create(ctx, sb); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	var sandboxes sandboxv1beta1.SandboxList
	if err := c.List(ctx, &sandboxes); err != nil {
		return err
	}
	for i := range sandboxes.Items {
		sb := &sandboxes.Items[i]
		pod := &corev1.Pod{}
		key := client.ObjectKeyFromObject(sb)
		claim := &extv1beta1.SandboxClaim{}
		claimErr := c.Get(ctx, key, claim)
		if apierrors.IsNotFound(claimErr) {
			for _, object := range []client.Object{pod, &corev1.PersistentVolumeClaim{}} {
				name := sb.Name
				if _, ok := object.(*corev1.PersistentVolumeClaim); ok {
					name = durableVolumeName + "-" + sb.Name
				}
				if err := c.Get(ctx, types.NamespacedName{Namespace: sb.Namespace, Name: name}, object); err == nil {
					if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
						pv := &corev1.PersistentVolume{}
						if err := c.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err == nil {
							if err := c.Delete(ctx, pv); err != nil {
								return err
							}
						}
					}
					if err := c.Delete(ctx, object); err != nil {
						return err
					}
				} else if !apierrors.IsNotFound(err) {
					return err
				}
			}
			if err := c.Delete(ctx, sb); err != nil {
				return err
			}
			continue
		} else if claimErr != nil {
			return claimErr
		}
		if sb.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended {
			if err := c.Get(ctx, key, pod); err == nil {
				if err := c.Delete(ctx, pod); err != nil {
					return err
				}
			} else if !apierrors.IsNotFound(err) {
				return err
			}
			sb.Status.Conditions = []metav1.Condition{{Type: string(sandboxv1beta1.SandboxConditionSuspended), Status: metav1.ConditionTrue, ObservedGeneration: sb.Generation, Reason: sandboxv1beta1.SandboxReasonSuspendedPodTerminated}}
			if err := c.Status().Update(ctx, sb); err != nil {
				return err
			}
			continue
		}
		if err := c.Get(ctx, key, pod); err == nil {
			continue
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		controller := true
		pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: sb.Namespace, Name: sb.Name, Labels: maps.Clone(sb.Spec.PodTemplate.ObjectMeta.Labels), Annotations: maps.Clone(sb.Spec.PodTemplate.ObjectMeta.Annotations), OwnerReferences: []metav1.OwnerReference{{APIVersion: sandboxv1beta1.GroupVersion.String(), Kind: "Sandbox", Name: sb.Name, UID: sb.UID, Controller: &controller}}}, Spec: *sb.Spec.PodTemplate.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.2"}}
		pod.Labels[sandboxcontrollers.SandboxNameHashLabel] = sandboxcontrollers.NameHash(sb.Name)
		for _, volume := range sb.Spec.VolumeClaimTemplates {
			name := volume.Name + "-" + sb.Name
			pvc := &corev1.PersistentVolumeClaim{}
			err := c.Get(ctx, types.NamespacedName{Namespace: sb.Namespace, Name: name}, pvc)
			if apierrors.IsNotFound(err) {
				pvc = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: sb.Namespace, Name: name, OwnerReferences: pod.OwnerReferences}, Spec: *volume.Spec.DeepCopy()}
				pvc.Spec.VolumeName = "pv-" + name
				if err := c.Create(ctx, pvc); err != nil {
					return err
				}
				pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName, Annotations: map[string]string{"pv.kubernetes.io/provisioned-by": "example.test/csi"}}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}, StorageClassName: *pvc.Spec.StorageClassName, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete}}
				if err := c.Create(ctx, pv); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: volume.Name, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}})
		}
		if err := c.Create(ctx, pod); err != nil {
			return err
		}
	}
	return nil
}

type simulatedLifecycle struct{ *Lifecycle }

func (d *simulatedLifecycle) tick(ctx context.Context, observed workspaceprovider.AllocationObservation, err error) (workspaceprovider.AllocationObservation, error) {
	if err == nil {
		err = nativeTick(ctx, d.client)
	}
	return observed, err
}
func (d *simulatedLifecycle) EnsureAllocation(ctx context.Context, request workspaceprovider.WorkloadRequest) (workspaceprovider.AllocationObservation, error) {
	o, e := d.Lifecycle.EnsureAllocation(ctx, request)
	return d.tick(ctx, o, e)
}
func (d *simulatedLifecycle) StopInstance(ctx context.Context, key workspaceprovider.AllocationKey, id workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	o, e := d.Lifecycle.StopInstance(ctx, key, id)
	return d.tick(ctx, o, e)
}
func (d *simulatedLifecycle) SuspendInstance(ctx context.Context, key workspaceprovider.AllocationKey, id workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	o, e := d.Lifecycle.SuspendInstance(ctx, key, id)
	return d.tick(ctx, o, e)
}
func (d *simulatedLifecycle) DeleteAllocation(ctx context.Context, key workspaceprovider.AllocationKey, id workspaceprovider.InstanceIdentity, p workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (workspaceprovider.AllocationObservation, error) {
	o, e := d.Lifecycle.DeleteAllocation(ctx, key, id, p)
	return d.tick(ctx, o, e)
}

func ready(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest) workspaceprovider.AllocationObservation {
	t.Helper()
	for range 5 {
		observed, err := (&simulatedLifecycle{New(c)}).EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationReady {
			return observed
		}
	}
	t.Fatal("allocation never became ready")
	return workspaceprovider.AllocationObservation{}
}

func TestLifecycleConformance(t *testing.T) {
	c, request := fixture(t, false)
	if err := conformance.Check(t.Context(), func() workspaceprovider.Lifecycle { return &simulatedLifecycle{New(c)} }, request); err != nil {
		t.Fatal(err)
	}
}

func TestDataSuspensionConformance(t *testing.T) {
	c, request := fixture(t, true)
	if err := conformance.CheckSuspension(t.Context(), func() workspaceprovider.Lifecycle { return &simulatedLifecycle{New(c)} }, request, admit(t, c)); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementConformance(t *testing.T) {
	c, request := fixture(t, false)
	if err := conformance.CheckReplacement(t.Context(), func() workspaceprovider.Lifecycle { return &simulatedLifecycle{New(c)} }, request, admit(t, c)); err != nil {
		t.Fatal(err)
	}
}

func TestUpstreamTemplateReferenceHashPreservesExactOwnership(t *testing.T) {
	for _, mutation := range []string{"valid upstream hash", "foreign hash", "extra label", "changed owner"} {
		t.Run(mutation, func(t *testing.T) {
			c, request := fixture(t, false)
			ready(t, c, request) // nativeTick adds the pinned upstream hash.
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			template := &extv1beta1.SandboxTemplate{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Template.Name}, template); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "foreign hash":
				template.Labels[sandboxv1beta1.SandboxTemplateRefHashLabel] = "foreign"
			case "extra label":
				template.Labels["foreign.example/owner"] = "foreign"
			case "changed owner":
				template.OwnerReferences[0].UID = "foreign"
			}
			if err := c.Update(t.Context(), template); err != nil {
				t.Fatal(err)
			}
			_, err = New(c).EnsureAllocation(t.Context(), request)
			if mutation == "valid upstream hash" {
				if err != nil {
					t.Fatalf("upstream bookkeeping rejected: %v", err)
				}
			} else if !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
				t.Fatalf("foreign ownership accepted: %v", err)
			}
		})
	}
}

func TestForeignNativeIdentityAndPodMutation(t *testing.T) {
	for _, mutation := range []string{"sandboxUID", "claimOwner", "podOwner", "podImage", "podRestart", "volumeSource", "pvUID"} {
		t.Run(mutation, func(t *testing.T) {
			c, request := fixture(t, true)
			observed := ready(t, c, request)
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: observed.Startup.Pod.Namespace, Name: observed.Startup.Pod.Name}, pod); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "sandboxUID", "claimOwner":
				sb := &sandboxv1beta1.Sandbox{}
				if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil {
					t.Fatal(err)
				}
				if mutation == "sandboxUID" {
					sb.UID = "impostor"
				} else {
					sb.OwnerReferences[0].UID = "impostor"
				}
				if err := c.Update(t.Context(), sb); err != nil {
					t.Fatal(err)
				}
			case "podOwner":
				pod.OwnerReferences[0].UID = "impostor"
			case "podImage":
				pod.Spec.Containers[0].Image = "injected"
			case "podRestart":
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "supervisor", RestartCount: 1}}
				if err := c.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "volumeSource":
				pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "foreign"
			case "pvUID":
				pv := &corev1.PersistentVolume{}
				if err := c.Get(t.Context(), types.NamespacedName{Name: record.Storage.VolumeName}, pv); err != nil {
					t.Fatal(err)
				}
				pv.UID = "impostor"
				if err := c.Update(t.Context(), pv); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if _, err := New(c).Observe(t.Context(), request.Key); err == nil {
				t.Fatal("foreign native materialization was accepted")
			}
		})
	}
}

func TestMissingJournalAndMarkerFailClosed(t *testing.T) {
	c, request := fixture(t, false)
	ready(t, c, request)
	cm, _, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("missing required journal was reconstructed")
	}
	if err := New(c).proveNoAllocation(t.Context(), request.Key); err == nil {
		t.Fatal("missing journal was treated as clean deletion")
	}
}

type lostResponseClient struct {
	client.Client
	kind string
	lose bool
}

func (c *lostResponseClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if err := c.Client.Create(ctx, object, options...); err != nil {
		return err
	}
	if c.lose && fmt.Sprintf("%T", object) == c.kind {
		c.lose = false
		return errors.New("lost create response")
	}
	return nil
}

func TestLostNativeCreateResponseRecoversSameUID(t *testing.T) {
	for _, kind := range []string{"*v1.ConfigMap", "*v1beta1.SandboxTemplate", "*v1beta1.SandboxClaim"} {
		t.Run(kind, func(t *testing.T) {
			c, request := fixture(t, false)
			faulty := &lostResponseClient{Client: c, kind: kind, lose: true}
			if _, err := New(faulty).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("expected lost response")
			}
			ready(t, c, request)
			var claims extv1beta1.SandboxClaimList
			if err := c.List(t.Context(), &claims); err != nil {
				t.Fatal(err)
			}
			if len(claims.Items) != 1 {
				t.Fatal("native claim was duplicated")
			}
		})
	}
}
