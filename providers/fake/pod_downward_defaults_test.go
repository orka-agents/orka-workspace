// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"context"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type downwardAdmissionClient struct{ client.Client }

func (c *downwardAdmissionClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if pod, ok := object.(*corev1.Pod); ok {
		// Match Kubernetes v1.37 Pod defaulting independently of comparison:
		// volume DefaultMode and FieldRef.APIVersion default; item Mode stays
		// untouched (it inherits the volume mode when files are written).
		for i := range pod.Spec.Volumes {
			volume := &pod.Spec.Volumes[i]
			var groups [][]corev1.DownwardAPIVolumeFile
			if volume.DownwardAPI != nil {
				if volume.DownwardAPI.DefaultMode == nil {
					volume.DownwardAPI.DefaultMode = new(int32(0644))
				}
				groups = append(groups, volume.DownwardAPI.Items)
			}
			if volume.Projected != nil {
				if volume.Projected.DefaultMode == nil {
					volume.Projected.DefaultMode = new(int32(0644))
				}
				for j := range volume.Projected.Sources {
					if source := volume.Projected.Sources[j].DownwardAPI; source != nil {
						groups = append(groups, source.Items)
					}
				}
			}
			for _, items := range groups {
				for j := range items {
					if items[j].FieldRef != nil && items[j].FieldRef.APIVersion == "" {
						items[j].FieldRef.APIVersion = "v1"
					}
				}
			}
		}
	}
	return c.Client.Create(ctx, object, options...)
}

func TestDownwardAdmissionDefaultsPreserveExactFakeStartup(t *testing.T) {
	for _, projected := range []bool{false, true} {
		for _, explicitMode := range []bool{false, true} {
			name := map[bool]string{false: "direct", true: "projected"}[projected] + "/" + map[bool]string{false: "inherited item mode", true: "explicit item mode"}[explicitMode]
			t.Run(name, func(t *testing.T) {
				for _, drift := range []string{"API version", "item mode", "field", "volume mode"} {
					t.Run(drift, func(t *testing.T) {
						base, request := runtimeFixture(t)
						c := &downwardAdmissionClient{Client: base}
						item := corev1.DownwardAPIVolumeFile{Path: "uid", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}
						if explicitMode {
							item.Mode = new(int32(0640))
						}
						volume := corev1.Volume{Name: "metadata"}
						if projected {
							volume.Projected = &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{item}}}}}
						} else {
							volume.DownwardAPI = &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{item}}
						}
						request.Runtime.Template.Spec.Volumes = []corev1.Volume{volume}
						request.Runtime.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: volume.Name, MountPath: "/metadata", ReadOnly: true}}
						var err error
						request.Revision, err = sdk.WorkloadRevision(request)
						if err != nil {
							t.Fatal(err)
						}
						if err := publishRequest(t.Context(), c, request); err != nil {
							t.Fatal(err)
						}
						ready, err := New(c).EnsureAllocation(t.Context(), request)
						if err != nil || ready.State != sdk.AllocationReady || ready.Startup == nil || ready.Startup.Pod == nil {
							t.Fatalf("defaulted downward volume lost startup: observation=%+v error=%v", ready, err)
						}
						identity := *ready.Startup.Pod
						if observed, err := New(c).Observe(t.Context(), request.Key); err != nil || observed.State != sdk.AllocationReady || observed.Startup == nil || *observed.Startup.Pod != identity || observed.Identity != ready.Identity {
							t.Fatalf("defaulted volume lost its exact observed fence: observation=%+v error=%v", observed, err)
						}
						pod := &corev1.Pod{}
						if err := c.Get(t.Context(), client.ObjectKey{Namespace: identity.Namespace, Name: identity.Name}, pod); err != nil {
							t.Fatal(err)
						}
						var actualItem *corev1.DownwardAPIVolumeFile
						var volumeMode **int32
						if projected {
							actualItem = &pod.Spec.Volumes[0].Projected.Sources[0].DownwardAPI.Items[0]
							volumeMode = &pod.Spec.Volumes[0].Projected.DefaultMode
						} else {
							actualItem = &pod.Spec.Volumes[0].DownwardAPI.Items[0]
							volumeMode = &pod.Spec.Volumes[0].DownwardAPI.DefaultMode
						}
						if actualItem.FieldRef.APIVersion != "v1" || *volumeMode == nil || **volumeMode != 0644 || (actualItem.Mode != nil) != explicitMode || (explicitMode && *actualItem.Mode != 0640) {
							t.Fatal("fixture did not apply the pinned downward defaults")
						}
						if revision, err := sdk.WorkloadRevision(request); err != nil || revision != request.Revision || item.FieldRef.APIVersion != "" {
							t.Fatal("normalization changed frozen admitted metadata")
						}
						switch drift {
						case "API version":
							actualItem.FieldRef.APIVersion = "different/v1"
						case "item mode":
							actualItem.Mode = new(int32(0400))
						case "field":
							actualItem.FieldRef.FieldPath = "metadata.name"
						case "volume mode":
							*volumeMode = new(int32(0400))
						}
						if err := c.Update(t.Context(), pod); err != nil {
							t.Fatal(err)
						}
						if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
							t.Fatalf("explicit downward drift published startup: observation=%+v error=%v", observed, err)
						}
						if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
							t.Fatalf("explicit downward drift remained observable: observation=%+v error=%v", observed, err)
						}
						if base.creates != 1 {
							t.Fatal("defaulting or drift replaced the exact Pod lifetime")
						}
					})
				}
			})
		}
	}
}
