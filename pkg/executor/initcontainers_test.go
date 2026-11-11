package executor

import (
	"slices"
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// interlink.eu/disable-offload-init-containers names, comma separated, init
// containers that the job does not run, on the template or on the owner: the
// member the plugin gets has neither them nor the volumes only they mount.
func TestDisableOffloadInitContainers(t *testing.T) {
	const list = " agent-injector, ,missing,cache "
	for name, annotate := range map[string]func(*v1alpha1.OffloadRequest){
		"template": func(q *v1alpha1.OffloadRequest) {
			q.Spec.PodSets[0].Template.Annotations = map[string]string{DisableOffloadInitContainersAnnotation: list}
		},
		"owner": func(q *v1alpha1.OffloadRequest) {
			q.Spec.OwnerAnnotations = map[string]string{DisableOffloadInitContainersAnnotation: list}
		},
		"none": func(*v1alpha1.OffloadRequest) {},
	} {
		t.Run(name, func(t *testing.T) {
			q := testRequest(1)
			emptyDir := v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}
			spec := &q.Spec.PodSets[0].Template.Spec
			spec.Volumes = []v1.Volume{{Name: "agent-binary", VolumeSource: emptyDir}, {Name: "scratch", VolumeSource: emptyDir}, {Name: "data", VolumeSource: emptyDir}}
			spec.InitContainers = []v1.Container{
				{Name: "agent-injector", Image: busyboxImage, VolumeMounts: []v1.VolumeMount{{Name: "agent-binary", MountPath: "/agent-binary"}, {Name: "scratch", MountPath: "/scratch"}}},
				{Name: "fetch", Image: busyboxImage, VolumeMounts: []v1.VolumeMount{{Name: "data", MountPath: "/data"}}},
				{Name: "cache", Image: busyboxImage, VolumeMounts: []v1.VolumeMount{{Name: "scratch", MountPath: "/scratch"}}},
			}
			spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "agent-binary", MountPath: "/usr/bin/agent", SubPath: "agent"}, {Name: "data", MountPath: "/data"}}
			annotate(q)

			e := newExecutorEnv(t)
			e.submit(q)
			pod := e.plugin.createdPods()[0]
			var inits, volumes []string
			for _, c := range pod.Spec.InitContainers {
				inits = append(inits, c.Name)
			}
			for _, v := range pod.Spec.Volumes {
				volumes = append(volumes, v.Name)
			}
			// the executor adds the ServiceAccount directory's volume
			wantInits, wantVolumes := []string{"fetch"}, []string{"agent-binary", "data", "kube-api-access-interlink"}
			if name == "none" {
				wantInits, wantVolumes = []string{"agent-injector", "fetch", "cache"}, []string{"agent-binary", "scratch", "data", "kube-api-access-interlink"}
			}
			if !slices.Equal(inits, wantInits) || !slices.Equal(volumes, wantVolumes) {
				t.Fatalf("init containers %v, volumes %v; want %v, %v", inits, volumes, wantInits, wantVolumes)
			}
		})
	}
}
