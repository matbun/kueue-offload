package executor

import (
	"testing"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// A pod group whose head names no launcher runs every member's command to its
// end (gang-end all); one with a launcher, and independent pods, keep the
// plugin's default.
func TestGangEnd(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*v1alpha1.OffloadRequest)
		count int32
		want  string
	}{
		{"one pod", nil, 1, ""},
		{"pod group", nil, 2, GangEndAll},
		{"launcher in the template", func(q *v1alpha1.OffloadRequest) {
			q.Spec.PodSets[0].Template.Annotations = map[string]string{LauncherAnnotation: "torchrun"}
		}, 2, ""},
		{"launcher from the flavor", func(q *v1alpha1.OffloadRequest) {
			q.Spec.PodSets[0].Annotations[LauncherAnnotation] = "srun"
		}, 2, ""},
		{"independent pods", func(q *v1alpha1.OffloadRequest) {
			q.Spec.PodSets[0].Template.Annotations = map[string]string{executorapi.IndependentPodsAnnotation: "true"}
		}, 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := testRequest(tc.count)
			if tc.edit != nil {
				tc.edit(q)
			}
			req := jobRequestOf(q)
			for i := 0; i < int(tc.count); i++ {
				if got := buildMember(req, i).Annotations[GangEndAnnotation]; got != tc.want {
					t.Errorf("member %d: gang-end %q, want %q", i, got, tc.want)
				}
			}
		})
	}
}
