package offload

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// recordingIndexer keeps the index functions SetupIndexes registers, by object
// type and field.
type recordingIndexer map[string]client.IndexerFunc

func (r recordingIndexer) IndexField(_ context.Context, obj client.Object, field string, fn client.IndexerFunc) error {
	r[fmt.Sprintf("%T %s", obj, field)] = fn
	return nil
}

// Pods are indexed by the Workload their podSetUpdates annotation names, so
// that the binder and the virtual node find a Workload's pods.
func TestPodWorkloadIndex(t *testing.T) {
	indexes := recordingIndexer{}
	if err := SetupIndexes(context.Background(), indexes); err != nil {
		t.Fatal(err)
	}
	index, ok := indexes[fmt.Sprintf("%T %s", &corev1.Pod{}, PodWorkloadKey)]
	if !ok {
		t.Fatalf("no Pod index %s among %v", PodWorkloadKey, slices.Sorted(maps.Keys(indexes)))
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "train-0", Annotations: map[string]string{v1alpha1.WorkloadAnnotation: "job-train-5d565"}}}
	if got := index(pod); !slices.Equal(got, []string{"job-train-5d565"}) {
		t.Errorf("annotated pod indexed as %v", got)
	}
	if got := index(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other"}}); got != nil {
		t.Errorf("pod without the annotation indexed as %v", got)
	}
}
