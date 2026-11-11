package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/offload"
)

// recordingIndexer keeps the index functions offload.SetupIndexes registers,
// for the fake client.
type recordingIndexer map[string]client.IndexerFunc

func (r recordingIndexer) IndexField(_ context.Context, obj client.Object, field string, fn client.IndexerFunc) error {
	r[fmt.Sprintf("%T %s", obj, field)] = fn
	return nil
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, kueue.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// apiCall is a write the reconciler sent, in order.
type apiCall struct {
	verb, object string
	patch        string
	grace        *int64
	uid          *types.UID
}

// fakeClient is a fake API server with the offload controller's indexes; it
// records every write.
func fakeClient(t *testing.T, calls *[]apiCall, objs ...client.Object) client.Client {
	t.Helper()
	idx := recordingIndexer{}
	if err := offload.SetupIndexes(context.Background(), idx); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&corev1.Pod{}, &v1alpha1.OffloadRequest{}, &kueue.Workload{}).
		WithIndex(&corev1.Pod{}, offload.PodWorkloadKey, idx[fmt.Sprintf("%T %s", &corev1.Pod{}, offload.PodWorkloadKey)]).
		WithIndex(&v1alpha1.OffloadRequest{}, offload.RequestWorkloadKey, idx[fmt.Sprintf("%T %s", &v1alpha1.OffloadRequest{}, offload.RequestWorkloadKey)]).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				data, _ := patch.Data(obj)
				*calls = append(*calls, apiCall{verb: "patch", object: obj.GetName(), patch: string(data)})
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				data, _ := patch.Data(obj)
				*calls = append(*calls, apiCall{verb: "patch " + sub, object: obj.GetName(), patch: string(data)})
				return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				do := &client.DeleteOptions{}
				do.ApplyOptions(opts)
				call := apiCall{verb: "delete", object: obj.GetName(), grace: do.GracePeriodSeconds}
				if do.Preconditions != nil {
					call.uid = do.Preconditions.UID
				}
				*calls = append(*calls, call)
				return c.Delete(ctx, obj, opts...)
			},
		}).
		Build()
}

func reconcilePod(t *testing.T, c client.Client, name string) {
	t.Helper()
	r := &PodStatusReconciler{Client: c, NodeIP: testHostIP, Now: func() time.Time { return t2 }}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}}); err != nil {
		t.Fatal(err)
	}
}

func getPod(t *testing.T, c client.Client, name string) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

// The node shows the live request, the newest attempt of the pod's Workload
// that is not being deleted, and writes the pod's status only when it changes.
func TestReconcileShowsLiveRequest(t *testing.T) {
	live := runningRequest()
	stale := testRequest()
	stale.Name, stale.Spec.Attempt, stale.Spec.Workload.UID = testWL+"-hpc-2", 2, "uid-old-wl"
	deleting := testRequest()
	deleting.Name, deleting.Spec.Attempt = testWL+"-hpc-3", 3
	deleting.Finalizers, deleting.DeletionTimestamp = []string{v1alpha1.AttemptFinalizer}, &metav1.Time{Time: t1}
	var calls []apiCall
	c := fakeClient(t, &calls, testPod("train-0", "0"), testWorkload(), live, stale, deleting)

	reconcilePod(t, c, "train-0")
	pod := getPod(t, c, "train-0")
	if pod.Status.Phase != corev1.PodRunning || pod.Status.HostIP != testHostIP {
		t.Fatalf("phase %s hostIP %q, want running on %s (calls %+v)", pod.Status.Phase, pod.Status.HostIP, testHostIP, calls)
	}
	if len(calls) != 1 || calls[0].verb != "patch status" {
		t.Fatalf("calls %+v, want one status patch", calls)
	}
	reconcilePod(t, c, "train-0")
	if len(calls) != 1 {
		t.Errorf("an unchanged status was written again: %+v", calls[1:])
	}
}

// A pod bound elsewhere is not the node's.
func TestReconcileIgnoresOtherNodes(t *testing.T) {
	pod := testPod("train-0", "0")
	pod.Spec.NodeName = "worker-1"
	var calls []apiCall
	c := fakeClient(t, &calls, pod, testWorkload(), runningRequest())
	reconcilePod(t, c, "train-0")
	if len(calls) != 0 {
		t.Errorf("calls %+v for a pod on another node", calls)
	}
}

func groupPod(name, rank string) *corev1.Pod {
	pod := testPod(name, rank)
	pod.Labels[podconstants.GroupNameLabel] = "group-a"
	return pod
}

// A member of a pod group is marked not retriable before it is shown Failed:
// Kueue would otherwise wait for a replacement no allocation can take.
func TestReconcileFailedGroupMemberNotRetriable(t *testing.T) {
	q := testRequest(cond(v1alpha1.OffloadRequestFinished, v1alpha1.OffloadRequestFailed))
	q.Status.Ranks = []v1alpha1.RankStatus{{Rank: 0, State: v1alpha1.RankFailed, ExitCode: 2}, {Rank: 1, State: v1alpha1.RankFailed}}
	var calls []apiCall
	c := fakeClient(t, &calls, groupPod("train-0", "0"), testWorkload(), q)
	reconcilePod(t, c, "train-0")
	if len(calls) != 2 || calls[0].verb != "patch" || calls[1].verb != "patch status" {
		t.Fatalf("calls %+v, want the annotation patch, then the status", calls)
	}
	pod := getPod(t, c, "train-0")
	if pod.Annotations[podconstants.RetriableInGroupAnnotationKey] != podconstants.RetriableInGroupAnnotationValue {
		t.Errorf("annotations %v", pod.Annotations)
	}
	if pod.Status.Phase != corev1.PodFailed || code(pod.Status.ContainerStatuses[0]) != 2 {
		t.Errorf("phase %s containers %+v, want failed with the member's exit code 2", pod.Status.Phase, pod.Status.ContainerStatuses)
	}
}

// A pod being deleted ends first, as a kubelet ends one whose containers it
// stopped, and is then deleted at once: a finalizer (Kueue's, on a pod group's
// pods) keeps it until it has ended. A pod group member whose job has not
// finished is first marked not retriable.
func TestReconcileDeletedPod(t *testing.T) {
	deleting := func(grace int64) *corev1.Pod {
		pod := groupPod("train-0", "0")
		pod.Finalizers = []string{"kueue.x-k8s.io/managed"}
		pod.DeletionTimestamp, pod.DeletionGracePeriodSeconds = &metav1.Time{Time: t1}, ptr.To(grace)
		return pod
	}
	t.Run("job running", func(t *testing.T) {
		var calls []apiCall
		c := fakeClient(t, &calls, deleting(30), testWorkload(), runningRequest())
		reconcilePod(t, c, "train-0")
		if len(calls) != 3 || calls[0].verb != "patch" || calls[1].verb != "patch status" || calls[2].verb != "delete" {
			t.Fatalf("calls %+v, want the annotation patch, the status, then the delete", calls)
		}
		if g := calls[2].grace; g == nil || *g != 0 {
			t.Errorf("delete grace %v, want 0", g)
		}
		if u := calls[2].uid; u == nil || *u != "uid-train-0" {
			t.Errorf("delete precondition %v, want the pod's UID", u)
		}
		pod := getPod(t, c, "train-0")
		if pod.Status.Phase != corev1.PodFailed || code(pod.Status.ContainerStatuses[0]) != 137 {
			t.Errorf("phase %s containers %+v, want Failed, its container stopped with 137", pod.Status.Phase, pod.Status.ContainerStatuses)
		}
	})
	t.Run("job finished", func(t *testing.T) {
		q := testRequest(cond(v1alpha1.OffloadRequestFinished, v1alpha1.OffloadRequestSucceeded))
		var calls []apiCall
		c := fakeClient(t, &calls, deleting(30), testWorkload(), q)
		reconcilePod(t, c, "train-0")
		if len(calls) != 2 || calls[0].verb != "patch status" || calls[1].verb != "delete" {
			t.Fatalf("calls %+v, want the member's end, then the delete", calls)
		}
		if pod := getPod(t, c, "train-0"); pod.Status.Phase != corev1.PodSucceeded {
			t.Errorf("phase %s, want the member's Succeeded", pod.Status.Phase)
		}
	})
	// Deleted with no grace already, but still running: it ends, with no
	// second delete.
	t.Run("deleted before it ended", func(t *testing.T) {
		pod := deleting(0)
		pod.Annotations[podconstants.RetriableInGroupAnnotationKey] = podconstants.RetriableInGroupAnnotationValue
		var calls []apiCall
		c := fakeClient(t, &calls, pod, testWorkload(), runningRequest())
		reconcilePod(t, c, "train-0")
		if len(calls) != 1 || calls[0].verb != "patch status" {
			t.Fatalf("calls %+v, want only the status", calls)
		}
		if got := getPod(t, c, "train-0"); got.Status.Phase != corev1.PodFailed {
			t.Errorf("phase %s, want Failed", got.Status.Phase)
		}
	})
	t.Run("ended", func(t *testing.T) {
		pod := deleting(30)
		pod.Status.Phase = corev1.PodSucceeded
		var calls []apiCall
		c := fakeClient(t, &calls, pod, testWorkload(), runningRequest())
		reconcilePod(t, c, "train-0")
		if len(calls) != 2 || calls[0].verb != "patch" || calls[1].verb != "delete" {
			t.Fatalf("calls %+v, want the annotation patch, then only the delete", calls)
		}
	})
}
