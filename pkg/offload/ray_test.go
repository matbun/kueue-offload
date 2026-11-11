package offload

import (
	"reflect"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// rayPodSetsOf are the PodSets Kueue builds for a Ray owner: the head, one
// worker group of workers replicas and, for a RayJob in K8sJobMode, the
// submitter, each admitted on flavor.
func rayPodSetsOf(workers int32, submitter bool, flavor kueue.ResourceFlavorReference) ([]kueue.PodSet, []kueue.PodSetAssignment) {
	head := podTemplate()
	head.Spec.Containers[0].Name = "ray-head"
	worker := podTemplate()
	worker.Spec.Containers[0].Name = "ray-worker"
	podSets := []kueue.PodSet{{Name: rayHeadPodSet, Count: 1, Template: head}, {Name: "gpu", Count: workers, Template: worker}}
	if submitter {
		sub := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-job-submitter", Image: "ghcr.io/example/train:1.4"}}}}
		podSets = append(podSets, kueue.PodSet{Name: raySubmitterPodSet, Count: 1, Template: sub})
	}
	var assignments []kueue.PodSetAssignment
	for _, ps := range podSets {
		f := flavor
		if ps.Name == raySubmitterPodSet {
			// The submitter asks for CPU only, which Kueue may admit elsewhere.
			f = "hpc-a-cpu"
		}
		assignments = append(assignments, kueue.PodSetAssignment{Name: ps.Name, Count: ptr.To(ps.Count),
			Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": f, "nvidia.com/gpu": f}})
	}
	return podSets, assignments
}

// rayOwned makes the Workload a Ray owner's, stored unstructured as the
// offload controller reads it, with the PodSets Kueue builds for it.
func rayOwned(k ownerKind, workers int32, owner func(u *unstructured.Unstructured)) envOption {
	return func(objs *[]client.Object, funcs *interceptor.Funcs) {
		customOwned(k)(objs, funcs)
		for _, o := range *objs {
			switch o := o.(type) {
			case *unstructured.Unstructured:
				if owner != nil {
					owner(o)
				}
			case *kueue.Workload:
				o.Spec.PodSets, o.Status.Admission.PodSetAssignments = rayPodSetsOf(workers, k == rayJobKind, "hpc-a-gpu")
			}
		}
	}
}

func rayWorkload(k ownerKind, workers int32, submitter bool) *kueue.Workload {
	wl := workload()
	wl.OwnerReferences = []metav1.OwnerReference{{APIVersion: k.gvk.GroupVersion().String(), Kind: k.gvk.Kind,
		Name: "train", UID: customUID, Controller: ptr.To(true)}}
	wl.Spec.PodSets, wl.Status.Admission.PodSetAssignments = rayPodSetsOf(workers, submitter, "hpc-a-gpu")
	return wl
}

func podSetNames(podSets []v1alpha1.PodSet) []string {
	var out []string
	for _, ps := range podSets {
		out = append(out, string(ps.Name))
	}
	return out
}

func portNumbers(c corev1.Container) []int32 {
	var out []int32
	for _, p := range c.Ports {
		out = append(out, p.ContainerPort)
	}
	return out
}

// TestRayJobPodSets: the submitter, admitted on another flavor, does not run;
// head and workers name the ray launcher, and the head declares the dashboard
// and client ports once, as a RayCluster's: KubeRay reaches it through the
// shadow.
func TestRayJobPodSets(t *testing.T) {
	wl := rayWorkload(rayJobKind, 2, true)
	wl.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "dash", ContainerPort: 8265}}
	podSets, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := podSetNames(podSets); !slices.Equal(got, []string{"head", "gpu"}) {
		t.Fatalf("podSets %v, want head and gpu", got)
	}
	for _, ps := range podSets {
		if got := ps.Template.Annotations[launcherAnnotation]; got != RayClusterLauncher {
			t.Errorf("podSet %s launcher %q", ps.Name, got)
		}
		if got := ps.Annotations[flagsKey]; got != "--account=proj42 --partition=gpu" {
			t.Errorf("podSet %s flags %q", ps.Name, got)
		}
		if ps.Template.Labels != nil {
			t.Errorf("podSet %s labels %v", ps.Name, ps.Template.Labels)
		}
	}
	if got := portNumbers(podSets[0].Template.Spec.Containers[0]); !slices.Equal(got, []int32{8265, 10001}) {
		t.Errorf("head ports %v", got)
	}
	if len(podSets[1].Template.Spec.Containers[0].Ports) != 0 {
		t.Errorf("worker ports %v", podSets[1].Template.Spec.Containers[0].Ports)
	}
	if len(wl.Spec.PodSets) != 3 || wl.Spec.PodSets[0].Template.Annotations[launcherAnnotation] != "" {
		t.Errorf("the Workload was changed")
	}
}

// TestRayClusterPodSets: the head keeps its labels, KubeRay's pod has its own,
// and declares the dashboard and client ports once; every PodSet names the ray
// launcher, unless its template names another.
func TestRayClusterPodSets(t *testing.T) {
	wl := rayWorkload(rayClusterKind, 1, false)
	wl.Spec.PodSets[0].Template.Labels = map[string]string{"team": "ml"}
	wl.Spec.PodSets[1].Template.Annotations[launcherAnnotation] = "ray-ib"
	podSets, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := podSets[0].Template.Labels; !reflect.DeepEqual(got, map[string]string{"team": "ml"}) {
		t.Errorf("head labels %v, want the template's", got)
	}
	if got := podSets[0].Template.Annotations[launcherAnnotation]; got != RayClusterLauncher {
		t.Errorf("head launcher %q", got)
	}
	if got := podSets[1].Template.Annotations[launcherAnnotation]; got != "ray-ib" {
		t.Errorf("worker launcher %q, want the template's", got)
	}
	if got := portNumbers(podSets[0].Template.Spec.Containers[0]); !slices.Equal(got, []int32{8265, 10001}) {
		t.Errorf("head ports %v", got)
	}
}

// TestRayHeadAlone: a Ray owner whose worker groups have no replicas, or that
// has none, runs its head alone, through the launcher; a worker group of no
// replicas is not sent, and the executor sees the same PodSets.
func TestRayHeadAlone(t *testing.T) {
	for _, k := range []ownerKind{rayJobKind, rayClusterKind} {
		for name, groups := range map[string]bool{"worker group of 0": true, "no worker group": false} {
			t.Run(k.gvk.Kind+" "+name, func(t *testing.T) {
				wl := rayWorkload(k, 0, k == rayJobKind)
				if !groups {
					wl.Spec.PodSets = slices.DeleteFunc(wl.Spec.PodSets, func(ps kueue.PodSet) bool { return ps.Count == 0 })
					wl.Status.Admission.PodSetAssignments = slices.DeleteFunc(wl.Status.Admission.PodSetAssignments,
						func(a kueue.PodSetAssignment) bool { return ptr.Deref(a.Count, 1) == 0 })
				}
				podSets, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
				if err != nil {
					t.Fatal(err)
				}
				if got := podSetNames(podSets); !slices.Equal(got, []string{string(rayHeadPodSet)}) || podSets[0].Count != 1 {
					t.Fatalf("PodSets %v, want the head alone", got)
				}
				if got := podSets[0].Template.Annotations[launcherAnnotation]; got == "" {
					t.Errorf("the head names no launcher")
				}
				got, admitted := RequestPodSets(wl)
				if len(got) != 1 || len(admitted) != 1 || got[0].Name != rayHeadPodSet || admitted[0].Name != rayHeadPodSet {
					t.Errorf("the executor checks against %d PodSets and %d assignments, want the head's", len(got), len(admitted))
				}
			})
		}
	}
}

// The podSetUpdates of a RayJob leave out the submitter, which runs in the
// cluster with the default scheduler.
func TestRayJobPodSetUpdates(t *testing.T) {
	var got []string
	for _, u := range podSetUpdates(rayWorkload(rayJobKind, 2, true), testNode) {
		got = append(got, string(u.Name))
	}
	if !slices.Equal(got, []string{"head", "gpu"}) {
		t.Errorf("podSetUpdates for %v, want head and gpu", got)
	}
}

// TestRayOwnerRunEnds: a Ray owner's Workload is deactivated when its job ends
// at the site: KubeRay would run on an allocation that is gone. One admitted
// keeps its check Ready, one never admitted Pending. A refused one is
// Rejected and left to Kueue, which deactivates it: KubeRay would make a failed
// head again.
func TestRayOwnerRunEnds(t *testing.T) {
	for _, k := range []ownerKind{rayJobKind, rayClusterKind} {
		owner := rayOwned(k, 1, func(u *unstructured.Unstructured) {
			u.Object["spec"] = map[string]any{"suspend": true, "entrypoint": "python x.py"}
		})
		for _, admitted := range []bool{true, false} {
			t.Run(k.gvk.Kind, func(t *testing.T) {
				e := newEnv(t, owner)
				e.submitted()
				if got := podSetNames(e.request(1).Spec.PodSets); !slices.Equal(got, []string{"head", "gpu"}) {
					t.Errorf("request podSets %v", got)
				}
				want := kueue.CheckStatePending
				if admitted {
					e.report(1, e.startedAt(e.now))
					e.reconcile()
					want = kueue.CheckStateReady
				}
				e.report(1, e.finished(false, "DUE TO TIME LIMIT"))
				e.reconcile()
				if st := e.check("hpc-a"); st.State != want || st.Message != "job 123 failed: DUE TO TIME LIMIT" {
					t.Errorf("check %s %q, want %s", st.State, st.Message, want)
				}
				if wl := e.workload(); ptr.Deref(wl.Spec.Active, true) {
					t.Errorf("Workload still active after its job ended")
				}
				if _, ok := e.custom(k).Object["status"]; ok {
					t.Errorf("owner status written")
				}
			})
		}
		t.Run(k.gvk.Kind+" refused", func(t *testing.T) {
			e := newEnv(t, owner)
			e.reconcile()
			e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
				e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", "invalid account")
			})
			e.reconcile()
			if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected || st.Message != "invalid account" {
				t.Errorf("check %s %q, want Rejected", st.State, st.Message)
			}
			if wl := e.workload(); !ptr.Deref(wl.Spec.Active, true) || e.failed() != "" {
				t.Errorf("a rejected Ray owner's Workload deactivated %v or failed %q by the offload controller", !ptr.Deref(wl.Spec.Active, true), e.failed())
			}
		})
	}
}

// TestRequestPodSets: what the executor checks a request against is what the
// controller sends, for every owner kind: names, counts and templates.
func TestRequestPodSets(t *testing.T) {
	for _, wl := range []*kueue.Workload{workload(), rayWorkload(rayClusterKind, 2, false), rayWorkload(rayJobKind, 1, true)} {
		sent, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
		if err != nil {
			t.Fatal(err)
		}
		got, admitted := RequestPodSets(wl)
		if len(got) != len(sent) || len(admitted) != len(sent) {
			t.Fatalf("%d PodSets and %d assignments, the controller sends %d", len(got), len(admitted), len(sent))
		}
		for i := range sent {
			if got[i].Name != sent[i].Name || admitted[i].Name != sent[i].Name || !equality.Semantic.DeepEqual(got[i].Template, sent[i].Template) {
				t.Fatalf("PodSet %d: %s %s, the controller sends %s with another template", i, got[i].Name, admitted[i].Name, sent[i].Name)
			}
		}
	}
}
