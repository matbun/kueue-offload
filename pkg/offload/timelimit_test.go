package offload

import (
	"strconv"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// onDebugFlavor admits the Workload on hpc-a-debug, whose jobs run at most two
// hours, and lets f change the owner Job, the Workload and the owner pods.
func onDebugFlavor(f func(o client.Object)) envOption {
	return func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			switch o := o.(type) {
			case *v1alpha1.OffloadConfig:
				debug := o.Spec.Flavors["hpc-a-debug"]
				debug.MaxExecutionTimeSeconds = ptr.To[int32](7200)
				o.Spec.Flavors["hpc-a-debug"] = debug
			case *kueue.Workload:
				o.Status.Admission.PodSetAssignments[0].Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-debug", "nvidia.com/gpu": "hpc-a-debug"}
			}
			if f != nil {
				f(o)
			}
		}
	}
}

func templateTimeLimit(v string) func(o client.Object) {
	return func(o client.Object) {
		if wl, ok := o.(*kueue.Workload); ok {
			wl.Spec.PodSets[0].Template.Annotations[TimeLimitAnnotation] = v
		}
	}
}

// The annotation of the virtual kubelet sets the wall time when the Workload has
// none: on the pod template, on the owner, or on both, where the template wins;
// Kueue's maximumExecutionTimeSeconds wins over either.
func TestTimeLimitAnnotationSetsTheWallTime(t *testing.T) {
	for _, tt := range []struct {
		name     string
		template string
		owner    string
		kueue    *int32
		want     int32
	}{
		{name: "template", template: "00:05:00", want: 300},
		{name: "owner", owner: "01:00:00", want: 3600},
		{name: "template over owner", template: "00:15:00", owner: "01:00:00", want: 900},
		{name: "rounded up to minutes", template: "00:00:30", want: 30},
		{name: "the queue's limit", template: "02:00:00", want: 7200},
		{name: "kueue over annotation", template: "01:00:00", kueue: ptr.To[int32](600), want: 600},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, onDebugFlavor(func(o client.Object) {
				switch o := o.(type) {
				case *batchv1.Job:
					if tt.owner != "" {
						o.Annotations[TimeLimitAnnotation] = tt.owner
					}
				case *kueue.Workload:
					o.Spec.MaximumExecutionTimeSeconds = tt.kueue
					if tt.template != "" {
						o.Spec.PodSets[0].Template.Annotations[TimeLimitAnnotation] = tt.template
					}
				}
			}))
			e.reconcile()
			sp := e.request(1).Spec
			if got := ptr.Deref(sp.MaximumExecutionTimeSeconds, 0); got != tt.want {
				t.Errorf("maximumExecutionTimeSeconds %d, want %d", got, tt.want)
			}
			minutes := (tt.want + 59) / 60
			if got, want := sp.PodSets[0].Annotations[flagsKey], "--account=proj42 --partition=gpu --qos=debug --time="+strconv.Itoa(int(minutes)); got != want {
				t.Errorf("flags %q, want %q", got, want)
			}
		})
	}
}

// A single pod's PodSet template holds its spec only: its own annotation is the
// owner's.
func TestTimeLimitOfASinglePod(t *testing.T) {
	e := newEnv(t, podOwned(1), onDebugFlavor(func(o client.Object) {
		if p, ok := o.(*corev1.Pod); ok {
			p.Annotations[TimeLimitAnnotation] = "00:20:00"
		}
	}))
	e.reconcile()
	if got := e.request(1).Spec.PodSets[0].Annotations[flagsKey]; !strings.HasSuffix(got, " --time=20") {
		t.Errorf("flags %q, want --time=20", got)
	}
}

// A time the flavor's queue does not allow, or one that is not HH:MM:SS, is
// refused before anything reaches the site.
func TestTimeLimitRefusals(t *testing.T) {
	for _, tt := range []struct {
		name string
		opt  envOption
		want string
	}{
		{"above the queue's limit", onDebugFlavor(templateTimeLimit("02:01:00")),
			"a job runs at most 02:00:00 on queue hpc-a-debug; interlink.eu/time-limit asks for 02:01:00"},
		{"kueue above the queue's limit", onDebugFlavor(func(o client.Object) {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.Spec.MaximumExecutionTimeSeconds = ptr.To[int32](7201)
			}
		}), "a job runs at most 02:00:00 on queue hpc-a-debug; maximumExecutionTimeSeconds asks for 02:00:01"},
		{"minutes only", onDebugFlavor(templateTimeLimit("5:00")),
			`interlink.eu/time-limit "5:00" is not HH:MM:SS, for example 01:30:00`},
		{"days", onDebugFlavor(templateTimeLimit("1-00:00:00")),
			`interlink.eu/time-limit "1-00:00:00" is not HH:MM:SS`},
		{"no time", onDebugFlavor(templateTimeLimit("00:00:00")),
			`interlink.eu/time-limit "00:00:00" asks for no time`},
		{"on the owner", onDebugFlavor(func(o client.Object) {
			if j, ok := o.(*batchv1.Job); ok {
				j.Annotations[TimeLimitAnnotation] = "90m"
			}
		}), `interlink.eu/time-limit "90m" is not HH:MM:SS`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, tt.opt)
			e.reconcile()
			if len(e.requests()) != 0 {
				t.Fatalf("requests %v, want none", e.requests())
			}
			if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, tt.want) {
				t.Errorf("check = %s %q, want Rejected with %q", st.State, st.Message, tt.want)
			}
			if c := jobCondition(e.job(), batchv1.JobFailed); c == nil || !strings.Contains(c.Message, tt.want) {
				t.Errorf("owner Job failed condition %+v, want one with %q", c, tt.want)
			}
		})
	}
}

// A flavor without a limit takes any time, and the limit of one flavor does not
// apply to another.
func TestTimeLimitOnAFlavorWithoutLimit(t *testing.T) {
	e := newEnv(t, withConfig(func(sc *v1alpha1.OffloadConfig) {
		debug := sc.Spec.Flavors["hpc-a-debug"]
		debug.MaxExecutionTimeSeconds = ptr.To[int32](7200)
		sc.Spec.Flavors["hpc-a-debug"] = debug
	}), func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.Spec.PodSets[0].Template.Annotations[TimeLimitAnnotation] = "48:00:00"
			}
		}
	})
	e.reconcile()
	if got := ptr.Deref(e.request(1).Spec.MaximumExecutionTimeSeconds, 0); got != 48*3600 {
		t.Errorf("maximumExecutionTimeSeconds %d, want %d", got, 48*3600)
	}
}

// The job runs in one allocation, so its wall time is the longest any PodSet
// asks for; a PodSet without its own value takes the owner's.
func TestJobWallTimeIsTheLongestPodSet(t *testing.T) {
	wl := workload()
	launcher := kueue.PodSet{Name: "launcher", Count: 1, Template: podTemplate()}
	launcher.Template.Annotations[TimeLimitAnnotation] = "00:10:00"
	workers := kueue.PodSet{Name: "workers", Count: 2, Template: podTemplate()}
	wl.Spec.PodSets = []kueue.PodSet{launcher, workers}

	wt, err := jobWallTime(wl, map[string]string{TimeLimitAnnotation: "00:40:00"})
	if err != nil || wt == nil || wt.seconds != 2400 || wt.from != TimeLimitAnnotation {
		t.Errorf("wall time %+v, %v; want 2400 s from %s", wt, err, TimeLimitAnnotation)
	}
	wt, err = jobWallTime(wl, nil)
	if err != nil || wt == nil || wt.seconds != 600 {
		t.Errorf("without the owner's: wall time %+v, %v; want 600 s", wt, err)
	}
	wt, err = jobWallTime(workload(), nil)
	if err != nil || wt != nil {
		t.Errorf("without any: wall time %+v, %v; want none", wt, err)
	}
	workers.Template.Annotations[TimeLimitAnnotation] = "10:00"
	wl.Spec.PodSets = []kueue.PodSet{launcher, workers}
	if _, err := jobWallTime(wl, nil); err == nil {
		t.Error("a malformed value on the second PodSet was accepted")
	}
}

// An empty value is unset, as on the virtual kubelet: a template's empty value
// takes the owner's, and an empty owner's value asks for nothing.
func TestTimeLimitEmptyIsUnset(t *testing.T) {
	wl := workload()
	wl.Spec.PodSets[0].Template.Annotations[TimeLimitAnnotation] = ""
	wt, err := jobWallTime(wl, map[string]string{TimeLimitAnnotation: "00:40:00"})
	if err != nil || wt == nil || wt.seconds != 2400 {
		t.Errorf("empty on the template: wall time %+v, %v; want the owner's 2400 s", wt, err)
	}
	for _, owner := range []map[string]string{nil, {TimeLimitAnnotation: ""}} {
		if wt, err := jobWallTime(wl, owner); err != nil || wt != nil {
			t.Errorf("owner %v: wall time %+v, %v; want none", owner, wt, err)
		}
	}
}
