package offload

import (
	"bytes"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// forwardedAnnotationPrefixes are the owner annotations the OffloadRequest
// carries in ownerAnnotations, for every PodSet template.
var forwardedAnnotationPrefixes = []string{"slurm-job.vk.io/", "htcondor-job.vk.io/", "interlink.eu/", "offload.interlink.eu/"}

// TimeLimitAnnotation asks for the job's wall time as HH:MM:SS, as on the
// virtual kubelet, on the owner or on a PodSet's pod template; an empty value
// is unset. The Workload's maximumExecutionTimeSeconds (Kueue's label
// kueue.x-k8s.io/max-exec-time-seconds) wins over it.
const TimeLimitAnnotation = "interlink.eu/time-limit"

var timeLimitFormat = regexp.MustCompile(`^([0-9]{1,3}):([0-5][0-9]):([0-5][0-9])$`)

// wallTime is the job's wall time and what asked for it, for refusals.
type wallTime struct {
	seconds int32
	from    string
}

// jobWallTime is the Workload's maximumExecutionTimeSeconds, else the longest
// TimeLimitAnnotation of its PodSets, a template's own value winning over the
// owner's; nil when neither is set. A value that is not HH:MM:SS is an error.
func jobWallTime(wl *kueue.Workload, ownerAnnotations map[string]string) (*wallTime, error) {
	if s := wl.Spec.MaximumExecutionTimeSeconds; s != nil {
		return &wallTime{seconds: *s, from: "maximumExecutionTimeSeconds"}, nil
	}
	var longest *wallTime
	for _, ps := range wl.Spec.PodSets {
		v := ps.Template.Annotations[TimeLimitAnnotation]
		if v == "" {
			v = ownerAnnotations[TimeLimitAnnotation]
		}
		if v == "" {
			continue
		}
		seconds, err := parseTimeLimit(v)
		if err != nil {
			return nil, err
		}
		if longest == nil || seconds > longest.seconds {
			longest = &wallTime{seconds: seconds, from: TimeLimitAnnotation}
		}
	}
	return longest, nil
}

func parseTimeLimit(v string) (int32, error) {
	m := timeLimitFormat.FindStringSubmatch(v)
	if m == nil {
		return 0, fmt.Errorf("%s %q is not HH:MM:SS, for example 01:30:00", TimeLimitAnnotation, v)
	}
	var seconds int32
	for _, part := range m[1:] {
		n, _ := strconv.Atoi(part)
		seconds = seconds*60 + int32(n)
	}
	if seconds == 0 {
		return 0, fmt.Errorf("%s %q asks for no time: ask for HH:MM:SS above 00:00:00", TimeLimitAnnotation, v)
	}
	return seconds, nil
}

func formatSeconds(s int32) string {
	return fmt.Sprintf("%02d:%02d:%02d", s/3600, s%3600/60, s%60)
}

// withinFlavorTime refuses a wall time above the maxExecutionTimeSeconds of a
// flavor admitted for the PodSet: Kueue cannot choose a flavor by time, and the
// queue would hold or refuse the job.
func withinFlavorTime(a *kueue.PodSetAssignment, s site, wt *wallTime) error {
	if wt == nil {
		return nil
	}
	for _, f := range sortedKeys(flavorSet(a)) {
		if limit := s.flavors[f].MaxExecutionTimeSeconds; limit != nil && wt.seconds > *limit {
			return fmt.Errorf("a job runs at most %s on queue %s; %s asks for %s", formatSeconds(*limit), f, wt.from, formatSeconds(wt.seconds))
		}
	}
	return nil
}

// buildPodSets turns the Workload's PodSets that run (runPodSets) into the
// OffloadRequest's, with the flavors and count Kueue admitted and the
// annotations of each PodSet's flavor expanded for account and the job's wall
// time wt, which none of the admitted flavors may limit to less; a Ray owner's
// are prepared for the site's launcher (rayPodSets). An error means the
// Workload can never be submitted.
func buildPodSets(wl *kueue.Workload, s site, account string, wt *wallTime) ([]v1alpha1.PodSet, error) {
	podSets, admitted := runPodSets(wl)
	assignments := map[kueue.PodSetReference]*kueue.PodSetAssignment{}
	if err := oneFlavor(admitted); err != nil {
		return nil, err
	}
	for i := range admitted {
		a := &admitted[i]
		if err := withinFlavorTime(a, s, wt); err != nil {
			return nil, err
		}
		assignments[a.Name] = a
	}
	var seconds int32
	if wt != nil {
		seconds = wt.seconds
	}
	data := newFlavorData(account, seconds)
	out := make([]v1alpha1.PodSet, 0, len(podSets))
	for _, ps := range podSets {
		p := v1alpha1.PodSet{Name: ps.Name, Count: ps.Count, Template: *ps.Template.DeepCopy()}
		if a := assignments[ps.Name]; a != nil {
			if a.Count != nil {
				p.Count = *a.Count
			}
			ann, err := annotationsFor(a, s, data)
			if err != nil {
				return nil, fmt.Errorf("podSet %s: %w", ps.Name, err)
			}
			p.Flavors, p.Annotations = maps.Clone(a.Flavors), ann
		}
		p.SecretRefs, p.ConfigMapRefs = references(&p.Template.Spec)
		out = append(out, p)
	}
	if c := rayOwnerOf(wl); c != nil {
		if err := rayPodSets(c, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// FlavorData is what a flavor's annotation templates are executed with.
type FlavorData struct {
	// Account is the account of the Workload's ClusterQueue.
	Account string
	// MaxExecSeconds is the job's wall time: the Workload's
	// maximumExecutionTimeSeconds, else TimeLimitAnnotation, 0 when neither is
	// set.
	MaxExecSeconds int32
	// MaxExecMinutes is MaxExecSeconds in minutes, rounded up.
	MaxExecMinutes int32
}

func newFlavorData(account string, maxExecSeconds int32) FlavorData {
	return FlavorData{Account: account, MaxExecSeconds: maxExecSeconds, MaxExecMinutes: (maxExecSeconds + 59) / 60}
}

// ExpandAnnotations executes the annotation templates of a flavor with data.
// Missing keys are errors.
func ExpandAnnotations(templates map[string]string, data FlavorData) (map[string]string, error) {
	if len(templates) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(templates))
	for _, k := range slices.Sorted(maps.Keys(templates)) {
		t, err := template.New(k).Option("missingkey=error").Parse(templates[k])
		if err != nil {
			return nil, fmt.Errorf("annotation %s: %w", k, err)
		}
		var buf bytes.Buffer
		if err := t.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("annotation %s: %w", k, err)
		}
		out[k] = buf.String()
	}
	return out, nil
}

// annotationsFor maps the flavors admitted for a PodSet's resources to one batch
// queue: the expanded annotations of the listed flavors, which must agree. A
// PodSet none of whose flavors is listed is refused, since its job would carry
// no account.
func annotationsFor(a *kueue.PodSetAssignment, s site, data FlavorData) (map[string]string, error) {
	var (
		found  bool
		chosen map[string]string
		from   string
	)
	for _, f := range sortedKeys(flavorSet(a)) {
		t, ok := s.flavors[f]
		if !ok {
			continue
		}
		ann, err := ExpandAnnotations(t.Annotations, data)
		if err != nil {
			return nil, fmt.Errorf("flavor %s: %w", f, err)
		}
		if !found {
			found, chosen, from = true, ann, f
			continue
		}
		if !maps.Equal(ann, chosen) {
			return nil, fmt.Errorf("admitted flavors %s and %s map to different queues", from, f)
		}
	}
	if !found {
		return nil, fmt.Errorf("none of the admitted flavors %s is listed in the flavors of OffloadConfig %s", strings.Join(sortedKeys(flavorSet(a)), ", "), s.name)
	}
	return chosen, nil
}

// oneFlavor refuses PodSets admitted on different flavors: the Workload runs in
// one allocation, which takes one queue (kueue-native.md 3.1). A PodSet admitted
// on no flavor is left to annotationsFor.
func oneFlavor(assignments []kueue.PodSetAssignment) error {
	var first *kueue.PodSetAssignment
	for i := range assignments {
		a := &assignments[i]
		if len(a.Flavors) == 0 {
			continue
		}
		if first == nil {
			first = a
			continue
		}
		if !maps.Equal(flavorSet(first), flavorSet(a)) {
			return fmt.Errorf("podSet %s was admitted on %s and podSet %s on %s: the Workload runs in one allocation, so pin one queue on every PodSet",
				first.Name, strings.Join(sortedKeys(flavorSet(first)), ", "), a.Name, strings.Join(sortedKeys(flavorSet(a)), ", "))
		}
	}
	return nil
}

func flavorSet(a *kueue.PodSetAssignment) map[string]bool {
	set := map[string]bool{}
	for _, f := range a.Flavors {
		set[string(f)] = true
	}
	return set
}

// forwardedAnnotations returns the owner's annotations under the forwarded
// prefixes.
func forwardedAnnotations(owner map[string]string) map[string]string {
	var out map[string]string
	for k, v := range owner {
		if !hasForwardedPrefix(k) {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = v
	}
	return out
}

func hasForwardedPrefix(k string) bool {
	for _, p := range forwardedAnnotationPrefixes {
		if strings.HasPrefix(k, p) {
			return true
		}
	}
	return false
}

// references lists every Secret and ConfigMap the pod spec names through
// volumes, envFrom and env valueFrom, sorted and without duplicates.
func references(spec *corev1.PodSpec) (secrets, configMaps []string) {
	sec, cm := map[string]bool{}, map[string]bool{}
	add := func(m map[string]bool, name string) {
		if name != "" {
			m[name] = true
		}
	}
	for _, v := range spec.Volumes {
		if v.Secret != nil {
			add(sec, v.Secret.SecretName)
		}
		if v.ConfigMap != nil {
			add(cm, v.ConfigMap.Name)
		}
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.Secret != nil {
					add(sec, src.Secret.Name)
				}
				if src.ConfigMap != nil {
					add(cm, src.ConfigMap.Name)
				}
			}
		}
	}
	containers := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
	for _, c := range containers {
		for _, ef := range c.EnvFrom {
			if ef.SecretRef != nil {
				add(sec, ef.SecretRef.Name)
			}
			if ef.ConfigMapRef != nil {
				add(cm, ef.ConfigMapRef.Name)
			}
		}
		for _, e := range c.Env {
			if e.ValueFrom == nil {
				continue
			}
			if e.ValueFrom.SecretKeyRef != nil {
				add(sec, e.ValueFrom.SecretKeyRef.Name)
			}
			if e.ValueFrom.ConfigMapKeyRef != nil {
				add(cm, e.ValueFrom.ConfigMapKeyRef.Name)
			}
		}
	}
	return sortedKeys(sec), sortedKeys(cm)
}

func sortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
