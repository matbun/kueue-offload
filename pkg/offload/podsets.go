package offload

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// forwardedAnnotationPrefixes are the owner annotations the OffloadRequest
// carries in ownerAnnotations, for every PodSet template.
var forwardedAnnotationPrefixes = []string{"slurm-job.vk.io/", "htcondor-job.vk.io/", "interlink.eu/", "offload.interlink.eu/"}

// buildPodSets turns the Workload's PodSets into the OffloadRequest's, with the
// flavors and count Kueue admitted and the annotations of each PodSet's flavor
// expanded for account. An error means the Workload can never be submitted.
func buildPodSets(wl *kueue.Workload, s site, account string) ([]v1alpha1.PodSet, error) {
	assignments := map[kueue.PodSetReference]*kueue.PodSetAssignment{}
	if wl.Status.Admission != nil {
		if err := oneFlavor(wl.Status.Admission.PodSetAssignments); err != nil {
			return nil, err
		}
		for i := range wl.Status.Admission.PodSetAssignments {
			a := &wl.Status.Admission.PodSetAssignments[i]
			assignments[a.Name] = a
		}
	}
	data := newFlavorData(account, ptr.Deref(wl.Spec.MaximumExecutionTimeSeconds, 0))
	out := make([]v1alpha1.PodSet, 0, len(wl.Spec.PodSets))
	for _, ps := range wl.Spec.PodSets {
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
	return out, nil
}

// FlavorData is what a flavor's annotation templates are executed with.
type FlavorData struct {
	// Account is the account of the Workload's ClusterQueue.
	Account string
	// MaxExecSeconds is the Workload's maximumExecutionTimeSeconds, 0 when unset.
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
