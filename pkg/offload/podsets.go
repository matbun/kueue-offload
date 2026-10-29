package offload

import (
	"fmt"
	"github.com/matbun/kueue-offload/api/v1alpha1"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// forwardedAnnotationPrefixes are the owner annotations merged into every
// PodSet template (design D5).
var forwardedAnnotationPrefixes = []string{"slurm-job.vk.io/", "interlink.eu/"}

// buildPodSets turns the Workload's PodSets into the executor's form, with the
// owner's annotations merged in. An error means the Workload can never be
// submitted.
func buildPodSets(wl *kueue.Workload, ownerAnnotations map[string]string, s site) ([]executorapi.PodSet, error) {
	assignments := map[kueue.PodSetReference]*kueue.PodSetAssignment{}
	if wl.Status.Admission != nil {
		for i := range wl.Status.Admission.PodSetAssignments {
			a := &wl.Status.Admission.PodSetAssignments[i]
			assignments[a.Name] = a
		}
	}
	out := make([]executorapi.PodSet, 0, len(wl.Spec.PodSets))
	for _, ps := range wl.Spec.PodSets {
		count := ps.Count
		var target v1alpha1.FlavorTarget
		if a := assignments[ps.Name]; a != nil {
			if a.Count != nil {
				count = *a.Count
			}
			t, err := targetFor(a, s)
			if err != nil {
				return nil, fmt.Errorf("podSet %s: %w", ps.Name, err)
			}
			target = t
		}
		tmpl := ps.Template.DeepCopy()
		mergeOwnerAnnotations(tmpl, ownerAnnotations)
		secrets, configMaps := references(&tmpl.Spec)
		out = append(out, executorapi.PodSet{
			Name:          string(ps.Name),
			Count:         count,
			Partition:     target.Partition,
			QOS:           target.QOS,
			Template:      *tmpl,
			SecretRefs:    secrets,
			ConfigMapRefs: configMaps,
		})
	}
	return out, nil
}

// targetFor maps the flavors admitted for a PodSet's resources to one Slurm queue.
func targetFor(a *kueue.PodSetAssignment, s site) (v1alpha1.FlavorTarget, error) {
	targets := map[v1alpha1.FlavorTarget]bool{}
	for _, f := range a.Flavors {
		if t, ok := s.flavors[string(f)]; ok && t != (v1alpha1.FlavorTarget{}) {
			targets[t] = true
		}
	}
	switch len(targets) {
	case 0:
		return v1alpha1.FlavorTarget{}, nil
	case 1:
		for t := range targets {
			return t, nil
		}
	}
	names := map[string]bool{}
	for t := range targets {
		names["partition "+t.Partition+" qos "+t.QOS] = true
	}
	return v1alpha1.FlavorTarget{}, fmt.Errorf("admitted flavors map to more than one Slurm queue: %s", strings.Join(sortedKeys(names), ", "))
}

func mergeOwnerAnnotations(tmpl *corev1.PodTemplateSpec, ownerAnnotations map[string]string) {
	for k, v := range ownerAnnotations {
		if !hasForwardedPrefix(k) {
			continue
		}
		if _, ok := tmpl.Annotations[k]; ok {
			continue
		}
		if tmpl.Annotations == nil {
			tmpl.Annotations = map[string]string{}
		}
		tmpl.Annotations[k] = v
	}
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
