package membership

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// recentEvent is how old a FailedMount event may be and still count. The
// kubelet repeats it on every mount retry, so an older one is resolved.
const recentEvent = 10 * time.Minute

var (
	secretNotFound = regexp.MustCompile(`secret "([^"]+)" not found`)
	keyNotFound    = regexp.MustCompile(`couldn't find key (\S+) in Secret (\S+)`)
)

// credentialProblem extracts a missing Secret or Secret key from a kubelet
// message.
func credentialProblem(msg string) string {
	if m := secretNotFound.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("secret %q not found", m[1])
	}
	if m := keyNotFound.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("key %s missing in Secret %s", m[1], m[2])
	}
	return ""
}

func deploymentAvailable(d *appsv1.Deployment) bool {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// checkCredentials records on the namespace whether an executor Deployment of
// the site cannot start because a Secret it references is missing. It
// reads Deployments, their pods and the pods' events, never a Secret. It
// reports whether any executor Deployment is not available.
func (r *Reconciler) checkCredentials(ctx context.Context, s site, nsName string) (bool, error) {
	list := &appsv1.DeploymentList{}
	if err := r.Client.List(ctx, list, client.InNamespace(nsName), client.MatchingLabels(managedLabels(s.name))); err != nil {
		return false, err
	}
	unavailable := false
	var missing []string
	for i := range list.Items {
		d := &list.Items[i]
		if deploymentAvailable(d) {
			continue
		}
		unavailable = true
		m, err := r.missingSecrets(ctx, d)
		if err != nil {
			return unavailable, err
		}
		for _, p := range m {
			missing = append(missing, fmt.Sprintf("Deployment %s: %s", d.Name, p))
		}
	}

	ns := &corev1.Namespace{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: nsName}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return unavailable, nil
		}
		return unavailable, err
	}
	key := ExecutorAnnotation(s.name)
	_, has := ns.Annotations[key]
	switch {
	case len(missing) > 0 && ns.Annotations[key] != MissingCredentials:
		if err := r.patchNamespace(ctx, nsName, nil, map[string]any{key: MissingCredentials}); err != nil {
			return unavailable, err
		}
		r.Recorder.Eventf(ns, nil, corev1.EventTypeWarning, EventMissingCredentials, "CheckExecutor",
			"executor of site %s cannot start: %s", s.name, strings.Join(missing, "; "))
	case len(missing) == 0 && has:
		if err := r.patchNamespace(ctx, nsName, nil, map[string]any{key: nil}); err != nil {
			return unavailable, err
		}
		r.Recorder.Eventf(ns, nil, corev1.EventTypeNormal, EventCredentialsPresent, "CheckExecutor",
			"executor of site %s no longer misses credentials", s.name)
	}
	return unavailable, nil
}

// missingSecrets lists the Secrets the pods of d wait for: a container stuck
// in CreateContainerConfigError, or a recent FailedMount event.
func (r *Reconciler) missingSecrets(ctx context.Context, d *appsv1.Deployment) ([]string, error) {
	sel, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.InNamespace(d.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	add := func(p string) {
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	names := map[string]bool{}
	for _, p := range pods.Items {
		names[p.Name] = true
		statuses := append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...)
		for _, st := range statuses {
			if w := st.State.Waiting; w != nil && w.Reason == "CreateContainerConfigError" {
				add(credentialProblem(w.Message))
			}
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	evs := &corev1.EventList{}
	if err := r.APIReader.List(ctx, evs, client.InNamespace(d.Namespace)); err != nil {
		return nil, err
	}
	cutoff := r.now().Add(-recentEvent)
	for _, ev := range evs.Items {
		if ev.InvolvedObject.Kind != "Pod" || !names[ev.InvolvedObject.Name] || ev.Reason != "FailedMount" {
			continue
		}
		if eventTime(ev).Before(cutoff) {
			continue
		}
		add(credentialProblem(ev.Message))
	}
	sort.Strings(out)
	return out, nil
}

// eventTime is when an event was last seen.
func eventTime(ev corev1.Event) time.Time {
	t := ev.FirstTimestamp.Time
	if ev.LastTimestamp.After(t) {
		t = ev.LastTimestamp.Time
	}
	if ev.EventTime.After(t) {
		t = ev.EventTime.Time
	}
	if ev.Series != nil && ev.Series.LastObservedTime.After(t) {
		t = ev.Series.LastObservedTime.Time
	}
	return t
}

// stampSecretVersions sets SecretVersionsAnnotation on the pod templates of
// objs, from the metadata of the Secrets they reference in ns: never their
// data. A missing Secret keeps the version the live object carries, so that a
// running executor stays as it is until the Secret is back, and is left out of
// an object that has none, so that its creation rolls the pods.
func (r *Reconciler) stampSecretVersions(ctx context.Context, objs []*unstructured.Unstructured, ns string) error {
	versions := map[string]string{}
	for _, u := range objs {
		path := podTemplatePath(u.GroupVersionKind().GroupKind())
		if path == nil {
			continue
		}
		m, found, err := unstructured.NestedMap(u.Object, path...)
		if err != nil || !found {
			continue
		}
		pt := &corev1.PodTemplateSpec{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, pt); err != nil {
			return fmt.Errorf("%s %s: %w", u.GetKind(), u.GetName(), err)
		}
		var (
			stamp []string
			live  map[string]string
		)
		for _, name := range secretNames(&pt.Spec) {
			v, ok := versions[name]
			if !ok {
				if v, err = r.secretVersion(ctx, ns, name); err != nil {
					return fmt.Errorf("Secret %s/%s: %w", ns, name, err)
				}
				versions[name] = v
			}
			if v == "" {
				if live == nil {
					if live, err = r.liveSecretVersions(ctx, u, path, ns); err != nil {
						return fmt.Errorf("%s %s: %w", u.GetKind(), u.GetName(), err)
					}
				}
				v = live[name]
			}
			if v != "" {
				stamp = append(stamp, name+"="+v)
			}
		}
		if len(stamp) == 0 {
			continue
		}
		annPath := append(append([]string(nil), path...), "metadata", "annotations")
		ann, _, err := unstructured.NestedStringMap(u.Object, annPath...)
		if err != nil {
			return fmt.Errorf("%s %s: %w", u.GetKind(), u.GetName(), err)
		}
		if ann == nil {
			ann = map[string]string{}
		}
		ann[SecretVersionsAnnotation] = strings.Join(stamp, ",")
		if err := unstructured.SetNestedStringMap(u.Object, ann, annPath...); err != nil {
			return err
		}
	}
	return nil
}

// liveSecretVersions reads SecretVersionsAnnotation from the pod template of
// the object in ns that u renders, empty when that object does not exist.
func (r *Reconciler) liveSecretVersions(ctx context.Context, u *unstructured.Unstructured, path []string, ns string) (map[string]string, error) {
	cur := &unstructured.Unstructured{}
	cur.SetGroupVersionKind(u.GroupVersionKind())
	out := map[string]string{}
	err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ns, Name: u.GetName()}, cur)
	if apierrors.IsNotFound(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	ann, _, err := unstructured.NestedStringMap(cur.Object, append(append([]string(nil), path...), "metadata", "annotations")...)
	if err != nil {
		return nil, err
	}
	for _, kv := range splitList(ann[SecretVersionsAnnotation]) {
		if name, v, ok := strings.Cut(kv, "="); ok {
			out[name] = v
		}
	}
	return out, nil
}

// secretVersion is the resourceVersion of a Secret, empty when it does not
// exist. It reads the Secret's metadata only.
func (r *Reconciler) secretVersion(ctx context.Context, ns, name string) (string, error) {
	m := &metav1.PartialObjectMetadata{}
	m.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, m)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	return m.ResourceVersion, err
}

// podTemplatePath is where an object of kind gk holds its pod template, nil
// for a kind without one.
func podTemplatePath(gk schema.GroupKind) []string {
	switch gk {
	case schema.GroupKind{Group: appsv1.GroupName, Kind: "Deployment"}, schema.GroupKind{Group: appsv1.GroupName, Kind: "StatefulSet"},
		schema.GroupKind{Group: appsv1.GroupName, Kind: "DaemonSet"}, schema.GroupKind{Group: appsv1.GroupName, Kind: "ReplicaSet"},
		schema.GroupKind{Group: batchv1.GroupName, Kind: "Job"}:
		return []string{"spec", "template"}
	case schema.GroupKind{Group: batchv1.GroupName, Kind: "CronJob"}:
		return []string{"spec", "jobTemplate", "spec", "template"}
	}
	return nil
}

// secretNames lists, sorted, the Secrets a pod spec mounts (secret and
// projected volumes) or reads into its containers' environment.
func secretNames(spec *corev1.PodSpec) []string {
	seen := map[string]bool{}
	add := func(name string) {
		if name != "" {
			seen[name] = true
		}
	}
	for _, v := range spec.Volumes {
		if v.Secret != nil {
			add(v.Secret.SecretName)
		}
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.Secret != nil {
					add(src.Secret.Name)
				}
			}
		}
	}
	for _, c := range append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...) {
		for _, ef := range c.EnvFrom {
			if ef.SecretRef != nil {
				add(ef.SecretRef.Name)
			}
		}
		for _, e := range c.Env {
			if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
				add(e.ValueFrom.SecretKeyRef.Name)
			}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
