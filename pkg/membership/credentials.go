package membership

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// the site cannot start because a Secret it references is missing (D10). It
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
