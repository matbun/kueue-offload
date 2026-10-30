package membership

import (
	"context"
	"fmt"
	"strings"
	"text/template"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

var jobKind = batchv1.SchemeGroupVersion.WithKind("Job")

// offboard takes down the site in a namespace that left it, once nothing of
// the namespace runs at the site. It first deletes the OffloadRequests of its
// finished Workloads, so that the executor cleans up after their jobs, then
// runs the offboard Jobs once, then deletes the objects it made there for the
// site and its own labels and annotations. A failed offboard Job is reported and
// does not stop the rest, as what it would clean up cannot be reached
// otherwise. The Secrets of the namespace stay, so that adding it back restores
// it. It reports whether the namespace is done.
func (r *Reconciler) offboard(ctx context.Context, sc *v1alpha1.OffloadConfig, s site, executor *template.Template, ns *corev1.Namespace) (bool, error) {
	logger := log.FromContext(ctx).WithValues("site", s.name, "namespace", ns.Name)
	running, finished, err := r.siteRequests(ctx, s, ns.Name)
	if err != nil {
		return false, err
	}
	if len(running) > 0 {
		logger.Info("namespace left the site, offboarded once its jobs there end", "requests", running)
		return false, nil
	}
	if len(finished) > 0 {
		var names []string
		for _, q := range finished {
			if err := r.Client.Delete(ctx, q, client.Preconditions{UID: &q.UID}); client.IgnoreNotFound(err) != nil {
				return false, fmt.Errorf("deleting OffloadRequest %s: %w", q.Name, err)
			}
			names = append(names, q.Name)
		}
		logger.Info("namespace left the site, the executor cleans up after its finished jobs", "requests", names)
		return false, nil
	}
	data := TemplateData{
		Namespace:       ns.Name,
		Site:            s.name,
		HPCUser:         ns.Annotations[UserAnnotation(s.name)],
		Accounts:        splitList(ns.Annotations[LeftAnnotation(s.name)]),
		Image:           s.executorImage,
		ExecutorService: s.executorService,
		ExecutorPort:    s.executorPort,
	}
	if data.HPCUser == "" {
		return false, fmt.Errorf("annotation %s is empty", UserAnnotation(s.name))
	}
	if s.offboard != nil && !hasKey(ns.Annotations, OffboardedAnnotation(s.name)) {
		done, err := r.runOffboardJobs(ctx, sc, s, ns, data)
		if err != nil || !done {
			return false, err
		}
		if err := r.patchNamespace(ctx, ns.Name, nil, map[string]any{OffboardedAnnotation(s.name): "true"}); err != nil {
			return false, err
		}
	}

	// The kinds to delete are those the executor template renders for the
	// accounts the namespace had, and Jobs.
	kinds := []schema.GroupVersionKind{jobKind}
	switch {
	case s.template != nil && executor == nil:
		return false, fmt.Errorf("executor template refused: its objects are not deleted")
	case executor != nil:
		objs, err := renderObjects(executor, data)
		if err != nil {
			return false, fmt.Errorf("executor template: %w", err)
		}
		for _, u := range objs {
			if gvk := u.GroupVersionKind(); !containsKind(kinds, gvk) {
				kinds = append(kinds, gvk)
			}
		}
	}
	for _, gvk := range kinds {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		err := r.Client.DeleteAllOf(ctx, u, client.InNamespace(ns.Name), client.MatchingLabels(managedLabels(s.name)),
			client.PropagationPolicy(metav1.DeletePropagationBackground))
		if err != nil && !meta.IsNoMatchError(err) {
			return false, fmt.Errorf("deleting %s objects: %w", gvk.Kind, err)
		}
	}

	labels := map[string]any{}
	for _, k := range splitList(ns.Annotations[LabelsAnnotation(s.name)]) {
		if hasKey(ns.Labels, k) {
			labels[k] = nil
		}
	}
	if hasKey(ns.Labels, TenantLabel) && !memberOfOtherSite(ns, s.name) {
		labels[TenantLabel] = nil
	}
	annotations := map[string]any{LeftAnnotation(s.name): nil, OffboardedAnnotation(s.name): nil}
	for _, k := range []string{UserAnnotation(s.name), LabelsAnnotation(s.name), ExecutorAnnotation(s.name)} {
		if hasKey(ns.Annotations, k) {
			annotations[k] = nil
		}
	}
	if err := r.patchNamespace(ctx, ns.Name, labels, annotations); err != nil {
		return false, err
	}
	logger.Info("offboarded")
	r.Recorder.Eventf(ns, nil, corev1.EventTypeNormal, EventOffboarded, "Offboard",
		"namespace left site %s: the controller's objects of the site are deleted, the namespace's Secrets stay", s.name)
	return true, nil
}

// siteRequests sorts the namespace's OffloadRequests of the site's workers into
// the names of those in use and those of finished Workloads. A request is in
// use while its Workload runs, while its Workload is gone (the garbage
// collector deletes it) and while it is deleted (the executor finalizes it).
func (r *Reconciler) siteRequests(ctx context.Context, s site, ns string) ([]string, []*v1alpha1.OffloadRequest, error) {
	l := &v1alpha1.OffloadRequestList{}
	if err := r.APIReader.List(ctx, l, client.InNamespace(ns)); err != nil {
		return nil, nil, err
	}
	var (
		running  []string
		finished []*v1alpha1.OffloadRequest
	)
	for i := range l.Items {
		q := &l.Items[i]
		if !contains(s.workers, q.Labels[v1alpha1.SiteLabel]) {
			continue
		}
		done := false
		if q.DeletionTimestamp.IsZero() {
			wl := &kueue.Workload{}
			err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ns, Name: q.Spec.Workload.Name}, wl)
			if client.IgnoreNotFound(err) != nil {
				return nil, nil, err
			}
			done = err == nil && wl.UID == q.Spec.Workload.UID && kueueworkload.IsFinished(wl)
		}
		if done {
			finished = append(finished, q)
		} else {
			running = append(running, q.Name)
		}
	}
	return running, finished, nil
}

// runOffboardJobs applies the Jobs of the offboard template and reports whether
// they all ended.
func (r *Reconciler) runOffboardJobs(ctx context.Context, sc *v1alpha1.OffloadConfig, s site, ns *corev1.Namespace, data TemplateData) (bool, error) {
	text, err := r.readConfigMap(ctx, *s.offboard)
	var objs []*unstructured.Unstructured
	if err == nil {
		objs, err = renderOffboard(text, data)
	}
	if err == nil {
		err = r.prepareObjects(objs, s, data.Namespace)
	}
	if err != nil {
		r.warn(sc, EventTemplateRefused, fmt.Sprintf("namespace %s: offboard template: %v", data.Namespace, err))
		return false, fmt.Errorf("offboard template: %w", err)
	}
	done := true
	for _, u := range objs {
		labels := u.GetLabels()
		labels[OffboardLabel] = "true"
		u.SetLabels(labels)
		if err := r.applyUnstructured(ctx, u); err != nil {
			return false, fmt.Errorf("applying Job %s/%s: %w", data.Namespace, u.GetName(), err)
		}
		job := &batchv1.Job{}
		if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: data.Namespace, Name: u.GetName()}, job); err != nil {
			return false, err
		}
		switch c := jobEnd(job); {
		case c == nil:
			done = false
		case c.Type == batchv1.JobFailed:
			r.Recorder.Eventf(ns, nil, corev1.EventTypeWarning, EventOffboardFailed, "Offboard", "offboard Job %s of site %s failed (%s: %s): what it cleans up is left",
				job.Name, s.name, c.Reason, c.Message)
		}
	}
	return done, nil
}

// renderOffboard renders the offboard template, which may hold Jobs only.
func renderOffboard(text string, data TemplateData) ([]*unstructured.Unstructured, error) {
	t, err := ParseTemplate(text)
	if err != nil {
		return nil, err
	}
	objs, err := renderObjects(t, data)
	if err != nil {
		return nil, err
	}
	for _, u := range objs {
		if u.GroupVersionKind() != jobKind {
			return nil, fmt.Errorf("%s %s: the offboard template holds batch/v1 Jobs only", u.GetKind(), u.GetName())
		}
	}
	return objs, nil
}

// jobEnd is the condition that ended the Job, nil while it runs.
func jobEnd(job *batchv1.Job) *batchv1.JobCondition {
	for i, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return &job.Status.Conditions[i]
		}
	}
	return nil
}

// deleteOffboardJobs deletes the site's offboard Jobs in ns.
func (r *Reconciler) deleteOffboardJobs(ctx context.Context, s site, ns string) error {
	labels := managedLabels(s.name)
	labels[OffboardLabel] = "true"
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(jobKind)
	return r.Client.DeleteAllOf(ctx, u, client.InNamespace(ns), client.MatchingLabels(labels),
		client.PropagationPolicy(metav1.DeletePropagationBackground))
}

// memberOfOtherSite reports whether ns holds a membership of a site other than
// site.
func memberOfOtherSite(ns *corev1.Namespace, site string) bool {
	for k := range ns.Annotations {
		if k != AccountsAnnotation(site) && strings.HasPrefix(k, labelPrefix) && strings.HasSuffix(k, "-accounts") {
			return true
		}
	}
	return false
}

func containsKind(list []schema.GroupVersionKind, gvk schema.GroupVersionKind) bool {
	for _, x := range list {
		if x == gvk {
			return true
		}
	}
	return false
}
