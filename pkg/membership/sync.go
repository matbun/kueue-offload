package membership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"text/template"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
)

// unavailableRecheck bounds the requeue while an agent Deployment is not
// available, so that a missing credential is reported soon.
const unavailableRecheck = time.Minute

// Reconciler synchronises the memberships of one site.
type Reconciler struct {
	// Client reads from the cache and writes to the API server.
	Client client.Client
	// APIReader lists the pods and events of agents that are not available.
	APIReader client.Reader
	Recorder  events.EventRecorder
	// Site is the SiteConfig name.
	Site string
	// Namespace is the controller's own namespace, the only one it reads
	// ConfigMaps from.
	Namespace string
	Now       func() time.Time
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Reconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	after, err := r.Sync(ctx)
	if err != nil {
		syncs.WithLabelValues(r.Site, "error").Inc()
		return ctrl.Result{}, err
	}
	syncs.WithLabelValues(r.Site, "ok").Inc()
	return ctrl.Result{RequeueAfter: after}, nil
}

// Sync runs one pass of the white paper's reconcile for the site and returns
// when the next pass is due.
func (r *Reconciler) Sync(ctx context.Context) (time.Duration, error) {
	logger := log.FromContext(ctx).WithValues("site", r.Site)
	sc := &v1alpha1.SiteConfig{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: r.Site}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("SiteConfig not found")
			return 0, nil
		}
		return 0, err
	}
	if sc.Spec.Membership == nil {
		logger.Info("SiteConfig has no membership block")
		return 0, nil
	}
	s, err := resolveSite(sc)
	if err != nil {
		r.warn(sc, EventSyncFailed, err.Error())
		return 0, err
	}

	text, err := r.readConfigMap(ctx, s.associations)
	if err != nil {
		r.warn(sc, EventSyncFailed, "associations: "+err.Error())
		return 0, fmt.Errorf("associations: %w", err)
	}
	rows, bad := ParseAssociations(text)
	if len(bad) > 0 {
		logger.Info("ignoring malformed association rows", "rows", bad)
	}
	maps := &v1alpha1.IdentityMapList{}
	if err := r.Client.List(ctx, maps); err != nil {
		return 0, err
	}
	var entries []v1alpha1.IdentityEntry
	for _, m := range maps.Items {
		entries = append(entries, m.Spec.Entries...)
	}
	p, err := buildPlan(s, rows, entries, r.Namespace)
	if err != nil {
		r.warn(sc, EventSyncFailed, err.Error())
		return 0, err
	}
	for _, msg := range p.Skipped {
		logger.Info("skipped", "reason", msg)
		r.warn(sc, EventSkipped, msg)
	}

	var errs []error
	var tmpl *template.Template
	if s.template != nil {
		text, err := r.readConfigMap(ctx, *s.template)
		if err == nil {
			tmpl, err = ParseTemplate(text)
		}
		if err != nil {
			r.warn(sc, EventTemplateRefused, "agent template: "+err.Error())
			errs = append(errs, fmt.Errorf("agent template: %w", err))
		}
	}

	if err := r.apply(ctx, r.cohort(s), kueue.GroupVersion.WithKind("Cohort")); err != nil {
		errs = append(errs, fmt.Errorf("cohort %s: %w", s.cohort, err))
	}
	for _, a := range p.Accounts {
		cq, err := r.clusterQueue(ctx, s, a)
		if err == nil {
			err = r.apply(ctx, cq, kueue.GroupVersion.WithKind("ClusterQueue"))
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("ClusterQueue %s: %w", s.queueName(a), err))
		}
	}

	unavailable := false
	memberships := 0
	for _, ns := range sortedTenants(p) {
		t := p.Tenants[ns]
		memberships += len(t.Accounts)
		if err := r.ensureNamespace(ctx, s, t); err != nil {
			errs = append(errs, fmt.Errorf("namespace %s: %w", ns, err))
			continue
		}
		for _, a := range t.Accounts {
			if err := r.apply(ctx, localQueue(s, ns, a), kueue.GroupVersion.WithKind("LocalQueue")); err != nil {
				errs = append(errs, fmt.Errorf("LocalQueue %s/%s: %w", ns, s.queueName(a), err))
			}
		}
		if tmpl != nil {
			if err := r.applyAgent(ctx, sc, s, tmpl, t); err != nil {
				errs = append(errs, err)
			}
		}
		u, err := r.checkCredentials(ctx, s, ns)
		if err != nil {
			errs = append(errs, fmt.Errorf("agent of namespace %s: %w", ns, err))
		}
		unavailable = unavailable || u
	}
	membershipCount.WithLabelValues(r.Site).Set(float64(memberships))

	if err := r.revoke(ctx, s, p); err != nil {
		errs = append(errs, err)
	}

	after := s.interval
	if unavailable && after > unavailableRecheck {
		after = unavailableRecheck
	}
	return after, errors.Join(errs...)
}

func (r *Reconciler) warn(regarding runtime.Object, reason, note string) {
	r.Recorder.Eventf(regarding, nil, corev1.EventTypeWarning, reason, "Sync", "%s", note)
}

// readConfigMap reads one key of a ConfigMap in the controller's namespace.
func (r *Reconciler) readConfigMap(ctx context.Context, ref v1alpha1.ConfigMapKeyRef) (string, error) {
	if ref.Namespace != r.Namespace {
		return "", fmt.Errorf("ConfigMap %s/%s is outside the controller's namespace %s", ref.Namespace, ref.Name, r.Namespace)
	}
	cm := &corev1.ConfigMap{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, cm); err != nil {
		return "", err
	}
	v, ok := cm.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("ConfigMap %s/%s has no key %q", ref.Namespace, ref.Name, ref.Key)
	}
	return v, nil
}

func managedLabels(site string) map[string]string {
	return map[string]string{ManagedByLabel: ManagedByValue, SiteLabel: site}
}

func accountLabels(site, account string) map[string]string {
	l := managedLabels(site)
	l[AccountLabel] = account
	return l
}

func (r *Reconciler) cohort(s site) *kueue.Cohort {
	return &kueue.Cohort{ObjectMeta: metav1.ObjectMeta{Name: s.cohort, Labels: managedLabels(s.name)}}
}

// clusterQueue is the ClusterQueue of one account. Its quota comes from the
// ProjectQuota <site>-<account>, else from the site's default.
func (r *Reconciler) clusterQueue(ctx context.Context, s site, account string) (*kueue.ClusterQueue, error) {
	quota := s.defaultQuota
	pq := &v1alpha1.ProjectQuota{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: s.name + "-" + account}, pq)
	switch {
	case err == nil && pq.Spec.Site == s.name && pq.Spec.Account == account:
		quota = pq.Spec.ResourceGroups
	case err == nil:
		log.FromContext(ctx).Info("ignoring ProjectQuota whose spec does not match its name", "name", pq.Name)
	case !apierrors.IsNotFound(err):
		return nil, err
	}
	return &kueue.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{Name: s.queueName(account), Labels: accountLabels(s.name, account)},
		Spec: kueue.ClusterQueueSpec{
			CohortName: kueue.CohortReference(s.cohort),
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{ProjectLabel(s.name, account): "true"},
			},
			QueueingStrategy: kueue.BestEffortFIFO,
			Preemption: &kueue.ClusterQueuePreemption{
				WithinClusterQueue:  kueue.PreemptionPolicyNever,
				ReclaimWithinCohort: kueue.PreemptionPolicyNever,
			},
			AdmissionChecksStrategy: &kueue.AdmissionChecksStrategy{
				AdmissionChecks: []kueue.AdmissionCheckStrategyRule{{Name: kueue.AdmissionCheckReference(s.check)}},
			},
			ResourceGroups: quota,
		},
	}, nil
}

func localQueue(s site, ns, account string) *kueue.LocalQueue {
	return &kueue.LocalQueue{
		ObjectMeta: metav1.ObjectMeta{Name: s.queueName(account), Namespace: ns, Labels: accountLabels(s.name, account)},
		Spec:       kueue.LocalQueueSpec{ClusterQueue: kueue.ClusterQueueReference(s.queueName(account))},
	}
}

// apply converts a typed object and applies it.
func (r *Reconciler) apply(ctx context.Context, obj client.Object, gvk schema.GroupVersionKind) error {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetGroupVersionKind(gvk)
	unstructured.RemoveNestedField(u.Object, "status")
	unstructured.RemoveNestedField(u.Object, "metadata", "creationTimestamp")
	return r.applyUnstructured(ctx, u)
}

func (r *Reconciler) applyUnstructured(ctx context.Context, u *unstructured.Unstructured) error {
	return r.Client.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldManager), client.ForceOwnership)
}

// ensureNamespace creates the tenant namespace or merges its membership
// labels into it.
func (r *Reconciler) ensureNamespace(ctx context.Context, s site, t *tenant) error {
	ns := &corev1.Namespace{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: t.Namespace}, ns)
	if apierrors.IsNotFound(err) {
		ns = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:        t.Namespace,
			Labels:      map[string]string{ManagedByLabel: ManagedByValue, TenantLabel: "true"},
			Annotations: map[string]string{AccountsAnnotation(s.name): strings.Join(t.Accounts, ",")},
		}}
		for _, a := range t.Accounts {
			ns.Labels[ProjectLabel(s.name, a)] = "true"
		}
		return r.Client.Create(ctx, ns, client.FieldOwner(FieldManager))
	}
	if err != nil {
		return err
	}
	return r.patchMemberships(ctx, s, ns, t.Accounts)
}

// patchMemberships makes the site's membership labels of ns match accounts.
// It removes only labels the accounts annotation says it set, and writes its
// own keys only, so every other label stays.
func (r *Reconciler) patchMemberships(ctx context.Context, s site, ns *corev1.Namespace, accounts []string) error {
	labels := map[string]any{}
	annotations := map[string]any{}
	if len(accounts) > 0 && ns.Labels[TenantLabel] != "true" {
		labels[TenantLabel] = "true"
	}
	for _, a := range accounts {
		if k := ProjectLabel(s.name, a); ns.Labels[k] != "true" {
			labels[k] = "true"
		}
	}
	key := AccountsAnnotation(s.name)
	cur, recorded := ns.Annotations[key]
	for _, a := range splitAccounts(cur) {
		if contains(accounts, a) {
			continue
		}
		if k := ProjectLabel(s.name, a); hasKey(ns.Labels, k) {
			labels[k] = nil
		}
	}
	want := strings.Join(accounts, ",")
	switch {
	case len(accounts) == 0 && recorded:
		annotations[key] = nil
	case len(accounts) > 0 && cur != want:
		annotations[key] = want
	}
	return r.patchNamespace(ctx, ns.Name, labels, annotations)
}

func (r *Reconciler) patchNamespace(ctx context.Context, name string, labels, annotations map[string]any) error {
	md := map[string]any{}
	if len(labels) > 0 {
		md["labels"] = labels
	}
	if len(annotations) > 0 {
		md["annotations"] = annotations
	}
	if len(md) == 0 {
		return nil
	}
	body, err := json.Marshal(map[string]any{"metadata": md})
	if err != nil {
		return err
	}
	target := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	return r.Client.Patch(ctx, target, client.RawPatch(types.MergePatchType, body), client.FieldOwner(FieldManager))
}

// applyAgent renders the agent objects of one tenant and applies them. A
// rendered set with any refused object is not applied at all.
func (r *Reconciler) applyAgent(ctx context.Context, sc *v1alpha1.SiteConfig, s site, tmpl *template.Template, t *tenant) error {
	objs, err := renderObjects(tmpl, TemplateData{
		Namespace:    t.Namespace,
		Site:         s.name,
		HPCUser:      t.HPCUser,
		Accounts:     append([]string(nil), t.Accounts...),
		Image:        s.agentImage,
		AgentService: s.agentService,
		AgentPort:    s.agentPort,
	})
	if err == nil {
		err = r.prepareObjects(objs, s, t.Namespace)
	}
	if err != nil {
		r.warn(sc, EventTemplateRefused, fmt.Sprintf("namespace %s: %v", t.Namespace, err))
		return fmt.Errorf("agent objects of namespace %s: %w", t.Namespace, err)
	}
	var subjects []rbacv1.Subject
	for _, u := range objs {
		if err := r.applyUnstructured(ctx, u); err != nil {
			return fmt.Errorf("applying %s %s/%s: %w", u.GetKind(), t.Namespace, u.GetName(), err)
		}
		if u.GroupVersionKind().GroupKind() == (schema.GroupKind{Kind: "ServiceAccount"}) {
			subjects = append(subjects, rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Name: u.GetName(), Namespace: t.Namespace})
		}
	}
	if !s.tokenReview {
		return nil
	}
	if len(subjects) == 0 {
		log.FromContext(ctx).Info("agentTokenReview is set but the template renders no ServiceAccount", "namespace", t.Namespace)
		return nil
	}
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: tokenReviewBinding(s.name, t.Namespace), Labels: managedLabels(s.name)},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "system:auth-delegator"},
		Subjects:   subjects,
	}
	if err := r.apply(ctx, crb, rbacv1.SchemeGroupVersion.WithKind("ClusterRoleBinding")); err != nil {
		return fmt.Errorf("ClusterRoleBinding %s: %w", crb.Name, err)
	}
	return nil
}

// tokenReviewBinding names the per-tenant system:auth-delegator binding. Dots
// cannot occur in site or namespace names, so names of different tenants
// never collide.
func tokenReviewBinding(site, ns string) string { return "interlink-agent." + site + "." + ns }

// prepareObjects refuses Secrets, cluster-scoped objects and objects in
// another namespace, and labels the rest.
func (r *Reconciler) prepareObjects(objs []*unstructured.Unstructured, s site, ns string) error {
	for _, u := range objs {
		gvk := u.GroupVersionKind()
		id := gvk.Kind + " " + u.GetName()
		if gvk.Group == "" && gvk.Kind == "Secret" {
			return fmt.Errorf("%s: the controller never writes Secrets", id)
		}
		namespaced, err := r.Client.IsObjectNamespaced(u)
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		if !namespaced {
			return fmt.Errorf("%s is cluster scoped", id)
		}
		if n := u.GetNamespace(); n != "" && n != ns {
			return fmt.Errorf("%s is in namespace %s, not %s", id, n, ns)
		}
		u.SetNamespace(ns)
		labels := u.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		for k, v := range managedLabels(s.name) {
			labels[k] = v
		}
		u.SetLabels(labels)
	}
	return nil
}

// revoke removes the memberships that disappeared: the label and the
// LocalQueue. Namespaces and agents stay.
func (r *Reconciler) revoke(ctx context.Context, s site, p plan) error {
	var errs []error
	nss := &corev1.NamespaceList{}
	if err := r.Client.List(ctx, nss); err != nil {
		return err
	}
	for i := range nss.Items {
		ns := &nss.Items[i]
		if _, ok := p.Tenants[ns.Name]; ok {
			continue
		}
		if _, ok := ns.Annotations[AccountsAnnotation(s.name)]; !ok {
			continue
		}
		if err := r.patchMemberships(ctx, s, ns, nil); err != nil {
			errs = append(errs, fmt.Errorf("namespace %s: %w", ns.Name, err))
		}
	}
	lqs := &kueue.LocalQueueList{}
	if err := r.Client.List(ctx, lqs, client.MatchingLabels(managedLabels(s.name))); err != nil {
		return errors.Join(append(errs, err)...)
	}
	for i := range lqs.Items {
		lq := &lqs.Items[i]
		a := lq.Labels[AccountLabel]
		if p.hasMembership(lq.Namespace, a) && lq.Name == s.queueName(a) {
			continue
		}
		if err := r.Client.Delete(ctx, lq); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("LocalQueue %s/%s: %w", lq.Namespace, lq.Name, err))
		}
	}
	return errors.Join(errs...)
}

func sortedTenants(p plan) []string {
	out := make([]string, 0, len(p.Tenants))
	for ns := range p.Tenants {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

func hasKey(m map[string]string, k string) bool {
	_, ok := m[k]
	return ok
}
