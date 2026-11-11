// Package membership keeps Kueue objects, namespace labels and executors in step
// with the Slurm associations of one site.
package membership

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// FieldManager owns every object and label the controller writes.
const FieldManager = "kueue-offload-membership"

// Labels and annotations the controller writes.
const (
	labelPrefix = "hpc.interlink.eu/"

	// ManagedByLabel marks every object the controller applies.
	ManagedByLabel = labelPrefix + "managed-by"
	ManagedByValue = FieldManager
	// TenantLabel marks a namespace that holds at least one membership.
	TenantLabel = labelPrefix + "tenant"
	// SiteLabel and AccountLabel say which membership an object belongs to. On
	// a ClusterQueue, AccountLabel names the account the offload controller
	// charges.
	SiteLabel    = labelPrefix + "site"
	AccountLabel = v1alpha1.AccountLabel
	// QueueLabel names, on the ClusterQueues and LocalQueues of one of
	// membership.queues, that queue. The queue of every flavor has none.
	QueueLabel = labelPrefix + "queue"
	// AssociationsLabel marks a ConfigMap of the controller's namespace whose
	// key AssociationsKey holds more association rows of the site it names.
	AssociationsLabel = labelPrefix + "associations"
	AssociationsKey   = "associations"
	// OffboardLabel marks the Jobs of the offboard template.
	OffboardLabel = labelPrefix + "offboard"

	// SecretVersionsAnnotation on a rendered pod template lists the
	// resourceVersion of each Secret the template mounts or reads into its
	// environment, so that a replaced Secret rolls a Deployment and replaces a
	// Job. A Secret that is missing keeps the version the live object
	// carries.
	SecretVersionsAnnotation = labelPrefix + "secret-versions"

	// MissingCredentials is the value of the executor annotation when a Secret
	// the executor references is missing.
	MissingCredentials = "missing credentials"
)

// ProjectLabel is the namespace label that grants the namespace a project.
func ProjectLabel(site, account string) string { return labelPrefix + site + "-" + account }

// AccountsAnnotation records on a namespace the accounts of a site the
// controller granted, so that it removes only labels it set.
func AccountsAnnotation(site string) string { return labelPrefix + site + "-accounts" }

// ExecutorAnnotation reports on a namespace that the site's executor cannot start.
func ExecutorAnnotation(site string) string { return labelPrefix + site + "-executor" }

// UserAnnotation records on a namespace the HPC user of the site's executor.
// It stays after the namespace left the site, until it is offboarded.
func UserAnnotation(site string) string { return labelPrefix + site + "-user" }

// LeftAnnotation records on a namespace that left the site the accounts it had
// there, until it is offboarded.
func LeftAnnotation(site string) string { return labelPrefix + site + "-left" }

// OffboardedAnnotation records on a namespace that left the site that its
// offboard Jobs ended, so that they run once even when deleting its objects
// takes more than one pass.
func OffboardedAnnotation(site string) string { return labelPrefix + site + "-offboarded" }

// LabelsAnnotation records on a namespace the keys of the site's
// namespaceLabels the controller added, so that it removes only those.
func LabelsAnnotation(site string) string { return labelPrefix + site + "-labels" }

// Event reasons.
const (
	EventSkipped            = "MembershipSkipped"
	EventTemplateRefused    = "TemplateRefused"
	EventSyncFailed         = "SyncFailed"
	EventMissingCredentials = "MissingCredentials"
	EventCredentialsPresent = "CredentialsPresent"
	EventOffboardFailed     = "OffboardFailed"
	EventOffboarded         = "Offboarded"
)

// site is a OffloadConfig with its membership defaults applied.
type site struct {
	name            string
	slurmCluster    string
	cohort          string
	check           string
	queuePrefix     string
	interval        time.Duration
	executorService string
	executorPort    int32
	executorImage   string
	associations    v1alpha1.ConfigMapKeyRef
	template        *v1alpha1.ConfigMapKeyRef
	offboard        *v1alpha1.ConfigMapKeyRef
	namespaceLabels map[string]string
	// workers label the site's OffloadRequests.
	workers      []string
	defaultQuota []kueue.ResourceGroup
	queues       []v1alpha1.MembershipQueue
	fairSharing  bool
}

// reservedQueues are queue names that would make a project's names ambiguous:
// <site>-<account>-project is its Cohort.
var reservedQueues = []string{"all", "project"}

func resolveSite(sc *v1alpha1.OffloadConfig) (site, error) {
	m := sc.Spec.Membership
	if m == nil {
		return site{}, fmt.Errorf("OffloadConfig %s has no membership block", sc.Name)
	}
	s := site{
		name:            sc.Name,
		slurmCluster:    m.SlurmCluster,
		cohort:          m.Cohort,
		check:           m.AdmissionCheck,
		queuePrefix:     sc.Name + "-",
		interval:        15 * time.Minute,
		executorService: sc.Spec.ExecutorService,
		executorPort:    sc.Spec.ExecutorPort,
		executorImage:   m.ExecutorImage,
		associations:    m.Associations,
		template:        m.ExecutorTemplate,
		offboard:        m.OffboardTemplate,
		namespaceLabels: m.NamespaceLabels,
		workers:         sc.Spec.Workers,
		defaultQuota:    m.DefaultProjectQuota,
		queues:          m.Queues,
		fairSharing:     m.AdmissionFairSharing,
	}
	if s.slurmCluster == "" {
		return site{}, fmt.Errorf("OffloadConfig %s: membership.slurmCluster is empty", sc.Name)
	}
	if s.cohort == "" {
		s.cohort = sc.Name
	}
	if s.check == "" {
		s.check = sc.Name
	}
	if d := m.SyncInterval; d != nil && d.Duration > 0 {
		s.interval = d.Duration
	}
	if s.executorService == "" {
		s.executorService = "kueue-offload-executor"
	}
	if s.executorPort == 0 {
		s.executorPort = 8000
	}
	for k, v := range s.namespaceLabels {
		if strings.HasPrefix(k, labelPrefix) {
			return site{}, fmt.Errorf("OffloadConfig %s: membership.namespaceLabels: %s is the controller's own prefix", sc.Name, k)
		}
		errs := append(validation.IsQualifiedName(k), validation.IsValidLabelValue(v)...)
		if len(errs) > 0 {
			return site{}, fmt.Errorf("OffloadConfig %s: membership.namespaceLabels %s=%q: %s", sc.Name, k, v, strings.Join(errs, "; "))
		}
	}
	seen := map[string]bool{}
	for _, q := range s.queues {
		if err := checkQueue(q, sc.Spec.Flavors, seen); err != nil {
			return site{}, fmt.Errorf("OffloadConfig %s: membership.queues %q: %w", sc.Name, q.Name, err)
		}
		seen[q.Name] = true
	}
	return s, nil
}

// checkQueue says why a queue cannot be rendered. seen holds the names of the
// queues before it.
func checkQueue(q v1alpha1.MembershipQueue, flavors map[string]v1alpha1.FlavorTarget, seen map[string]bool) error {
	if errs := validation.IsDNS1123Label(q.Name); len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	if contains(reservedQueues, q.Name) {
		return fmt.Errorf("the name is reserved")
	}
	if seen[q.Name] {
		return fmt.Errorf("listed twice")
	}
	if len(q.Flavors) == 0 {
		return fmt.Errorf("no flavor")
	}
	for i, f := range q.Flavors {
		if _, ok := flavors[string(f)]; !ok {
			return fmt.Errorf("flavor %s is not in flavors", f)
		}
		if slices.Contains(q.Flavors[:i], f) {
			return fmt.Errorf("flavor %s listed twice", f)
		}
	}
	return nil
}

// dropQueuesWithoutQuota leaves out of the site's queues those none of whose
// flavors has quota in the default or in a ProjectQuota of the site, which
// could admit nothing, and says so in msgs; a queue that lacks quota on some
// of its flavors only is kept and reported too.
func (s *site) dropQueuesWithoutQuota(quotas map[string][]kueue.ResourceGroup) (msgs []string) {
	have := map[kueue.ResourceFlavorReference]bool{}
	for _, groups := range append([][]kueue.ResourceGroup{s.defaultQuota}, slices.Collect(maps.Values(quotas))...) {
		for _, g := range groups {
			for _, f := range g.Flavors {
				have[f.Name] = true
			}
		}
	}
	var kept []v1alpha1.MembershipQueue
	for _, q := range s.queues {
		var missing []string
		for _, f := range q.Flavors {
			if !have[f] {
				missing = append(missing, string(f))
			}
		}
		what := fmt.Sprintf("membership.queues %q: flavor %s has no quota in defaultProjectQuota or a ProjectQuota of the site", q.Name, strings.Join(missing, ", "))
		if len(missing) > 1 {
			what = fmt.Sprintf("membership.queues %q: flavors %s have no quota in defaultProjectQuota or a ProjectQuota of the site", q.Name, strings.Join(missing, ", "))
		}
		switch {
		case len(missing) == len(q.Flavors):
			msgs = append(msgs, what+", the queue is skipped")
			continue
		case len(missing) > 0:
			msgs = append(msgs, what+", the queue admits on its other flavors")
		}
		kept = append(kept, q)
	}
	s.queues = kept
	return msgs
}

// queueNames are the names of the site's queues, in their order.
func (s site) queueNames() []string {
	out := make([]string, 0, len(s.queues))
	for _, q := range s.queues {
		out = append(out, q.Name)
	}
	return out
}

// queueName is the ClusterQueue and LocalQueue name of a queue of an account:
// <site>-<account> for the queue of every flavor (queue ""), else
// <site>-<account>-<queue>.
func (s site) queueName(account, queue string) string {
	if queue == "" {
		return s.queuePrefix + account
	}
	return s.queuePrefix + account + "-" + queue
}

// projectQuota is an account's quota, its ProjectQuota in quotas, else the
// site's default, and from names it in a message.
func (s site) projectQuota(quotas map[string][]kueue.ResourceGroup, account string) (quota []kueue.ResourceGroup, from string) {
	if q, ok := quotas[account]; ok {
		return q, "ProjectQuota " + s.name + "-" + account
	}
	return s.defaultQuota, "defaultProjectQuota"
}

// projectCohort is the name of an account's Cohort.
func (s site) projectCohort(account string) string { return s.queuePrefix + account + "-project" }

// fits reports whether a ClusterQueue, LocalQueue or Cohort name is no longer
// than a DNS label.
func fits(name string) bool { return len(name) <= validation.DNS1123LabelMaxLength }

// accountQueues are the queues of every account: "" for the one of every
// flavor, then the site's queues.
func (s site) accountQueues() []string { return append([]string{""}, s.queueNames()...) }

// accountNames are the names the controller renders for an account: its
// ClusterQueues, which its LocalQueues share, and its Cohort.
func (s site) accountNames(account string) []string {
	var out []string
	for _, q := range s.accountQueues() {
		out = append(out, s.queueName(account, q))
	}
	return append(out, s.projectCohort(account))
}

// isQueueOf reports whether name is the ClusterQueue or LocalQueue of one of
// the account's queues.
func (s site) isQueueOf(account, name string) bool {
	for _, q := range s.accountQueues() {
		if s.queueName(account, q) == name {
			return true
		}
	}
	return false
}

// queueFlavors are the flavors of a queue of the site, nil for the queue of
// every flavor.
func (s site) queueFlavors(queue string) []kueue.ResourceFlavorReference {
	for _, q := range s.queues {
		if q.Name == queue {
			return q.Flavors
		}
	}
	return nil
}
