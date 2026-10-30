// Package membership keeps Kueue objects, namespace labels and executors in step
// with the Slurm associations of one site.
package membership

import (
	"fmt"
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
}

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
	return s, nil
}

// queueName is the ClusterQueue and LocalQueue name of an account.
func (s site) queueName(account string) string { return s.queuePrefix + account }
