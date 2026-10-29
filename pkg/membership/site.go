// Package membership keeps Kueue objects, namespace labels and agents in step
// with the Slurm associations of one site.
package membership

import (
	"fmt"
	"time"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
)

// FieldManager owns every object and label the controller writes.
const FieldManager = "interlink-membership"

// Labels and annotations the controller writes.
const (
	labelPrefix = "hpc.interlink.eu/"

	// ManagedByLabel marks every object the controller applies.
	ManagedByLabel = labelPrefix + "managed-by"
	ManagedByValue = FieldManager
	// TenantLabel marks a namespace that holds at least one membership.
	TenantLabel = labelPrefix + "tenant"
	// SiteLabel and AccountLabel say which membership an object belongs to.
	SiteLabel    = labelPrefix + "site"
	AccountLabel = labelPrefix + "account"

	// MissingCredentials is the value of the agent annotation (D10).
	MissingCredentials = "missing credentials"
)

// ProjectLabel is the namespace label that grants the namespace a project.
func ProjectLabel(site, account string) string { return labelPrefix + site + "-" + account }

// AccountsAnnotation records on a namespace the accounts of a site the
// controller granted, so that it removes only labels it set.
func AccountsAnnotation(site string) string { return labelPrefix + site + "-accounts" }

// AgentAnnotation reports on a namespace that the site's agent cannot start.
func AgentAnnotation(site string) string { return labelPrefix + site + "-agent" }

// Event reasons.
const (
	EventSkipped            = "MembershipSkipped"
	EventTemplateRefused    = "TemplateRefused"
	EventSyncFailed         = "SyncFailed"
	EventMissingCredentials = "MissingCredentials"
	EventCredentialsPresent = "CredentialsPresent"
)

// site is a SiteConfig with its membership defaults applied.
type site struct {
	name         string
	slurmCluster string
	cohort       string
	check        string
	queuePrefix  string
	interval     time.Duration
	agentService string
	agentPort    int32
	agentImage   string
	tokenReview  bool
	associations v1alpha1.ConfigMapKeyRef
	template     *v1alpha1.ConfigMapKeyRef
	defaultQuota []kueue.ResourceGroup
}

func resolveSite(sc *v1alpha1.SiteConfig) (site, error) {
	m := sc.Spec.Membership
	if m == nil {
		return site{}, fmt.Errorf("SiteConfig %s has no membership block", sc.Name)
	}
	s := site{
		name:         sc.Name,
		slurmCluster: m.SlurmCluster,
		cohort:       m.Cohort,
		check:        m.AdmissionCheck,
		queuePrefix:  sc.Name + "-",
		interval:     15 * time.Minute,
		agentService: sc.Spec.AgentService,
		agentPort:    sc.Spec.AgentPort,
		agentImage:   m.AgentImage,
		tokenReview:  m.AgentTokenReview,
		associations: m.Associations,
		template:     m.AgentTemplate,
		defaultQuota: m.DefaultProjectQuota,
	}
	if s.slurmCluster == "" {
		return site{}, fmt.Errorf("SiteConfig %s: membership.slurmCluster is empty", sc.Name)
	}
	if s.cohort == "" {
		s.cohort = sc.Name
	}
	if s.check == "" {
		s.check = sc.Name
	}
	// The dispatcher derives the account from the ClusterQueue name with the
	// same rule, so queues are named with it.
	if p := sc.Spec.AccountRule.StripPrefix; p != nil {
		s.queuePrefix = *p
	}
	if d := m.SyncInterval; d != nil && d.Duration > 0 {
		s.interval = d.Duration
	}
	if s.agentService == "" {
		s.agentService = "interlink-agent"
	}
	if s.agentPort == 0 {
		s.agentPort = 8000
	}
	return s, nil
}

// queueName is the ClusterQueue and LocalQueue name of an account.
func (s site) queueName(account string) string { return s.queuePrefix + account }
