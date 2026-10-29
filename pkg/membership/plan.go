package membership

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
)

// Association is one row of sacctmgr show assoc format=cluster,account,user.
type Association struct {
	Cluster, Account, User string
}

// ParseAssociations reads the output of
// sacctmgr --parsable2 --noheader show assoc format=cluster,account,user.
// Account rows (empty user) are dropped. Lines that do not have three fields
// are returned in bad. A trailing separator, as --parsable prints it, is
// accepted.
func ParseAssociations(text string) (rows []Association, bad []string) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		f := strings.Split(line, "|")
		if len(f) == 4 && f[3] == "" {
			f = f[:3]
		}
		if len(f) != 3 {
			bad = append(bad, line)
			continue
		}
		a := Association{Cluster: strings.TrimSpace(f[0]), Account: strings.TrimSpace(f[1]), User: strings.TrimSpace(f[2])}
		if a.Cluster == "" || a.Account == "" {
			bad = append(bad, line)
			continue
		}
		if a.User == "" {
			continue
		}
		rows = append(rows, a)
	}
	return rows, bad
}

// tenant is one namespace and the accounts it is a member of on the site.
type tenant struct {
	Namespace string
	HPCUser   string
	Accounts  []string
}

// plan is the desired state of one site.
type plan struct {
	// Accounts get one ClusterQueue each: every account with at least one
	// member that resolves in the IdentityMap.
	Accounts []string
	Tenants  map[string]*tenant
	// Skipped says why an identity entry or an account was left out.
	Skipped []string
}

// hasMembership reports whether namespace ns is a member of account.
func (p plan) hasMembership(ns, account string) bool {
	t := p.Tenants[ns]
	if t == nil {
		return false
	}
	for _, a := range t.Accounts {
		if a == account {
			return true
		}
	}
	return false
}

// buildPlan resolves the associations of the site through the identity
// entries. ownNamespace is the controller's namespace, which is never a
// tenant.
func buildPlan(s site, rows []Association, entries []v1alpha1.IdentityEntry, ownNamespace string) (plan, error) {
	p := plan{Tenants: map[string]*tenant{}}

	clusterRows := 0
	for _, r := range rows {
		if r.Cluster == s.slurmCluster {
			clusterRows++
		}
	}
	// An empty or mislabelled association list would revoke every
	// membership of the site, so it stops the sync instead.
	if clusterRows == 0 {
		return p, fmt.Errorf("no user association of Slurm cluster %q", s.slurmCluster)
	}

	userOf := map[string]map[string]bool{}
	nsOf := map[string][]string{}
	for _, e := range entries {
		if e.Site != s.name {
			continue
		}
		ns := e.Namespace
		if ns == "" {
			ns = e.KubernetesUser
		}
		if msg := forbiddenNamespace(ns, ownNamespace); msg != "" {
			p.Skipped = append(p.Skipped, fmt.Sprintf("identity %s: namespace %q %s", e.KubernetesUser, ns, msg))
			continue
		}
		if userOf[ns] == nil {
			userOf[ns] = map[string]bool{}
		}
		if !userOf[ns][e.HPCUser] {
			userOf[ns][e.HPCUser] = true
			nsOf[e.HPCUser] = append(nsOf[e.HPCUser], ns)
		}
	}
	// One agent per site holds one HPC user (D8), so a namespace mapped to
	// two users of the same site gets neither.
	conflict := map[string]bool{}
	for ns, users := range userOf {
		if len(users) > 1 {
			conflict[ns] = true
			p.Skipped = append(p.Skipped, fmt.Sprintf("namespace %s maps to %d users of site %s", ns, len(users), s.name))
		}
	}

	accounts := map[string]bool{}
	badAccount := map[string]bool{}
	for _, r := range rows {
		if r.Cluster != s.slurmCluster {
			continue
		}
		for _, ns := range nsOf[r.User] {
			if conflict[ns] {
				continue
			}
			if msg := s.invalidAccount(r.Account); msg != "" {
				if !badAccount[r.Account] {
					badAccount[r.Account] = true
					p.Skipped = append(p.Skipped, fmt.Sprintf("account %q: %s", r.Account, msg))
				}
				continue
			}
			t := p.Tenants[ns]
			if t == nil {
				t = &tenant{Namespace: ns, HPCUser: r.User}
				p.Tenants[ns] = t
			}
			if !contains(t.Accounts, r.Account) {
				t.Accounts = append(t.Accounts, r.Account)
			}
			accounts[r.Account] = true
		}
	}
	for a := range accounts {
		p.Accounts = append(p.Accounts, a)
	}
	sort.Strings(p.Accounts)
	for _, t := range p.Tenants {
		sort.Strings(t.Accounts)
	}
	sort.Strings(p.Skipped)
	return p, nil
}

// invalidAccount says why an account cannot be represented in Kubernetes: its
// queue name must be a DNS label and its namespace label a qualified name.
func (s site) invalidAccount(account string) string {
	if errs := validation.IsDNS1123Label(s.queueName(account)); len(errs) > 0 {
		return fmt.Sprintf("queue name %q: %s", s.queueName(account), strings.Join(errs, "; "))
	}
	if errs := validation.IsQualifiedName(ProjectLabel(s.name, account)); len(errs) > 0 {
		return fmt.Sprintf("label %q: %s", ProjectLabel(s.name, account), strings.Join(errs, "; "))
	}
	return ""
}

// forbiddenNamespace says why ns cannot be a tenant namespace.
func forbiddenNamespace(ns, ownNamespace string) string {
	if errs := validation.IsDNS1123Label(ns); len(errs) > 0 {
		return "is not a valid namespace name, set namespace in the IdentityMap"
	}
	if ns == ownNamespace || ns == "default" || strings.HasPrefix(ns, "kube-") {
		return "is reserved"
	}
	return ""
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func splitAccounts(v string) []string {
	var out []string
	for _, a := range strings.Split(v, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
