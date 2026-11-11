package membership

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
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
	// Accounts get one Cohort and their ClusterQueues each: every account with
	// at least one member that resolves in the IdentityMap.
	Accounts []string
	Tenants  map[string]*tenant
	// SkippedQueues are, per account, the site's queues left out for it:
	// another account holds their name, or none of their flavors has quota in
	// the account's.
	SkippedQueues map[string][]string
	// Skipped says why an identity entry, an account or a queue was left out.
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

// queues are the queues the plan renders for account: "" for the one of every
// flavor, then the site's, less those whose name is too long and those in
// SkippedQueues.
func (p plan) queues(s site, account string) []string {
	return slices.DeleteFunc(s.accountQueues(), func(q string) bool {
		return !fits(s.queueName(account, q)) || contains(p.SkippedQueues[account], q)
	})
}

// queuesOf maps each of accounts to the names of the site's queues it has in
// the plan, in their order: the executor template's .QueuesOf.
func (p plan) queuesOf(s site, accounts []string) map[string][]string {
	out := make(map[string][]string, len(accounts))
	for _, a := range accounts {
		out[a] = slices.DeleteFunc(p.queues(s, a), func(q string) bool { return q == "" })
	}
	return out
}

// skipQueue leaves queue out for account.
func (p *plan) skipQueue(account, queue string) {
	if p.SkippedQueues == nil {
		p.SkippedQueues = map[string][]string{}
	}
	p.SkippedQueues[account] = append(p.SkippedQueues[account], queue)
}

// skipsQueue reports whether name is the ClusterQueue of a queue the plan
// leaves out for account.
func (p plan) skipsQueue(s site, account, name string) bool {
	for _, q := range p.SkippedQueues[account] {
		if s.queueName(account, q) == name {
			return true
		}
	}
	return false
}

// hasQueue reports whether the plan has LocalQueue name of account in
// namespace ns.
func (p plan) hasQueue(s site, ns, account, name string) bool {
	if !p.hasMembership(ns, account) {
		return false
	}
	for _, q := range p.queues(s, account) {
		if s.queueName(account, q) == name {
			return true
		}
	}
	return false
}

// buildPlan resolves the associations of the site through the identity
// entries. ownNamespace is the controller's namespace, which is never a
// tenant; owners are as in claimNames.
func buildPlan(s site, rows []Association, entries []v1alpha1.IdentityEntry, ownNamespace string, owners nameOwners) (plan, error) {
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
	// One executor per site holds one HPC user, so a namespace mapped to
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
	// badCohort is the accounts whose project Cohort cannot be made, and why.
	badCohort := map[string]string{}
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
			switch c := s.projectCohort(r.Account); {
			case !fits(c):
				badCohort[r.Account] = fmt.Sprintf("Cohort name %s is longer than %d characters", c, validation.DNS1123LabelMaxLength)
				continue
			case c == s.cohort:
				// a project Cohort cannot be its own parent
				badCohort[r.Account] = fmt.Sprintf("Cohort name %s is the site's Cohort", c)
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
	// An account whose Cohort cannot be made (its name too long, or the
	// site's Cohort) is left out, as an invalid one is, unless the site has a
	// ClusterQueue or Cohort of it, which an earlier controller made before
	// project Cohorts: leaving the account out would revoke them, so the sync
	// stops instead.
	held := map[string]bool{}
	for _, m := range []map[string]string{owners.clusterQueues, owners.cohorts} {
		for _, a := range m {
			held[a] = true
		}
	}
	var refused []string
	for _, a := range slices.Sorted(maps.Keys(badCohort)) {
		msg := fmt.Sprintf("account %q: %s", a, badCohort[a])
		if held[a] {
			refused = append(refused, msg)
		} else {
			p.Skipped = append(p.Skipped, msg)
		}
	}
	if len(refused) > 0 {
		return p, errors.New(strings.Join(refused, "; "))
	}
	for a := range accounts {
		p.Accounts = append(p.Accounts, a)
	}
	sort.Strings(p.Accounts)
	for _, t := range p.Tenants {
		sort.Strings(t.Accounts)
	}
	// A queue name too long is skipped for that account alone, which keeps
	// its other queues.
	for _, a := range p.Accounts {
		for _, q := range s.queueNames() {
			if n := s.queueName(a, q); !fits(n) {
				p.Skipped = append(p.Skipped, fmt.Sprintf("account %q %s: name %s is longer than %d characters, the queue is skipped", a, queueTitle(q), n, validation.DNS1123LabelMaxLength))
			}
		}
	}
	sort.Strings(p.Skipped)
	return p, nil
}

// skipQueuesWithoutQuota leaves out for each account the site's queues none of
// whose flavors has quota in the account's quota, which could admit nothing.
func (p *plan) skipQueuesWithoutQuota(s site, quotas map[string][]kueue.ResourceGroup) {
	for _, a := range p.Accounts {
		quota, from := s.projectQuota(quotas, a)
		have := map[kueue.ResourceFlavorReference]bool{}
		for _, g := range quota {
			for _, f := range g.Flavors {
				have[f.Name] = true
			}
		}
		for _, q := range s.queues {
			if !fits(s.queueName(a, q.Name)) || slices.ContainsFunc(q.Flavors, func(f kueue.ResourceFlavorReference) bool { return have[f] }) {
				continue
			}
			what := fmt.Sprintf("flavor %s has", q.Flavors[0])
			if len(q.Flavors) > 1 {
				names := make([]string, 0, len(q.Flavors))
				for _, f := range q.Flavors {
					names = append(names, string(f))
				}
				what = fmt.Sprintf("flavors %s have", strings.Join(names, ", "))
			}
			p.Skipped = append(p.Skipped, fmt.Sprintf("account %q %s: %s no quota in %s, the queue is skipped", a, queueTitle(q.Name), what, from))
			p.skipQueue(a, q.Name)
		}
	}
}

// nameOwners map the names of the site's ClusterQueues, which their
// LocalQueues share, and of its Cohorts to their account label. Kueue keeps
// the two kinds in separate name spaces, so a ClusterQueue and a Cohort can
// share a name.
type nameOwners struct {
	clusterQueues, cohorts map[string]string
}

// claimNames leaves out of the plan every name of an account that another
// account holds, so that no name has two owners of one kind: a Cohort name is
// checked against Cohorts, a queue name against ClusterQueues. In owners, the
// account that holds a name keeps it until it is pruned. Names no object holds
// go first to every account's Cohort and queue of every flavor, then to the
// other queues, each in account name order. Templates name the queue of every
// flavor of every account, so an account whose Cohort or queue of every flavor
// name is taken is left out; another queue whose name is taken is skipped
// alone. The site's Cohort is no account's.
func (p *plan) claimNames(s site, owners nameOwners) {
	queues, cohorts := map[string]string{}, map[string]string{}
	maps.Copy(queues, owners.clusterQueues)
	maps.Copy(cohorts, owners.cohorts)
	cohorts[s.cohort] = ""
	taken := func(claimed map[string]string, a, n string) string {
		o, ok := claimed[n]
		switch {
		case !ok || o == a:
			return ""
		case o == "":
			return fmt.Sprintf("name %s is the site's Cohort", n)
		}
		return fmt.Sprintf("name %s is account %s's", n, o)
	}
	var kept []string
	for _, a := range p.Accounts {
		clash := taken(cohorts, a, s.projectCohort(a))
		if clash == "" {
			clash = taken(queues, a, s.queueName(a, ""))
		}
		if clash != "" {
			p.Skipped = append(p.Skipped, fmt.Sprintf("account %q: %s", a, clash))
			continue
		}
		cohorts[s.projectCohort(a)] = a
		queues[s.queueName(a, "")] = a
		kept = append(kept, a)
	}
	for _, a := range kept {
		for _, q := range p.queues(s, a) {
			if q == "" {
				continue
			}
			if clash := taken(queues, a, s.queueName(a, q)); clash != "" {
				p.Skipped = append(p.Skipped, fmt.Sprintf("account %q %s: %s, the queue is skipped", a, queueTitle(q), clash))
				p.skipQueue(a, q)
				continue
			}
			queues[s.queueName(a, q)] = a
		}
	}
	if len(kept) == len(p.Accounts) {
		return
	}
	p.Accounts = kept
	for ns, t := range p.Tenants {
		t.Accounts = slices.DeleteFunc(t.Accounts, func(a string) bool { return !contains(kept, a) })
		if len(t.Accounts) == 0 {
			delete(p.Tenants, ns)
		}
	}
}

// queueTitle names a queue of an account in a message.
func queueTitle(queue string) string {
	if queue == "" {
		return "queue of every flavor"
	}
	return fmt.Sprintf("queue %q", queue)
}

// invalidAccount says why an account cannot be represented in Kubernetes
// whatever the length of its names: its queue and Cohort names must be DNS
// labels, which makes its namespace label, on <site>-<account>, a qualified
// name. buildPlan checks the lengths.
func (s site) invalidAccount(account string) string {
	tooLong := validation.MaxLenError(validation.DNS1123LabelMaxLength)
	for _, n := range s.accountNames(account) {
		if errs := slices.DeleteFunc(validation.IsDNS1123Label(n), func(e string) bool { return e == tooLong }); len(errs) > 0 {
			return fmt.Sprintf("name %q: %s", n, strings.Join(errs, "; "))
		}
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

func splitList(v string) []string {
	var out []string
	for _, a := range strings.Split(v, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
