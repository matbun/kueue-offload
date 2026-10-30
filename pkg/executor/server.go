// Package executor is the kueue-offload executor. It runs the OffloadRequests
// that the offload controller, an admission check controller, creates in its
// namespace for the Kueue Workloads to run at its site: it turns a request's
// PodSets into member pods and sends each to an interLink API server's /create,
// as a virtual kubelet sends a pod, so the plugin behind it runs them (one pod:
// one job; several: one job of a pod group, or one job each for independent
// pods). A plugin that lists pod-group-create gets a pod group's members in one
// /createPodGroup instead. On every poll it maps the members' /status to the
// request's status, and once the request is deleted it deletes the members and
// removes what it made for them. What it keeps about a request lives in the
// request's status.
package executor

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/log"
	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/shadow"
)

// maxWorkloadMembers bounds the members of a workload, each one /create.
const maxWorkloadMembers = 9999

// healthTTL is how long a ping of the interLink API server that succeeded
// answers /healthz.
const healthTTL = 30 * time.Second

// jobRequest is what the executor runs for an OffloadRequest: its spec, with the
// owner's annotations merged into every PodSet template, whose own value for a
// key wins.
type jobRequest struct {
	WorkloadUID  string
	WorkloadName string
	Namespace    string
	Owner        v1alpha1.OwnerReference
	OwnerLabels  map[string]string
	Site         string
	Account      string
	PodSets      []v1alpha1.PodSet
}

func jobRequestOf(q *v1alpha1.OffloadRequest) jobRequest {
	req := jobRequest{
		WorkloadUID:  string(q.Spec.Workload.UID),
		WorkloadName: q.Spec.Workload.Name,
		Namespace:    q.Namespace,
		Owner:        q.Spec.Owner,
		OwnerLabels:  q.Spec.OwnerLabels,
		Site:         q.Spec.Site,
		Account:      q.Spec.Account,
	}
	for _, ps := range q.Spec.PodSets {
		ps := *ps.DeepCopy()
		for k, v := range q.Spec.OwnerAnnotations {
			if _, own := ps.Template.Annotations[k]; own {
				continue
			}
			if ps.Template.Annotations == nil {
				ps.Template.Annotations = map[string]string{}
			}
			ps.Template.Annotations[k] = v
		}
		req.PodSets = append(req.PodSets, ps)
	}
	return req
}

// Executor runs OffloadRequests on top of an interLink API server.
type Executor struct {
	Config    Config
	InterLink *InterLink
	// Kube reads the workloads' Secrets and ConfigMaps, mints their tokens and
	// runs their stage Jobs, in the executor's namespace only.
	Kube kubernetes.Interface
	// Client reads the OffloadRequests of the executor's site from the cache and
	// writes their status and the executor's finalizer.
	Client client.Client
	// Reader reads from the API server: the Workload a request names, and a
	// request again before a submit that a stale cache would repeat.
	Reader client.Reader
	// Shadow renders the shadows. Nil: no shadows.
	Shadow ShadowManager

	// shadowsEnded holds the requests whose shadow was rendered for the job's
	// end. It is not kept across restarts: rendering it once more is idempotent.
	mu           sync.Mutex
	shadowsEnded map[types.UID]bool
	// healthyUntil is when the last successful ping stops answering /healthz;
	// caps are the capabilities that ping listed, once capsKnown.
	healthMu     sync.Mutex
	healthyUntil time.Time
	caps         []string
	capsKnown    bool
}

// workloadUIDPattern is a UUID: member UIDs are derived from the workload's.
var workloadUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// New returns an Executor. c and reader may be the same client.
func New(cfg Config, il *InterLink, kube kubernetes.Interface, c client.Client, reader client.Reader, shadow ShadowManager) *Executor {
	return &Executor{Config: cfg, InterLink: il, Kube: kube, Client: c, Reader: reader, Shadow: shadow, shadowsEnded: map[types.UID]bool{}}
}

// RegisterRoutes adds the executor's HTTP API, for the shadows, to mux.
func (d *Executor) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /getLogs", d.GetLogsHandler)
	mux.HandleFunc("GET /healthz", d.HealthzHandler)
}

// HealthzHandler answers 200 ok while the interLink API server answers its ping,
// which it passes to the plugin.
func (d *Executor) HealthzHandler(w http.ResponseWriter, r *http.Request) {
	d.healthMu.Lock()
	healthy := time.Now().Before(d.healthyUntil)
	d.healthMu.Unlock()
	if !healthy {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := d.ping(ctx); err != nil {
			http.Error(w, "interLink API server: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// refusal is why the executor does not go on with a request now. A permanent
// one refuses it for good (Accepted=False). An unreachable one is a failure of
// the interLink API server or the plugin that is not known to end: the executor
// writes no heartbeat, so the offload controller gives up after
// executorLostTimeout. Any other ends on its own, and the request waits for it
// holding the quota.
type refusal struct {
	permanent   bool
	unreachable bool
	msg         string
}

func permanentError(format string, args ...any) *refusal {
	return &refusal{permanent: true, msg: fmt.Sprintf(format, args...)}
}

func transientError(format string, args ...any) *refusal {
	return &refusal{msg: fmt.Sprintf(format, args...)}
}

func unreachableError(format string, args ...any) *refusal {
	return &refusal{unreachable: true, msg: fmt.Sprintf(format, args...)}
}

func (d *Executor) shadowManager() ShadowManager {
	if d.Shadow == nil || !d.Config.Shadow.Enabled {
		return noopShadowManager{}
	}
	return d.Shadow
}

// validateSubmit refuses a request this executor will never run.
func (d *Executor) validateSubmit(req jobRequest) *refusal {
	cfg := d.Config
	if !workloadUIDPattern.MatchString(req.WorkloadUID) {
		return permanentError("invalid workload UID %q", req.WorkloadUID)
	}
	site, ok := cfg.Sites[req.Site]
	if !ok {
		return permanentError("unknown site %q", req.Site)
	}
	if !slices.Contains(site.AllowedAccounts, req.Account) {
		return permanentError("account %q is not allowed on site %s", req.Account, req.Site)
	}
	if len(req.PodSets) == 0 {
		return permanentError("the workload has no PodSets")
	}
	total := 0
	for _, ps := range req.PodSets {
		if ps.Count < 1 || ps.Count > maxWorkloadMembers {
			return permanentError("PodSet %s: count %d is not between 1 and %d", ps.Name, ps.Count, maxWorkloadMembers)
		}
		total += int(ps.Count)
		if len(ps.Template.Spec.Containers) == 0 {
			return permanentError("PodSet %s: the template has no containers", ps.Name)
		}
	}
	if total > maxWorkloadMembers {
		return permanentError("the workload has %d members, more than %d", total, maxWorkloadMembers)
	}
	if podGroup(req) {
		if aerr := validatePodSetsShareAllocation(req.PodSets); aerr != nil {
			return aerr
		}
	}
	if ownerName(req) == "" {
		return permanentError("the workload has neither an owner name nor a name")
	}
	if aerr := d.validateTunnels(req); aerr != nil {
		return aerr
	}
	return d.validateStaging(req)
}

// validateTunnels refuses the tunnels of a site whose Shadow block names no login
// node: its shadows only stream logs (backends.md section 6). The shadow serves
// the head's template.
func (d *Executor) validateTunnels(req jobRequest) *refusal {
	cfg := d.Config.Shadow
	if !cfg.Enabled || cfg.LoginHost != "" {
		return nil
	}
	tmpl := req.PodSets[0].Template
	if shadow.HasReverseAnnotations(tmpl.Annotations) {
		return permanentError("%s is set, but this site's shadows have no login node (Shadow.LoginHost) to tunnel through", shadow.AnnReverseForwards)
	}
	if shadow.ForwardsPorts(tmpl) {
		return permanentError("the template exposes ports, but this site's shadows have no login node (Shadow.LoginHost) to forward them through")
	}
	return nil
}

// validatePodSetsShareAllocation refuses PodSets that cannot run in one
// allocation: it takes one queue, and a site launcher starts the head's container
// on every rank, so the others' would never run.
func validatePodSetsShareAllocation(podSets []v1alpha1.PodSet) *refusal {
	first := podSets[0]
	launcher := ""
	for _, ps := range podSets {
		if name := launcherName(ps.Template); name != "" {
			launcher = name
		}
	}
	for _, ps := range podSets[1:] {
		if !maps.Equal(ps.Annotations, first.Annotations) {
			return permanentError("PodSet %s: its flavor's annotations differ from PodSet %s's: the workload runs in one allocation", ps.Name, first.Name)
		}
		if launcher == "" {
			continue
		}
		a, b := first.Template.Spec.Containers[0], ps.Template.Spec.Containers[0]
		if a.Image != b.Image || !slices.Equal(a.Command, b.Command) || !slices.Equal(a.Args, b.Args) {
			return permanentError("PodSet %s: launcher %q starts PodSet %s's container on every rank, so every PodSet needs the same image and command", ps.Name, launcher, first.Name)
		}
	}
	return nil
}

// submitRequest is the request's workload as its members are built: with the
// ServiceAccount directory the API server would add to its pods.
func (d *Executor) submitRequest(q *v1alpha1.OffloadRequest) jobRequest {
	req := jobRequestOf(q)
	for p := range req.PodSets {
		withServiceAccountDir(&req.PodSets[p], d.Config.Executor.AutomountServiceAccountToken)
	}
	return req
}

// submit runs the workload of q, staging it in first, and records the job in
// q's status: its ID and every rank, a rank without a podUID waiting for the
// site to take it. The caller writes the status.
func (d *Executor) submit(ctx context.Context, q *v1alpha1.OffloadRequest) *refusal {
	// A caller that gives up does not stop a submit halfway.
	ctx = context.WithoutCancel(ctx)
	req := d.submitRequest(q)

	if aerr := d.checkShape(ctx, req); aerr != nil {
		return aerr
	}
	groupCall, aerr := d.createsPodGroup(ctx, req)
	if aerr != nil {
		return aerr
	}
	reverse, aerr := d.reverseTunnel(req)
	if aerr != nil {
		return aerr
	}
	refs := make([]podRefs, len(req.PodSets))
	for p, ps := range req.PodSets {
		if refs[p], aerr = d.fetchRefs(ctx, req.Namespace, ps); aerr != nil {
			return aerr
		}
	}
	if aerr := d.stageIn(ctx, q, req); aerr != nil {
		return aerr
	}
	for p, ps := range req.PodSets {
		if refs[p].tokens, aerr = d.mintTokens(ctx, req.Namespace, ps.Template.Spec); aerr != nil {
			return aerr
		}
	}

	count := workloadMembers(req)
	members := make([]*v1.Pod, 0, count)
	creates := make([]commonIL.PodCreateRequests, 0, count)
	for i := 0; i < count; i++ {
		pod, create, aerr := d.memberCreate(req, refs, reverse, i)
		if aerr != nil {
			return aerr
		}
		members = append(members, pod)
		creates = append(creates, create)
	}

	// The members go in rank order. A plugin that runs a pod group as one job
	// answers the members before the last with no job ID, or takes them all in
	// one /createPodGroup; independent members each get their own job.
	group := podGroup(req)
	ranks := make([]v1alpha1.RankStatus, count)
	var jobIDs []string
	pending, pendingMessage := 0, ""
	if groupCall {
		jobID, aerr := d.createPodGroup(ctx, req, members, creates)
		if aerr != nil {
			return aerr
		}
		groupCall = jobID != ""
		if groupCall {
			jobIDs = append(jobIDs, jobID)
		}
	}
	for i, create := range creates {
		ranks[i] = v1alpha1.RankStatus{Rank: int32(i), State: v1alpha1.RankQueued}
		if groupCall {
			ranks[i].PodUID = string(members[i].UID)
			continue
		}
		if pending > 0 {
			continue
		}
		res, err := d.InterLink.Create(ctx, req.WorkloadUID, create)
		if err != nil {
			aerr := createRefusal(err)
			if !group && i > 0 && !aerr.permanent && !aerr.unreachable {
				// The independent members the site took stay: the others wait for it,
				// so that a per-user job limit does not cancel the ranks it let in.
				pending, pendingMessage = count-i, aerr.msg
				continue
			}
			d.deleteMembersQuietly(ctx, req.WorkloadUID, members[:i+1])
			return aerr
		}
		ranks[i].PodUID = string(members[i].UID)
		if res.PodJID != "" {
			jobIDs = append(jobIDs, res.PodJID)
			if !group {
				ranks[i].JobID = res.PodJID
			}
		}
	}
	jobID := ""
	switch {
	case group && len(jobIDs) > 0:
		jobID = jobIDs[len(jobIDs)-1]
	case !group && len(jobIDs) == count-pending:
		jobID = jobIDList(jobIDs)
	}
	if jobID == "" {
		d.deleteMembersQuietly(ctx, req.WorkloadUID, members)
		return transientError("the job of workload %s was not submitted: the plugin gave no job ID", req.WorkloadUID)
	}

	st := &q.Status
	st.JobID, st.Ranks = jobID, ranks
	st.Reason, st.Message = "", ""
	if pending > 0 {
		st.Reason, st.Message = reasonWaitingForSite, fmt.Sprintf("rank %d waits for the site: %s", count-pending, pendingMessage)
	}
	setCondition(q, v1alpha1.OffloadRequestQuotaReserved, metav1.ConditionTrue, reasonSubmitted, "")
	setCondition(q, v1alpha1.OffloadRequestAdmitted, metav1.ConditionFalse, reasonQueued, "")
	log.L.Infof("executor: request %s/%s submitted as job %s (%d member(s), %d waiting for the site)", q.Namespace, q.Name, jobID, count, pending)
	return nil
}

// memberCreate builds rank i of the workload and its /create request.
func (d *Executor) memberCreate(req jobRequest, refs []podRefs, reverse *shadow.ReverseTunnel, i int) (*v1.Pod, commonIL.PodCreateRequests, *refusal) {
	pod := buildMember(req, i)
	if i == 0 && reverse != nil {
		addReversePreExec(pod, reverse)
	}
	memberRefs := refs[memberPodSet(req, i)]
	if aerr := resolveEnv(pod, memberRefs); aerr != nil {
		return nil, commonIL.PodCreateRequests{}, aerr
	}
	if api := d.Config.Executor.KubernetesAPI; api.Host != "" && len(memberRefs.tokens) > 0 {
		withKubernetesAPI(pod, api)
	}
	create, aerr := podCreateRequest(pod, memberRefs)
	return pod, create, aerr
}

// createPodGroup sends the members of a pod group in one /createPodGroup call,
// in rank order, and returns the group's job ID. A failed call deletes the
// members, as a failed /create does, and so does an answer that is not one
// entry per member, in rank order, all with the same job ID. It returns no job
// ID and no refusal when the call answers 404: the plugin lists
// pod-group-create, but the API server in front of it has no such route and
// submitted nothing, so the members go to /create one by one.
func (d *Executor) createPodGroup(ctx context.Context, req jobRequest, members []*v1.Pod, creates []commonIL.PodCreateRequests) (string, *refusal) {
	res, err := d.InterLink.CreatePodGroup(ctx, req.WorkloadUID, PodGroupCreateRequests{GroupUID: req.WorkloadUID, Pods: creates})
	if pluginCode(err) == http.StatusNotFound {
		log.L.Warning("executor: workload ", req.WorkloadUID, ": the interLink API server has no /createPodGroup, the members go to /create")
		return "", nil
	}
	if err != nil {
		d.deleteMembersQuietly(ctx, req.WorkloadUID, members)
		return "", createRefusal(err)
	}
	jobID, problem := podGroupJobID(res, members)
	if problem != "" {
		d.deleteMembersQuietly(ctx, req.WorkloadUID, members)
		return "", transientError("the job of workload %s was not submitted: the answer to /createPodGroup %s", req.WorkloadUID, problem)
	}
	return jobID, nil
}

// podGroupJobID returns the job ID of an answer to /createPodGroup, or what is
// wrong with the answer: it lists every member once, in rank order, all with
// the same job ID.
func podGroupJobID(res []commonIL.CreateStruct, members []*v1.Pod) (jobID, problem string) {
	if len(res) != len(members) {
		return "", fmt.Sprintf("lists %d pods, not the %d members", len(res), len(members))
	}
	for i, r := range res {
		switch {
		case r.PodUID != string(members[i].UID):
			return "", fmt.Sprintf("lists pod %q at rank %d, not %s", r.PodUID, i, members[i].UID)
		case r.PodJID == "":
			return "", fmt.Sprintf("gives rank %d no job ID", i)
		case i > 0 && r.PodJID != jobID:
			return "", fmt.Sprintf("gives rank %d job %s and rank 0 job %s", i, r.PodJID, jobID)
		}
		jobID = r.PodJID
	}
	return jobID, ""
}

// createPending creates the ranks of independent members that wait for the
// site, in rank order, until the site refuses one for now; it records each in
// q's status. It returns the site's last refusal for now, and refused when the
// site refused a rank for good, which fails the workload.
func (d *Executor) createPending(ctx context.Context, q *v1alpha1.OffloadRequest) (pendingMessage, refused string) {
	if !hasPendingRanks(q) {
		return "", ""
	}
	ctx = context.WithoutCancel(ctx)
	req := d.submitRequest(q)
	refs := make([]podRefs, len(req.PodSets))
	var aerr *refusal
	for p, ps := range req.PodSets {
		if refs[p], aerr = d.fetchRefs(ctx, req.Namespace, ps); aerr == nil {
			refs[p].tokens, aerr = d.mintTokens(ctx, req.Namespace, ps.Template.Spec)
		}
		if aerr != nil {
			return aerr.msg, ""
		}
	}
	ranks := q.Status.Ranks
	var jobIDs []string
	for i := range ranks {
		if ranks[i].PodUID == "" && pendingMessage == "" && refused == "" {
			pod, create, aerr := d.memberCreate(req, refs, nil, i)
			if aerr == nil {
				res, err := d.InterLink.Create(ctx, req.WorkloadUID, create)
				if err == nil {
					ranks[i].PodUID, ranks[i].JobID = string(pod.UID), res.PodJID
				} else {
					aerr = createRefusal(err)
				}
			}
			switch {
			case aerr == nil:
			case aerr.permanent:
				refused = fmt.Sprintf("rank %d: %s", i, aerr.msg)
			default:
				pendingMessage = aerr.msg
			}
		}
		if ranks[i].JobID != "" {
			jobIDs = append(jobIDs, ranks[i].JobID)
		}
	}
	q.Status.JobID = jobIDList(jobIDs)
	return pendingMessage, refused
}

// hasPendingRanks reports whether some rank of q waits for the site: it has no
// podUID, since it was never created.
func hasPendingRanks(q *v1alpha1.OffloadRequest) bool {
	return slices.ContainsFunc(q.Status.Ranks, func(r v1alpha1.RankStatus) bool { return r.PodUID == "" })
}

// rankPending reports whether rank of q waits for the site to take it.
func rankPending(q *v1alpha1.OffloadRequest, rank int) bool {
	return rank < len(q.Status.Ranks) && q.Status.Ranks[rank].PodUID == ""
}

// maxListedJobIDs bounds the job IDs of independent members that a workload's
// job ID lists, so that the check message stays short.
const maxListedJobIDs = 8

// jobIDList names the jobs of independent members, joined by commas: all of
// them up to maxListedJobIDs, else the first and the last.
func jobIDList(ids []string) string {
	if len(ids) <= maxListedJobIDs {
		return strings.Join(ids, ",")
	}
	return fmt.Sprintf("%s,...,%s (%d jobs)", ids[0], ids[len(ids)-1], len(ids))
}

// createRefusal maps a failed /create to a refusal: a 503 is one for now (a
// submit limit, a site that cannot be reached), a 400, 403 or 422 a permanent
// one, and any other answer, or none, a failure that the offload controller
// gives up on after executorLostTimeout.
func createRefusal(err error) *refusal {
	var pe *PluginError
	if !errors.As(err, &pe) {
		return unreachableError("the interLink API server cannot be reached: %v", err)
	}
	switch pe.Code {
	case http.StatusServiceUnavailable:
		return transientError("the site takes no job now: %s", pe.Message)
	case http.StatusBadRequest, http.StatusForbidden, http.StatusUnprocessableEntity:
		return &refusal{permanent: true, msg: pe.Message}
	}
	return unreachableError("the interLink API server answered %d: %s", pe.Code, pe.Message)
}

// deleteMembers sends /delete for every member: the plugin cancels the job unless
// it ended, drops members it buffers for a pod group, and removes their files.
func (d *Executor) deleteMembers(ctx context.Context, session string, members []*v1.Pod) error {
	var errs []error
	for _, m := range members {
		if err := d.InterLink.Delete(ctx, session, m); err != nil {
			errs = append(errs, fmt.Errorf("member %s: %w", m.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (d *Executor) deleteMembersQuietly(ctx context.Context, session string, members []*v1.Pod) {
	if err := d.deleteMembers(ctx, session, members); err != nil {
		log.L.Warning("executor: workload ", session, ": cannot delete the members created so far: ", err)
	}
}

// memberPods rebuilds the workload's members as submit made them, without the
// values of their references.
func memberPods(req jobRequest) []*v1.Pod {
	n := workloadMembers(req)
	pods := make([]*v1.Pod, 0, n)
	for i := 0; i < n; i++ {
		pods = append(pods, buildMember(req, i))
	}
	return pods
}

// createdMembers are the members of q the plugin has: none before the submit,
// and not the ranks that wait for the site.
func createdMembers(q *v1alpha1.OffloadRequest, req jobRequest) []*v1.Pod {
	if q.Status.JobID == "" {
		return nil
	}
	var created []*v1.Pod
	for i, m := range memberPods(req) {
		if !rankPending(q, i) {
			created = append(created, m)
		}
	}
	return created
}

// ReasonCancelled is the reason of a container a plugin reports cancelled outside
// the executor, whatever its exit code.
const ReasonCancelled = "Cancelled"

// Job states: a rank's (v1alpha1.Rank*), and cancelled for a job lost outside
// the executor.
const (
	jobQueued    = v1alpha1.RankQueued
	jobRunning   = v1alpha1.RankRunning
	jobCompleted = v1alpha1.RankCompleted
	jobFailed    = v1alpha1.RankFailed
	jobCancelled = "cancelled"
)

// jobState is the job as the plugin reports it, mapped to one state.
type jobState struct {
	JobID          string
	State          string
	Reason         string
	Message        string
	ExitCode       int32
	NodeName       string
	Nodes          []string
	SocketDir      string
	StartTime      *metav1.Time
	CompletionTime *metav1.Time
	Ranks          []v1alpha1.RankStatus
	// StagingOut is set while the stage-out Job of an ended job runs; State is
	// then running.
	StagingOut bool
}

// observe maps the members' status, as the plugin reports it through the
// interLink API server, to one job state. pendingMessage is why the ranks that
// wait for the site are not created yet, refused why the site refused one for
// good.
func (d *Executor) observe(ctx context.Context, q *v1alpha1.OffloadRequest, req jobRequest, pendingMessage, refused string) (jobState, *refusal) {
	members := memberPods(req)
	phase := v1.PodPending
	if started(q) {
		phase = v1.PodRunning
	}
	uids := make([]string, len(members))
	var asked []*v1.Pod
	for i, m := range members {
		uids[i] = string(m.UID)
		m.Status.Phase = phase
		if !rankPending(q, i) {
			asked = append(asked, m)
		}
	}
	st := jobState{JobID: q.Status.JobID}

	statuses, err := d.InterLink.Status(ctx, req.WorkloadUID, asked)
	if err != nil {
		return st, unreachableError("asking the plugin for the status of job %s: %v", q.Status.JobID, err)
	}
	byUID := make(map[string]commonIL.PodStatus, len(statuses))
	for _, s := range statuses {
		byUID[s.PodUID] = s
	}

	anyRunning := false
	allEnded := true
	failedRank, cancelledRank, missingRank := -1, -1, -1
	waitingReason, waitingMessage := "", ""
	for i, uid := range uids {
		if rankPending(q, i) {
			// Not created yet: it waits for the site, and is not missing.
			st.Ranks = append(st.Ranks, v1alpha1.RankStatus{Rank: int32(i), State: v1alpha1.RankQueued})
			allEnded = false
			if waitingReason == "" {
				waitingReason, waitingMessage = reasonWaitingForSite, fmt.Sprintf("rank %d waits for the site: %s", i, pendingMessage)
			}
			continue
		}
		ps, found := byUID[uid]
		if !found && missingRank < 0 {
			missingRank = i
		}
		rank := v1alpha1.RankStatus{Rank: int32(i), PodUID: uid, JobID: ps.JobID, Containers: ps.Containers}
		if rank.JobID == "" && i < len(q.Status.Ranks) {
			// The job /create named, while the plugin's status does not.
			rank.JobID = q.Status.Ranks[i].JobID
		}
		var ended bool
		rank.State, rank.ExitCode, ended = rankState(ps)
		allEnded = allEnded && ended && found
		anyRunning = anyRunning || rank.State == v1alpha1.RankRunning
		if rank.State == v1alpha1.RankFailed && failedRank < 0 {
			failedRank = i
		}
		if terminatedReason(ps.Containers) == ReasonCancelled && cancelledRank < 0 {
			cancelledRank = i
		}
		if waitingReason == "" {
			waitingReason, waitingMessage = waiting(ps.Containers)
		}
		if ps.NodeName != "" {
			st.Nodes = append(st.Nodes, ps.NodeName)
		}
		st.Ranks = append(st.Ranks, rank)
	}
	head := byUID[uids[0]]
	st.NodeName = head.NodeName
	st.SocketDir = head.SocketDir
	st.ExitCode = st.Ranks[0].ExitCode
	st.StartTime, st.CompletionTime = jobTimes(statuses)

	now := time.Now()
	vanished, since := d.trackMissing(q, missingRank, now)
	switch {
	case refused != "":
		st.State, st.Reason, st.Message = jobFailed, "Rejected", "the site refused "+refused
	case cancelledRank >= 0:
		st.State, st.Reason = jobCancelled, ReasonCancelled
		st.Message = fmt.Sprintf("rank %d was cancelled outside the executor", cancelledRank)
	case allEnded:
		st.State, st.Reason, st.Message = jobCompleted, "Completed", fmt.Sprintf("job %s completed", q.Status.JobID)
		if failedRank >= 0 {
			r := st.Ranks[failedRank]
			st.State, st.Reason = jobFailed, "NonZeroExitCode"
			st.Message = fmt.Sprintf("rank %d exited with code %d", failedRank, r.ExitCode)
			if reason := terminatedReason(r.Containers); reason != "" {
				st.Reason = reason
				st.Message += " (" + reason + ")"
			}
		}
	case vanished:
		st.State, st.Reason = jobCancelled, "Vanished"
		st.Message = fmt.Sprintf("rank %d has been missing from the plugin's status since %s", missingRank, since.UTC().Format(time.RFC3339))
	case hasPendingRanks(q) && !anyRunning:
		// The ranks the site took ended or wait: the workload waits for the site.
		st.State, st.Reason, st.Message = jobQueued, waitingReason, waitingMessage
	case anyRunning || st.NodeName != "":
		st.State = jobRunning
	default:
		st.State, st.Reason, st.Message = jobQueued, waitingReason, waitingMessage
		st.NodeName, st.SocketDir, st.Nodes = "", "", nil
	}
	if isTerminalJobState(st.State) {
		at := metav1.NewTime(now)
		if st.CompletionTime == nil {
			st.CompletionTime = &at
		}
		if st.StartTime == nil {
			st.StartTime = st.CompletionTime
		}
	} else {
		st.CompletionTime = nil
	}
	return st, nil
}

// trackMissing keeps on q since when a member is missing from the plugin's
// status, as an Evicted condition that is False with reason RankMissing, and
// reports whether that is longer than Executor.LostTimeout. missingRank is -1
// when every member is there.
func (d *Executor) trackMissing(q *v1alpha1.OffloadRequest, missingRank int, now time.Time) (bool, time.Time) {
	c := meta.FindStatusCondition(q.Status.Conditions, v1alpha1.OffloadRequestEvicted)
	switch {
	case missingRank < 0:
		if c != nil && c.Status == metav1.ConditionFalse {
			meta.RemoveStatusCondition(&q.Status.Conditions, v1alpha1.OffloadRequestEvicted)
		}
		return false, time.Time{}
	case c == nil:
		setCondition(q, v1alpha1.OffloadRequestEvicted, metav1.ConditionFalse, reasonRankMissing,
			fmt.Sprintf("rank %d is missing from the plugin's status", missingRank))
		return false, now
	}
	return now.Sub(c.LastTransitionTime.Time) >= d.Config.Executor.lostTimeout, c.LastTransitionTime.Time
}

func isTerminalJobState(state string) bool {
	return state == jobCompleted || state == jobFailed || state == jobCancelled
}

// rankState is one member's state from its containers' status. ended is set
// when every container terminated.
func rankState(ps commonIL.PodStatus) (state string, exitCode int32, ended bool) {
	if len(ps.Containers) == 0 {
		return v1alpha1.RankQueued, 0, false
	}
	ended = true
	running := false
	for _, c := range ps.Containers {
		switch {
		case c.State.Terminated != nil:
			if exitCode == 0 {
				exitCode = c.State.Terminated.ExitCode
			}
		case c.State.Running != nil:
			running = true
			ended = false
		default:
			ended = false
		}
	}
	switch {
	case ended && exitCode != 0:
		return v1alpha1.RankFailed, exitCode, true
	case ended:
		return v1alpha1.RankCompleted, 0, true
	case running || ps.NodeName != "":
		return v1alpha1.RankRunning, 0, false
	}
	return v1alpha1.RankQueued, 0, false
}

func terminatedReason(containers []v1.ContainerStatus) string {
	for _, c := range containers {
		if t := c.State.Terminated; t != nil && t.Reason != "" {
			return t.Reason
		}
	}
	return ""
}

// waiting returns the first reason a container waits for, and its message.
func waiting(containers []v1.ContainerStatus) (reason, message string) {
	for _, c := range containers {
		if w := c.State.Waiting; w != nil && w.Reason != "" {
			return w.Reason, w.Message
		}
	}
	return "", ""
}

// jobTimes are the first start and the last end of any member's containers.
func jobTimes(statuses []commonIL.PodStatus) (start, end *metav1.Time) {
	for _, s := range statuses {
		for _, c := range s.Containers {
			var started, finished metav1.Time
			switch {
			case c.State.Running != nil:
				started = c.State.Running.StartedAt
			case c.State.Terminated != nil:
				started, finished = c.State.Terminated.StartedAt, c.State.Terminated.FinishedAt
			}
			if !started.IsZero() && (start == nil || started.Before(start)) {
				start = &started
			}
			if !finished.IsZero() && (end == nil || end.Before(&finished)) {
				end = &finished
			}
		}
	}
	return start, end
}

// templateExposesPorts reports whether a container of the template declares a port.
func templateExposesPorts(t v1.PodTemplateSpec) bool {
	for _, c := range t.Spec.Containers {
		if len(c.Ports) > 0 {
			return true
		}
	}
	return false
}

// syncShadow keeps the workload's shadow in step with the job: one Ensure per
// poll while it runs on a known node, one more with Terminal set once it ended.
// It records the shadow's names, and its pod's IP and readiness, in q's status.
func (d *Executor) syncShadow(ctx context.Context, q *v1alpha1.OffloadRequest, req jobRequest, st jobState) {
	cfg := d.Config
	terminal := isTerminalJobState(st.State)
	if !cfg.Shadow.Enabled || d.Shadow == nil || (terminal && d.shadowEnded(q.UID)) {
		return
	}
	ps := req.PodSets[0]
	reverse, aerr := d.reverseTunnel(req)
	if aerr != nil {
		log.L.Warning("executor: shadow of workload ", req.WorkloadUID, " without its reverse tunnel: ", aerr.msg)
	}
	if !templateExposesPorts(ps.Template) && reverse == nil && !cfg.Shadow.AlwaysShadowEnabled() {
		return
	}
	// A tunnel needs the node. A shadow that only streams logs starts as soon as
	// the job runs, also on a backend that reports no node.
	tunnels := reverse != nil || shadow.ForwardsPorts(ps.Template)
	if !terminal && (st.State != jobRunning || (st.NodeName == "" && tunnels)) {
		return
	}
	spec := ShadowSpec{
		WorkloadUID:  req.WorkloadUID,
		WorkloadName: ownerName(req),
		Namespace:    req.Namespace,
		Template:     ps.Template,
		NodeName:     st.NodeName,
		SocketDir:    st.SocketDir,
		Terminal:     terminal,
		Reverse:      reverse,
		Labels:       req.OwnerLabels,
	}
	ref, err := d.Shadow.Ensure(ctx, spec)
	if err != nil {
		log.L.Warning("executor: shadow of workload ", spec.WorkloadUID, ": ", err)
		return
	}
	q.Status.Shadow = &v1alpha1.ShadowReference{Deployment: ref.Deployment, Service: ref.Service, PodIP: ref.PodIP, Ready: ref.Ready}
	if terminal {
		d.mu.Lock()
		d.shadowsEnded[q.UID] = true
		d.mu.Unlock()
	}
}

func (d *Executor) shadowEnded(uid types.UID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.shadowsEnded[uid]
}

// cancel deletes the members of q, which cancels its job unless it ended, and
// removes its shadow and its stage Jobs. On an error the caller keeps the
// executor's finalizer, so that it is tried again.
func (d *Executor) cancel(ctx context.Context, q *v1alpha1.OffloadRequest) error {
	req := jobRequestOf(q)
	uid := req.WorkloadUID
	if err := d.deleteMembers(ctx, uid, createdMembers(q, req)); err != nil {
		return fmt.Errorf("deleting the job of workload %s: %w", uid, err)
	}
	if err := d.shadowManager().Delete(ctx, uid); err != nil {
		return fmt.Errorf("deleting the shadow of workload %s: %w", uid, err)
	}
	if err := d.deleteStageJobs(ctx, uid); err != nil {
		return fmt.Errorf("deleting the stage Jobs of workload %s: %w", uid, err)
	}
	d.mu.Lock()
	delete(d.shadowsEnded, q.UID)
	d.mu.Unlock()
	log.L.Infof("executor: request %s/%s deleted (job %s)", q.Namespace, q.Name, q.Status.JobID)
	return nil
}
