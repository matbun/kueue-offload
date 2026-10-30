#!/usr/bin/env bash
# End-to-end test of kueue-offload with the HTCondor plugin (docs/backends.md
# section 6, test 3) on a local kind cluster: Kueue, the offload controller, one
# site (hpc-a) and one tenant namespace whose executor drives a stock interLink API
# server and the plugin's own container, an htcondor/mini pool with the plugin.
# It prints PASS or FAIL per case and exits 1 when a case failed.
#
#   test/htcondor/run.sh
#
# Environment:
#   PLUGIN_SRC     interlink-htcondor-plugin checkout (default ../interlink-htcondor-plugin)
#   INTERLINK_SRC  interLink checkout the API server is built from, with
#                  PodStatus.SocketDir (default ../interLink-hardening)
#   SKIP_BUILD=1   use the images built by an earlier run
#   KEEP=1         keep the cluster at the end
#   CLUSTER        kind cluster name (default kueue-offload-htcondor)
#   WORK           directory for the kubeconfig and the diagnostics (default a new
#                  temporary directory)
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
PLUGIN_SRC=${PLUGIN_SRC:-$REPO/../interlink-htcondor-plugin}
INTERLINK_SRC=${INTERLINK_SRC:-$REPO/../interLink-hardening}
CLUSTER=${CLUSTER:-kueue-offload-htcondor}
NODE_IMAGE=${NODE_IMAGE:-kindest/node:v1.34.3}
KUEUE_MANIFESTS=https://github.com/kubernetes-sigs/kueue/releases/download/v0.16.9/manifests.yaml
WORK=${WORK:-$(mktemp -d -t kueue-offload-htcondor.XXXXXX)}
TIMEOUT=${TIMEOUT:-300}
NS=team-a

IMG_KO=kueue-offload-e2e/kueue-offload:dev
IMG_API=kueue-offload-e2e/interlink:dev
IMG_PLUGIN=kueue-offload-e2e/htcondor-plugin:dev

mkdir -p "$WORK"
export KUBECONFIG=$WORK/kubeconfig
FAILED=0
RESULTS=()

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
pass() { RESULTS+=("PASS $1: $2"); log "PASS $1: $2"; }
fail() { RESULTS+=("FAIL $1: $2"); log "FAIL $1: $2"; FAILED=1; }

# until_ok <seconds> <command...> runs the command every 2 s until it succeeds.
until_ok() {
	local deadline=$((SECONDS + $1))
	shift
	until "$@" >/dev/null 2>&1; do
		[ "$SECONDS" -lt "$deadline" ] || return 1
		sleep 2
	done
}

pool() { kubectl -n "$NS" exec deploy/kueue-offload-executor -c plugin -- "$@"; }
job_cond() { kubectl -n "$NS" get job "$1" -o jsonpath="{.status.conditions[?(@.type==\"$2\")].$3}"; }
job_ended() { [ "$(job_cond "$1" Complete status)" = True ] || [ "$(job_cond "$1" Failed status)" = True ]; }
workload_of() {
	local uid
	uid=$(kubectl -n "$NS" get job "$1" -o jsonpath='{.metadata.uid}')
	kubectl -n "$NS" get workloads -l "kueue.x-k8s.io/job-uid=$uid" -o jsonpath='{.items[0].metadata.name}'
}
workload_uid() { kubectl -n "$NS" get workload "$1" -o jsonpath='{.metadata.uid}'; }
check_field() { kubectl -n "$NS" get workload "$1" -o jsonpath="{.status.admissionChecks[?(@.name==\"hpc-a\")].$2}"; }
check_is() { [ "$(check_field "$1" state)" = "$2" ]; }
# request_of names the newest OffloadRequest of a Workload UID.
request_of() { kubectl -n "$NS" get offloadrequests -o jsonpath="{range .items[?(@.spec.workload.uid==\"$1\")]}{.spec.attempt} {.metadata.name}{\"\\n\"}{end}" | sort -n | tail -1 | cut -d' ' -f2; }
request_field() { kubectl -n "$NS" get offloadrequest "$(request_of "$1")" -o jsonpath="{.status.$2}"; }
request_running() { [ "$(request_field "$1" 'conditions[?(@.type=="Admitted")].status')" = True ]; }
shadow_logs() {
	local dep
	dep=$(kubectl -n "$NS" get deploy -l "interlink.eu/shadow-of=$1" -o jsonpath='{.items[0].metadata.name}')
	[ -n "$dep" ] && kubectl -n "$NS" logs "deploy/$dep" -c job-logs
}
shadow_shows() { shadow_logs "$1" | grep -q "$2"; }

diagnostics() {
	local d=$WORK/diag
	mkdir -p "$d"
	kubectl get workloads,jobs,deploy,pods -A -o wide >"$d/objects.txt" 2>&1 || true
	kubectl -n "$NS" get workloads -o yaml >"$d/workloads.yaml" 2>&1 || true
	kubectl -n "$NS" get offloadrequests -o yaml >"$d/offloadrequests.yaml" 2>&1 || true
	kubectl get offloadworkers -o yaml >"$d/offloadworkers.yaml" 2>&1 || true
	kubectl get events -A --sort-by=.lastTimestamp >"$d/events.txt" 2>&1 || true
	kubectl -n kueue-offload logs deploy/kueue-offload >"$d/controller.log" 2>&1 || true
	for c in executor interlink plugin; do
		kubectl -n "$NS" logs deploy/kueue-offload-executor -c "$c" >"$d/$c.log" 2>&1 || true
	done
	pool condor_q -allusers -nobatch >"$d/condor_q.txt" 2>&1 || true
	pool condor_history -limit 50 >"$d/condor_history.txt" 2>&1 || true
	log "diagnostics in $d"
}

cleanup() {
	local rc=$?
	if [ "$rc" != 0 ] || [ "$FAILED" != 0 ]; then
		diagnostics
	fi
	if [ "${KEEP:-0}" = 1 ]; then
		log "keeping cluster $CLUSTER: export KUBECONFIG=$KUBECONFIG"
	else
		kind delete cluster --name "$CLUSTER" >&2 || true
	fi
}

setup() {
	if [ "${SKIP_BUILD:-0}" != 1 ]; then
		log "building $IMG_KO, $IMG_API and $IMG_PLUGIN"
		docker build -q -t "$IMG_KO" "$REPO" >&2
		docker build -q -f "$INTERLINK_SRC/docker/Dockerfile.interlink" -t "$IMG_API" "$INTERLINK_SRC" >&2
		docker build -q -f "$PLUGIN_SRC/docker/Dockerfile" -t "$IMG_PLUGIN" "$PLUGIN_SRC" >&2
	fi
	if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
		kind delete cluster --name "$CLUSTER" >&2
	fi
	log "creating kind cluster $CLUSTER"
	kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --kubeconfig "$KUBECONFIG" --wait 120s >&2
	kind load docker-image --name "$CLUSTER" "$IMG_KO" "$IMG_API" "$IMG_PLUGIN" >&2

	log "installing Kueue"
	kubectl apply --server-side -f "$KUEUE_MANIFESTS" >/dev/null
	kubectl -n kueue-system rollout status deploy/kueue-controller-manager --timeout=300s >&2

	log "installing kueue-offload, site hpc-a and tenant $NS"
	kubectl apply --server-side -k "$HERE/deploy" >/dev/null
	kubectl wait --for condition=established --timeout=60s crd/offloadconfigs.interlink.eu crd/offloadrequests.interlink.eu crd/offloadworkers.interlink.eu >/dev/null
	# Kueue's webhooks answer a while after its rollout.
	until_ok 180 kubectl apply --server-side -f "$HERE/site.yaml"
	until_ok 60 kubectl apply --server-side -f "$HERE/tenant.yaml"
	kubectl -n kueue-offload rollout status deploy/kueue-offload --timeout=180s >&2
	# The executor is ready once the plugin answers its ping through the API server.
	kubectl -n "$NS" rollout status deploy/kueue-offload-executor --timeout=600s >&2
	until_ok 300 sh -c "kubectl -n $NS exec deploy/kueue-offload-executor -c plugin -- condor_status -af Name | grep -q ."
	until_ok 120 sh -c "kubectl get clusterqueue hpc-a-proj42 -o jsonpath='{.status.conditions[?(@.type==\"Active\")].status}' | grep -qx True"
	log "pool slots: $(pool condor_status -af Name | tr '\n' ' ')"
}

# single: a single-pod Job succeeds and its output is read with kubectl logs on
# the shadow.
case_single() {
	if ! until_ok "$TIMEOUT" job_ended single; then
		fail single "Job single did not end in ${TIMEOUT}s"
		return
	fi
	if [ "$(job_cond single Complete status)" != True ]; then
		fail single "Job single failed: $(job_cond single Failed message)"
		return
	fi
	local uid line
	uid=$(workload_uid "$(workload_of single)")
	if ! until_ok 120 shadow_shows "$uid" 'hello from rank 0 of 1'; then
		fail single "Job Complete, but the shadow's job-logs never showed the output: $(shadow_logs "$uid" | tail -3 | tr '\n' '|')"
		return
	fi
	line=$(shadow_logs "$uid" | grep -m1 'hello from rank')
	pass single "Job Complete ($(job_cond single Complete message)); kubectl logs on the shadow, container job-logs: \"$line\""
}

# submit-lines: the flavor's account and wall time reach the submit file and the
# job's ClassAd.
case_submit_lines() {
	local uid jid lines ad
	uid=$(workload_uid "$(workload_of single)")
	jid=$(request_field "$uid" jobID)
	lines=$(pool sh -c 'cat /utils/.interlink/single-0-*/single-0-*.jdl' | grep -E '^(accounting_group|\+MaxWallTimeMins)' | tr '\n' '|')
	ad=$(pool condor_history "$jid" -af AcctGroup MaxWallTimeMins)
	if [[ "$lines" == *"accounting_group = group_proj42|"*"+MaxWallTimeMins = 20|" ]] && [ "$ad" = "group_proj42 20" ]; then
		pass submit-lines "submit file of job $jid: \"$lines\"; condor_history $jid -af AcctGroup MaxWallTimeMins: \"$ad\""
	else
		fail submit-lines "submit file of job $jid: \"$lines\"; condor_history: \"$ad\""
	fi
}

# indep: a Job of three independent pods runs as three HTCondor jobs.
case_indep() {
	if ! until_ok "$TIMEOUT" job_ended indep; then
		fail indep "Job indep did not end in ${TIMEOUT}s"
		return
	fi
	local uid ids hist n
	uid=$(workload_uid "$(workload_of indep)")
	ids=$(request_field "$uid" jobID)
	n=$(tr ',' '\n' <<<"$ids" | grep -c '^[0-9]*$')
	hist=$(for id in ${ids//,/ }; do pool condor_history "$id" -af ClusterId JobStatus ExitCode; done | tr '\n' '|')
	if [ "$(job_cond indep Complete status)" = True ] && [ "$n" = 3 ] && [ "$(grep -o ' 4 0|' <<<"$hist" | wc -l)" = 3 ]; then
		pass indep "Job Complete with $(kubectl -n "$NS" get job indep -o jsonpath='{.status.succeeded}') succeeded; jobID \"$ids\"; condor_history ClusterId JobStatus ExitCode: \"$hist\""
	else
		fail indep "Job conditions $(kubectl -n "$NS" get job indep -o jsonpath='{.status.conditions[*].type}'); jobID \"$ids\"; condor_history: \"$hist\""
	fi
}

# gang: two pods that must share one allocation are refused, naming the missing
# capability, and nothing reaches the plugin.
case_gang() {
	local wl msg dirs
	if ! until_ok 60 workload_of gang || ! until_ok "$TIMEOUT" check_is "$(workload_of gang)" Rejected; then
		fail gang "check state \"$(check_field "$(workload_of gang)" state)\": $(check_field "$(workload_of gang)" message)"
		return
	fi
	wl=$(workload_of gang)
	msg=$(check_field "$wl" message)
	dirs=$(pool sh -c 'ls -d /utils/.interlink/gang-* 2>/dev/null | wc -l')
	if [[ "$msg" == *'"pod-group"'* && "$msg" == *offload.interlink.eu/independent-pods* ]] && [ "$dirs" = 0 ]; then
		pass gang "check Rejected: \"$msg\"; job directories of gang members in the plugin: $dirs"
	else
		fail gang "check Rejected: \"$msg\"; job directories of gang members in the plugin: $dirs"
	fi
}

# exit3: a job that exits 3 fails the Job, with the code in the message.
case_exit3() {
	if ! until_ok "$TIMEOUT" job_ended exit3; then
		fail exit3 "Job exit3 did not end in ${TIMEOUT}s"
		return
	fi
	local msg
	msg=$(job_cond exit3 Failed message)
	if [ "$(job_cond exit3 Failed status)" = True ] && [[ "$msg" == *"code 3"* ]]; then
		pass exit3 "Job Failed: \"$msg\""
	else
		fail exit3 "Job conditions $(kubectl -n "$NS" get job exit3 -o jsonpath='{.status.conditions[*].type}'): \"$msg\""
	fi
}

# removed: condor_rm from inside the pool sends the check to Retry.
case_removed() {
	local wl uid jid ev
	if ! until_ok 60 workload_of removed; then
		fail removed "no Workload for Job removed"
		return
	fi
	wl=$(workload_of removed)
	uid=$(workload_uid "$wl")
	if ! until_ok "$TIMEOUT" request_running "$uid"; then
		fail removed "the job never ran: request reason \"$(request_field "$uid" reason)\": $(request_field "$uid" message)"
		return
	fi
	jid=$(request_field "$uid" jobID)
	pool condor_rm "$jid" >&2
	retried() { kubectl -n "$NS" get events --field-selector "involvedObject.name=$wl,reason=Retry" -o jsonpath='{.items[*].message}' | grep -q .; }
	if until_ok 120 retried; then
		ev=$(kubectl -n "$NS" get events --field-selector "involvedObject.name=$wl,reason=Retry" -o jsonpath='{.items[0].message}')
		pass removed "condor_rm $jid; Workload event Retry: \"$ev\""
	else
		fail removed "condor_rm $jid; check state \"$(check_field "$wl" state)\": $(check_field "$wl" message)"
	fi
	kubectl -n "$NS" delete job removed --wait=false >/dev/null
}

trap cleanup EXIT
setup
log "submitting the Jobs"
kubectl apply -f "$HERE/jobs.yaml" >/dev/null
for c in single submit_lines indep gang exit3 removed; do
	"case_$c" || fail "$c" "the case script stopped"
done
printf '%s\n' "${RESULTS[@]}"
exit "$FAILED"
