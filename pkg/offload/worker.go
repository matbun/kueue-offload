package offload

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// Defaults of the WorkerReconciler.
const (
	workerProbeInterval = time.Minute
	workerDialTimeout   = 10 * time.Second
)

// WorkerReconciler keeps each OffloadWorker's Active condition from site-level
// facts: a valid loginEndpoint and a TCP connection to it. It never looks at a
// tenant's executor.
type WorkerReconciler struct {
	Client client.Client
	// Dial opens the probe connection; nil uses a net.Dialer.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
	// Interval is the time between probes; 0 means a minute.
	Interval time.Duration
}

func (r *WorkerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	w := &v1alpha1.OffloadWorker{}
	if err := r.Client.Get(ctx, req.NamespacedName, w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	cond := metav1.Condition{
		Type:               v1alpha1.OffloadWorkerActive,
		Status:             metav1.ConditionTrue,
		Reason:             "Active",
		Message:            fmt.Sprintf("login endpoint %s reachable", w.Spec.LoginEndpoint),
		ObservedGeneration: w.Generation,
	}
	if err := validEndpoint(w.Spec.LoginEndpoint); err != nil {
		cond.Status, cond.Reason = metav1.ConditionFalse, "BadConfig"
		cond.Message = fmt.Sprintf("loginEndpoint %q: %v", w.Spec.LoginEndpoint, err)
	} else if err := r.probe(ctx, w.Spec.LoginEndpoint); err != nil {
		cond.Status, cond.Reason = metav1.ConditionFalse, "Unreachable"
		cond.Message = fmt.Sprintf("login endpoint %s: %v", w.Spec.LoginEndpoint, err)
	}
	interval := r.Interval
	if interval <= 0 {
		interval = workerProbeInterval
	}
	if old := apimeta.FindStatusCondition(w.Status.Conditions, v1alpha1.OffloadWorkerActive); sameCondition(old, &cond) {
		return ctrl.Result{RequeueAfter: interval}, nil
	}
	orig := w.DeepCopy()
	apimeta.SetStatusCondition(&w.Status.Conditions, cond)
	return ctrl.Result{RequeueAfter: interval}, client.IgnoreNotFound(r.Client.Status().Patch(ctx, w, client.MergeFrom(orig)))
}

func (r *WorkerReconciler) probe(ctx context.Context, endpoint string) error {
	ctx, cancel := context.WithTimeout(ctx, workerDialTimeout)
	defer cancel()
	dial := r.Dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	conn, err := dial(ctx, "tcp", endpoint)
	if err != nil {
		return err
	}
	return conn.Close()
}

// validEndpoint accepts host:port with a port number.
func validEndpoint(endpoint string) error {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return err
	}
	if host == "" {
		return fmt.Errorf("no host")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port %q is not a number from 1 to 65535", port)
	}
	return nil
}

// SetupWithManager registers the reconciler. Its own status writes do not
// trigger a new probe.
func (r *WorkerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("offloadworker").
		For(&v1alpha1.OffloadWorker{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}
