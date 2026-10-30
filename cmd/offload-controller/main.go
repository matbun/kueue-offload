// Command offload-controller is the Kueue admission check controller that hands
// Workloads whose quota is reserved on an HPC flavor to the executor of their
// namespace, as OffloadRequests. One Deployment serves every AdmissionCheck with
// controllerName interlink.eu/offload.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/offload"
)

func main() {
	var (
		namespace              string
		metricsAddr, probeAddr string
		leaderElect            bool
	)
	flag.StringVar(&namespace, "namespace", "kueue-offload", "namespace of the leader election lease")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address, 0 disables it")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe address")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kueue.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          leaderElect,
		LeaderElectionID:        "kueue-offload",
		LeaderElectionNamespace: namespace,
	})
	if err != nil {
		logger.Error(err, "creating manager")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := offload.SetupIndexes(ctx, mgr.GetFieldIndexer()); err != nil {
		logger.Error(err, "setting up indexes")
		os.Exit(1)
	}
	configs, err := offload.NewConfigHelper(mgr.GetClient())
	if err != nil {
		logger.Error(err, "setting up the OffloadConfig helper")
		os.Exit(1)
	}

	wr := &offload.WorkloadReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Checks:    &offload.SSACheckWriter{Client: mgr.GetClient()},
		Configs:   configs,
		Recorder:  mgr.GetEventRecorder("kueue-offload"),
	}
	if err := wr.SetupWithManager(mgr); err != nil {
		logger.Error(err, "setting up workload controller")
		os.Exit(1)
	}
	if err := (&offload.AdmissionCheckReconciler{Client: mgr.GetClient(), Configs: configs}).SetupWithManager(mgr); err != nil {
		logger.Error(err, "setting up admission check controller")
		os.Exit(1)
	}
	if err := (&offload.WorkerReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		logger.Error(err, "setting up offload worker controller")
		os.Exit(1)
	}
	utilruntime.Must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	utilruntime.Must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	logger.Info("starting", "controllerName", offload.ControllerName)
	if err := mgr.Start(ctx); err != nil {
		logger.Error(err, "manager stopped")
		os.Exit(1)
	}
}
