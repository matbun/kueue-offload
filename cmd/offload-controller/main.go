// Command offload-controller is the Kueue admission check controller that submits
// admitted Workloads to an HPC site through the per-namespace executors.
package main

import (
	"flag"
	"net/http"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/offload"
)

func main() {
	var (
		site, namespace, tokenFile string
		metricsAddr, probeAddr     string
		leaderElect                bool
		resync, executorTimeout    time.Duration
	)
	flag.StringVar(&site, "site", "", "name of the OffloadConfig this offload controller serves")
	flag.StringVar(&namespace, "namespace", "kueue-offload", "namespace of the OffloadRecords")
	flag.StringVar(&tokenFile, "token-file", "/var/run/secrets/interlink/token", "bearer token sent to the executors")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address, 0 disables it")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe address")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	flag.DurationVar(&resync, "resync-interval", time.Minute, "how often OffloadRecords of deleted Workloads are cleaned up")
	flag.DurationVar(&executorTimeout, "executor-timeout", 30*time.Second, "timeout of one call to an executor")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger := ctrl.Log.WithName("setup")

	if site == "" {
		logger.Error(nil, "--site is required")
		os.Exit(2)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kueue.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          leaderElect,
		LeaderElectionID:        "kueue-offload-" + site,
		LeaderElectionNamespace: namespace,
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&v1alpha1.OffloadRecord{}: {Namespaces: map[string]cache.Config{namespace: {}}},
		}},
	})
	if err != nil {
		logger.Error(err, "creating manager")
		os.Exit(1)
	}

	executor := &offload.ExecutorClient{
		HTTP:  &http.Client{Timeout: executorTimeout},
		Token: &offload.TokenSource{Path: tokenFile},
	}
	wr := &offload.WorkloadReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Checks:    &offload.SSACheckWriter{Client: mgr.GetClient()},
		Executor:  executor,
		Recorder:  mgr.GetEventRecorder("kueue-offload"),
		Site:      site,
		Namespace: namespace,
	}
	if err := wr.SetupWithManager(mgr, resync); err != nil {
		logger.Error(err, "setting up workload controller")
		os.Exit(1)
	}
	if err := (&offload.AdmissionCheckReconciler{Client: mgr.GetClient(), Site: site}).SetupWithManager(mgr); err != nil {
		logger.Error(err, "setting up admission check controller")
		os.Exit(1)
	}
	utilruntime.Must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	utilruntime.Must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	logger.Info("starting", "site", site, "namespace", namespace)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error(err, "manager stopped")
		os.Exit(1)
	}
}
