// Command dispatcher is the Kueue admission check controller that submits
// admitted Workloads to an HPC site through the per-namespace agents.
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

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
	"github.com/interlink-hq/interlink-dispatch/pkg/dispatcher"
)

func main() {
	var (
		site, namespace, tokenFile string
		metricsAddr, probeAddr     string
		leaderElect                bool
		resync, agentTimeout       time.Duration
	)
	flag.StringVar(&site, "site", "", "name of the SiteConfig this dispatcher serves")
	flag.StringVar(&namespace, "namespace", "interlink-dispatch", "namespace of the SubmitRecords")
	flag.StringVar(&tokenFile, "token-file", "/var/run/secrets/interlink/token", "bearer token sent to the agents")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address, 0 disables it")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe address")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	flag.DurationVar(&resync, "resync-interval", time.Minute, "how often SubmitRecords of deleted Workloads are cleaned up")
	flag.DurationVar(&agentTimeout, "agent-timeout", 30*time.Second, "timeout of one call to an agent")
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
		LeaderElectionID:        "interlink-dispatch-" + site,
		LeaderElectionNamespace: namespace,
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&v1alpha1.SubmitRecord{}: {Namespaces: map[string]cache.Config{namespace: {}}},
		}},
	})
	if err != nil {
		logger.Error(err, "creating manager")
		os.Exit(1)
	}

	agent := &dispatcher.AgentClient{
		HTTP:  &http.Client{Timeout: agentTimeout},
		Token: &dispatcher.TokenSource{Path: tokenFile},
	}
	wr := &dispatcher.WorkloadReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Checks:    &dispatcher.SSACheckWriter{Client: mgr.GetClient()},
		Agent:     agent,
		Recorder:  mgr.GetEventRecorder("interlink-dispatch"),
		Site:      site,
		Namespace: namespace,
	}
	if err := wr.SetupWithManager(mgr, resync); err != nil {
		logger.Error(err, "setting up workload controller")
		os.Exit(1)
	}
	if err := (&dispatcher.AdmissionCheckReconciler{Client: mgr.GetClient(), Site: site}).SetupWithManager(mgr); err != nil {
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
