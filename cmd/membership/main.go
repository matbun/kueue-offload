// Command membership is the membership sync controller: it turns the Slurm
// associations of one site into Kueue queues, namespace labels and agents.
package main

import (
	"flag"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	"github.com/interlink-hq/interlink-dispatch/pkg/membership"
)

func main() {
	var (
		site, namespace        string
		metricsAddr, probeAddr string
		leaderElect            bool
	)
	flag.StringVar(&site, "site", "", "name of the SiteConfig whose memberships this controller keeps")
	flag.StringVar(&namespace, "namespace", "interlink-membership", "the controller's namespace, which holds the associations and the agent template")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address, 0 disables it")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe address")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
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

	managed := labels.SelectorFromSet(labels.Set{membership.ManagedByLabel: membership.ManagedByValue})
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          leaderElect,
		LeaderElectionID:        "interlink-membership-" + site,
		LeaderElectionNamespace: namespace,
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}:  {Namespaces: map[string]cache.Config{namespace: {}}},
			&appsv1.Deployment{}: {Label: managed},
			&kueue.LocalQueue{}:  {Label: managed},
		}},
	})
	if err != nil {
		logger.Error(err, "creating manager")
		os.Exit(1)
	}

	r := &membership.Reconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Recorder:  mgr.GetEventRecorder("interlink-membership"),
		Site:      site,
		Namespace: namespace,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		logger.Error(err, "setting up membership controller")
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
