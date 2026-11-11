// Command offload-controller is the Kueue admission check controller that hands
// Workloads whose quota is reserved on an HPC flavor to the executor of their
// namespace, as OffloadRequests. One Deployment serves every AdmissionCheck with
// controllerName interlink.eu/offload, and runs the virtual node of each
// OffloadConfig, which the pods of admitted Workloads are bound to.
package main

import (
	"flag"
	"net"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/node"
	"github.com/matbun/kueue-offload/pkg/offload"
)

func main() {
	var (
		namespace              string
		metricsAddr, probeAddr string
		leaderElect            bool
		kubeletPort            int
		nodeIP                 string
		approveServingCSR      bool
	)
	flag.StringVar(&namespace, "namespace", "kueue-offload", "namespace of the leader election lease")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address, 0 disables it")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe address")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	flag.IntVar(&kubeletPort, "kubelet-port", node.DefaultKubeletPort, "port of the virtual nodes' kubelet API")
	flag.StringVar(&nodeIP, "node-ip", os.Getenv("POD_IP"), "address of the virtual nodes, this pod's IP (default $POD_IP)")
	flag.BoolVar(&approveServingCSR, "approve-kubelet-serving-csr", true, "approve the controller's own kubelet serving CSRs")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger := ctrl.Log.WithName("setup")
	if net.ParseIP(nodeIP) == nil {
		logger.Error(nil, "the virtual nodes need an address: set --node-ip or POD_IP", "nodeIP", nodeIP)
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kueue.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	// The binder and the virtual nodes see only the pods of admitted offload
	// Workloads, which the check's podSetUpdates label.
	offloaded, err := labels.NewRequirement(v1alpha1.NodeLabel, selection.Exists, nil)
	utilruntime.Must(err)
	restConfig := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}: {Label: labels.NewSelector().Add(*offloaded)},
		}},
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
	kube := kubernetes.NewForConfigOrDie(restConfig)
	if err := (&node.NodeReconciler{
		Client:      mgr.GetClient(),
		Kube:        kube,
		Recorder:    mgr.GetEventRecorder("kueue-offload-node"),
		NodeIP:      nodeIP,
		KubeletPort: int32(kubeletPort),
	}).SetupWithManager(mgr); err != nil {
		logger.Error(err, "setting up the virtual node controller")
		os.Exit(1)
	}
	if err := (&node.PodStatusReconciler{Client: mgr.GetClient(), NodeIP: nodeIP}).SetupWithManager(mgr); err != nil {
		logger.Error(err, "setting up the virtual node pod controller")
		os.Exit(1)
	}
	cert, err := node.NewServingCert(kube, nodeIP)
	if err != nil {
		logger.Error(err, "setting up the kubelet serving certificate")
		os.Exit(1)
	}
	if approveServingCSR {
		if err := (&node.CSRApprover{Client: mgr.GetClient(), Kube: kube}).SetupWithManager(mgr); err != nil {
			logger.Error(err, "setting up the kubelet serving CSR approver")
			os.Exit(1)
		}
	}
	server := &node.Server{Port: int32(kubeletPort), Kube: kube, Provider: &node.Provider{Reader: mgr.GetClient()}, Cert: cert}
	if err := mgr.Add(server); err != nil {
		logger.Error(err, "setting up the kubelet API")
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
