// Command executor runs the OffloadRequests of its namespace and site through an
// interLink API server, in the tenant's namespace. "executor logs" is the
// shadow's job-logs container.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/containerd/containerd/log"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executor"
)

func main() {
	// The shadow's job-logs container copies the workload's log from the executor.
	// It reads no configuration.
	if len(os.Args) > 1 && os.Args[1] == "logs" {
		ctx := ctrl.SetupSignalHandler()
		if err := executor.RunLogsCommand(ctx, os.Args[2:], os.Stdout, os.Stderr); err != nil && ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	configPath := flag.String("config", "/etc/kueue-offload/executor.yaml", "the executor's configuration file")
	level := flag.String("log-level", "info", "log level: debug, info, warn or error")
	flag.Parse()
	if err := log.SetLevel(*level); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	// controller-runtime's own logs: reconcile errors.
	ctrl.SetLogger(zap.New())

	cfg, err := executor.LoadConfig(*configPath)
	if err != nil {
		log.L.Fatal("executor: ", err)
	}
	site, _ := cfg.Site()
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		log.L.Fatal("executor: no Kubernetes client configuration: ", err)
	}
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.L.Fatal("executor: ", err)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, kueue.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			log.L.Fatal("executor: ", err)
		}
	}
	// The cache holds the OffloadRequests of the executor's namespace and site
	// only, which a namespaced Role lets it watch.
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{cfg.Executor.Namespace: {}},
			ByObject: map[client.Object]cache.ByObject{
				&v1alpha1.OffloadRequest{}: {Label: labels.SelectorFromSet(labels.Set{v1alpha1.SiteLabel: site})},
			},
		},
	})
	if err != nil {
		log.L.Fatal("executor: ", err)
	}

	var shadows executor.ShadowManager
	if cfg.Shadow.Enabled {
		if shadows, err = executor.NewShadowManager(kube, cfg); err != nil {
			log.L.Fatal("executor: shadow: ", err)
		}
	}
	// No overall timeout: a followed log stays open for the whole job.
	il := &executor.InterLink{URL: cfg.InterLink.URL, HTTP: &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ResponseHeaderTimeout: 5 * time.Minute,
		IdleConnTimeout:       90 * time.Second,
	}}}
	d := executor.New(cfg, il, kube, mgr.GetClient(), mgr.GetAPIReader(), shadows)
	if err := d.SetupWithManager(mgr); err != nil {
		log.L.Fatal("executor: ", err)
	}

	mux := http.NewServeMux()
	d.RegisterRoutes(mux)
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		}()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})); err != nil {
		log.L.Fatal("executor: ", err)
	}
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		d.WatchCapabilities(ctx)
		return nil
	})); err != nil {
		log.L.Fatal("executor: ", err)
	}

	log.L.Infof("executor: namespace %s, site %s, interLink API server %s, listening on %s", cfg.Executor.Namespace, site, cfg.InterLink.URL, cfg.Listen)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.L.Fatal("executor: ", err)
	}
}
