package dispatcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	checkWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "interlink_dispatch_check_state_writes_total",
		Help: "Admission check states written on Workloads, by site and state.",
	}, []string{"site", "state"})

	submitLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "interlink_dispatch_submit_duration_seconds",
		Help:    "Duration of POST /submit calls to agents, by site and outcome.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
	}, []string{"site", "outcome"})
)

func init() {
	metrics.Registry.MustRegister(checkWrites, submitLatency)
}
