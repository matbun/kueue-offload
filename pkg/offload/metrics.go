package offload

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var checkWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "kueue_offload_check_state_writes_total",
	Help: "Admission check states written on Workloads, by check and state.",
}, []string{"check", "state"})

func init() {
	metrics.Registry.MustRegister(checkWrites)
}
