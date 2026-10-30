package offload

import (
	"context"
	"strings"
	"testing"

	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// Attempts the start estimate deferred are not retries: a loss on the attempt
// after them is retried within backoffLimitCount, and Kueue's retryCount stays
// the count of every Retry.
func TestDeferralsAreNotRetries(t *testing.T) {
	for _, tt := range []struct {
		name      string
		deferrals int32
		want      kueue.CheckState
		message   string
	}{
		{name: "every earlier attempt deferred", deferrals: 3, want: kueue.CheckStateRetry},
		{name: "one earlier loss", deferrals: 2, want: kueue.CheckStateRejected, message: "giving up after 1 retries"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, withRetryStrategy(&kueue.ProvisioningRequestRetryStrategy{BackoffLimitCount: ptr.To[int32](1)}))
			e.mutateWorkload(func(wl *kueue.Workload) { findCheck(wl, "hpc-a").RetryCount = ptr.To[int32](3) })
			e.reconcile()
			q := e.request(4)
			q.Spec.Deferrals = tt.deferrals
			if err := e.c.Update(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			e.takes(4)
			e.report(4, e.evicted("scancel by an operator"))
			e.reconcile()
			st := e.check("hpc-a")
			if st.State != tt.want || !strings.Contains(st.Message, tt.message) {
				t.Errorf("check = %s %q, want %s", st.State, st.Message, tt.want)
			}
			if tt.want == kueue.CheckStateRetry && ptr.Deref(st.RetryCount, -1) != 3 {
				t.Errorf("retryCount %v, want Kueue's 3", st.RetryCount)
			}
		})
	}
}
