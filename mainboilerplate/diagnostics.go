package mainboilerplate

import (
	_ "expvar" // Import for /debug/vars
	"fmt"
	"net/http"
	_ "net/http/pprof" // Import for /debug/pprof
	"os"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// DiagnosticsConfig configures pull-based application metrics, debugging and diagnostics.
type DiagnosticsConfig struct {
	// Nothing to see here (yet).
}

// InitDiagnosticsAndRecover enables serving of metrics and debugging services
// registered on the default HTTPMux. It also returns a closure which should be
// deferred, which recover a panic and attempt to log a K8s termination message.
func InitDiagnosticsAndRecover(cfg DiagnosticsConfig) func() {
	grpc.EnableTracing = true

	// Turn on histograms for client & server RPCs. These can be very helpful to
	// have around, but are also expensive to track at scale. You may want to
	// consider rules which selectively discard some histograms at scrape time.
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.EnableClientHandlingTimeHistogram()

	// Package "net/http/pprof" serves /debug/pprof/.
	// Package "expvar" serves /debug/vars

	// Serve a liveness check at /debug/ready.
	http.HandleFunc("/debug/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Serve Prometheus metrics at /debug/metrics.
	http.Handle("/debug/metrics", promhttp.Handler())

	return func() {
		if r := recover(); r != nil {
			writeExitMessage(r)
			panic(r)
		}
	}
}

// Must panics if |err| is non-nil, supplying |msg| and |extra| as
// formatter and fields of the generated panic.
func Must(err error, msg string, extra ...interface{}) {
	if err == nil {
		return
	}
	var f = log.Fields{"err": err}
	for i := 0; i+1 < len(extra); i += 2 {
		f[extra[i].(string)] = extra[i+1]
	}
	writeExitMessage(err)
	log.WithFields(f).Fatal(msg)
}

func writeExitMessage(msg interface{}) {
	// Make a best effort attempt to write a termination message.
	// Bug: https://github.com/kubernetes/kubernetes/issues/31839
	if f, err := os.OpenFile(k8sTerminationLog, os.O_WRONLY, 0777); err == nil {
		_, _ = fmt.Fprintf(f, "%+v", msg)
		_ = f.Close()
	}
}

const (
	// k8sTerminationLog is the location to write a termination message for
	// Kubernetes to retrieve.
	//
	// Link: https://kubernetes.io/docs/tasks/debug-application-cluster/determine-reason-pod-failure/#setting-the-termination-log-file
	k8sTerminationLog = "/dev/termination-log"
)

// Version is populated at build with $(git describe --dirty).
var Version = "development"

// BuildDate is populated at build with $(date +%F-%T-%Z).
var BuildDate = "unknown"
