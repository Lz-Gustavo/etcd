package etcdserver

import "os"

// LGX:
//   TODO: describe the idea behind server-side latency measure and why
//   it will be implemented this way with global variables on the pkg scope

const defaultLatencyFilename = "~/etcd-latency.out"

var (
	latencyFilename     = os.Getenv("ETCD_LAT_FILE")
	isMeasuringCurBatch bool
)

func init() {
	// TODO: initialize random seed and latency file
}

func mayMeasureCurrentBatch(index uint64) {
	// TODO: identify if its first command by modulo operation over batch size,
	// calculate rand chance, write into file
}
