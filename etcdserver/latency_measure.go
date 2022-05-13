package etcdserver

// LGX: DEPRECATED
// Although still supported on the standard logging configuration, server-side latency
// measurement for I/O calls is not utilized anymore while evaluating any aspect of etcd's
// performance. As of commit https://github.com/Lz-Gustavo/etcd/commit/082638b73f06c463bbeb22a0c8827acb04de668e,
// client-side latency measurement is utilized on every analysis conducted against this
// prototype. Latency measurement usage can still be seen on etcdserver/raft.go/startStdWAL(),
// and can be turned on for SL by setting isMeasuringLatency to true.

import (
	"bytes"
	"log"
	"math/rand"
	"os"
	"time"
)

const (
	defaultLatencyFilename = "~/etcd-latency.out"

	// NOTE: changing for now while testing etcd benchmark
	isMeasuringLatency = false
	latMeasureChance   = 10
)

var (
	latencyFilename = os.Getenv("ETCD_LAT_FILE")

	latFile *os.File
	latBuff *bytes.Buffer
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func mayMeasureLat() (int64, bool) {
	if rand.Intn(latMeasureChance) == 0 {
		return time.Now().UnixNano(), true
	}
	return 0, false
}

func flushLatBufferIntoFile() {
	if _, err := latBuff.WriteTo(latFile); err != nil {
		log.Fatalln("failed copying into file, err:", err.Error())
	}
	latFile.Close()
}
