package etcdserver

import (
	"bytes"
	"log"
	"math/rand"
	"os"
	"time"
)

// LGX:
//   TODO: describe the idea behind server-side latency measure and why
//   it will be implemented this way with global variables on the pkg scope

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
