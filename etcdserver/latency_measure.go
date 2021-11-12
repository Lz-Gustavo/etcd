package etcdserver

import (
	"bytes"
	"fmt"
	"io"
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
	measuringLatency       = true
	latMeasureChance       = 10
)

var (
	latencyFilename = os.Getenv("ETCD_LAT_FILE")

	latFile *os.File
	latBuff *bytes.Buffer
	// isMeasuringCurBatch bool
)

func init() {
	rand.Seed(time.Now().UnixNano())
	latBuff = &bytes.Buffer{}

	fn := latencyFilename
	if fn == "" {
		fn = defaultLatencyFilename
	}

	var err error
	latFile, err = os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalln("failed initializing latency file, err:", err.Error())
	}
}

// TODO: identify if its first command by modulo operation over batch size,
// calculate rand chance, write into file
func mayMeasureCurrentBatch(index uint64) {
	// not first command nor batch size = 1, ignore...
	if index%beelogBatchSize != 0 && beelogBatchSize != 1 {
		return
	}

	if rand.Intn(latMeasureChance) == 0 {
		fmt.Fprintln(latBuff, time.Now().UnixNano())
	}
}

func flushLatBufferIntoFile() {
	_, err := io.Copy(latFile, latBuff)
	if err != nil {
		log.Fatalln("failed copying into file, err:", err.Error())
	}
	latFile.Close()
}
