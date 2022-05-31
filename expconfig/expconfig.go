package expconfig

import (
	"log"
	"os"
	"strconv"
)

type LogCfgType int

const (
	NotWAL LogCfgType = iota
	StdWAL
	BatchWAL
	Beelog
)

const defaultLogBatchSize int = 1000

var (
	LogConfig    LogCfgType
	LogBatchSize int
)

// LGX: initialization procedure for config envs, called on NewServer()
func ParseLogConfigFromENV() {
	lc, _ := strconv.Atoi(os.Getenv("ETCD_LOG_CONFIG"))
	LogConfig = LogCfgType(lc)

	var err error
	bs := os.Getenv("ETCD_LOG_BATCH_SIZE")
	if LogBatchSize, err = strconv.Atoi(bs); err != nil {
		log.Println("using default value for ETCD_LOG_BATCH_SIZE")
		LogBatchSize = defaultLogBatchSize
	}
}
