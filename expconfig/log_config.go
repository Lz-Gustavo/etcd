package expconfig

import (
	"log"
	"os"
	"strconv"
	"strings"
)

type LogCfgType int

const (
	NotWAL LogCfgType = iota
	StdWAL
	BatchWAL
	Beelog
)

const (
	defaultLogBatchSize    int    = 1000
	defaultBeelogConcLevel int    = 2
	defaultBeelogLogsDir   string = "/tmp/beelog"
)

var (
	LogConfig      LogCfgType
	IsBeelogConfig bool
	LogBatchSize   int

	BeelogConcLevel    int
	BeelogIsParallelIO bool
	BeelogDirs         []string
)

// LGX: initialization procedure for config envs, called on NewServer()
func ParseLogConfigFromENV() {
	lc, _ := strconv.Atoi(os.Getenv("ETCD_LOG_CONFIG"))
	LogConfig = LogCfgType(lc)

	if LogConfig == Beelog {
		IsBeelogConfig = true
		parseBeelogConfigFromEnv()
		parseBeelogRecovConfigFromEnv()
	}

	var err error
	bs := os.Getenv("ETCD_LOG_BATCH_SIZE")
	if LogBatchSize, err = strconv.Atoi(bs); err != nil {
		log.Println("using default value for ETCD_LOG_BATCH_SIZE")
		LogBatchSize = defaultLogBatchSize
	}
}

func parseBeelogConfigFromEnv() {
	BeelogConcLevel, _ = strconv.Atoi(os.Getenv("ETCD_BEELOG_CONC_LEVEL"))
	if BeelogConcLevel <= 0 {
		log.Println("using default value for ETCD_BEELOG_CONC_LEVEL")
		BeelogConcLevel = defaultBeelogConcLevel
	}

	dir, exists := os.LookupEnv("ETCD_BEELOG_LOGS_DIR")
	if !exists {
		log.Println("using /tmp as value for ETCD_BEELOG_LOGS_DIR")
		dir = "/tmp"
	}
	BeelogDirs = strings.Split(dir, ",")

	BeelogIsParallelIO, _ = strconv.ParseBool(os.Getenv("ETCD_BEELOG_PARALLEL_IO"))
}
