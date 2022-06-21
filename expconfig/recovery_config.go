package expconfig

import (
	"os"
	"strconv"
)

// RecovCfgType enums different recovery strategies for beelog...
type RecovCfgType int

const (
	Naive RecovCfgType = iota
	Descending
	Parallel
)

var (
	BeelogRecovConfig RecovCfgType
	BeelogRecovDir    string
)

func parseBeelogRecovConfigFromEnv() {
	rc, _ := strconv.Atoi(os.Getenv("ETCD_BEELOG_RECOV_CONFIG"))
	BeelogRecovConfig = RecovCfgType(rc)

	// TODO: using the same ENV as beelog init procedure, refac
	BeelogRecovDir = BeelogDirs[0]
}
