package expconfig

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	defaultRecoveryFilename = "/tmp/recov-time.out"
)

var (
	IsRecoveryMsrEnabled bool
	RecoveryMeasure      *RecovMsr
)

func init() {
	RecoveryMeasure = NewRecovMsr()
}

type RecovMsr struct {
	buff *bytes.Buffer
	file *os.File
}

func NewRecovMsr() *RecovMsr {
	rm := &RecovMsr{
		buff: &bytes.Buffer{},
	}

	fn := parseRecoveryMsrConfigFromEnv()
	if !IsRecoveryMsrEnabled {
		return rm
	}

	if fn == "" {
		log.Println("utilizing default value for recovery measurement file")
		fn = defaultRecoveryFilename
	}

	fd, err := os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalln("failed creating recovery measurement, err:", err)
	}
	rm.file = fd
	return rm
}

func (rm *RecovMsr) RecordTimestamp() {
	if _, err := fmt.Fprintln(rm.buff, time.Now().UnixNano()); err != nil {
		log.Fatalln("failed recording current timestamp, err:", err)
	}
}

func (rm *RecovMsr) Flush() {
	if _, err := rm.buff.WriteTo(rm.file); err != nil {
		log.Fatalln("failed flushing data to disk, err:", err)
	}
}

func (rm *RecovMsr) Close() {
	rm.file.Close()
}

func parseRecoveryMsrConfigFromEnv() string {
	IsRecoveryMsrEnabled, _ = strconv.ParseBool(os.Getenv("ETCD_RECOVERY_MSR_ENABLED"))
	if !IsRecoveryMsrEnabled {
		return ""
	}
	return os.Getenv("ETCD_RECOVERY_MSR_FILE")
}
