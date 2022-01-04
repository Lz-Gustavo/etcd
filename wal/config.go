package wal

import (
	"log"
	"os"
	"strconv"
)

// LGX: utilized only to analyze O_SYNC flag implications on etcd standard
// WAL performance. The syncIO global is only utilized on wal.go to inform
// the O_SYNC flag to os.Open() if enabled.
var syncIO bool

func init() {
	syncIO, _ = strconv.ParseBool(os.Getenv("ETCD_SYNC_IO"))
	if syncIO {
		log.Println("ETCD_SYNC_IO enabled for standard WAL")
	}
}
