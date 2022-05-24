package etcdserver

import (
	"log"
	"os"
	"strconv"
	"strings"

	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

// RecovConfig enums different recovery strategies for beelog...
type RecovConfig int

const (
	Naive RecovConfig = iota
	Descending
	Parallel
)

var (
	recovConfig RecovConfig
	beelogDir   string
)

func parseBeelogRecovConfigFromEnv() {
	rc, _ := strconv.Atoi(os.Getenv("ETCD_BEELOG_RECOV_CONFIG"))
	recovConfig = RecovConfig(rc)

	// TODO: parsing the same ENV as beelog init procedure, refac
	dir, exists := os.LookupEnv("ETCD_BEELOG_LOGS_DIR")
	if !exists {
		log.Println("using /tmp as value for ETCD_BEELOG_LOGS_DIR")
		dir = "/tmp"
	}
	beelogDir = strings.Split(dir, ",")[0]
}

func BeelogRecovery(lg *zap.Logger, waldir string, snap walpb.Snapshot) (*wal.WAL, types.ID, types.ID, raftpb.HardState, []raftpb.Entry) {
	switch recovConfig {
	case Naive:
		return naiveRecovery(lg, waldir, snap)

	default:
		if lg != nil {
			lg.Fatal("unknow beelog recovery config")
		} else {
			plog.Fatalf("unknow beelog recovery config")
		}
	}
	return nil, 0, 0, raftpb.HardState{}, nil
}

func naiveRecovery(lg *zap.Logger, waldir string, snap walpb.Snapshot) (*wal.WAL, types.ID, types.ID, raftpb.HardState, []raftpb.Entry) {
	w, err := wal.OpenBeelog(lg, waldir, snap)
	if err != nil {
		if lg != nil {
			lg.Fatal("failed to open WAL", zap.Error(err))
		} else {
			plog.Fatalf("open wal error: %v", err)
		}
	}

	_, st, ents, err := w.ReadAll()
	if err != nil {
		lg.Fatal("error on reading beelog WAL:", zap.Error(err))
	}

	// TODO: check metadata and return proper ids
	return w, 0, 0, st, ents
}
