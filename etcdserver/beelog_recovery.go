package etcdserver

import (
	"os"
	"strconv"

	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.uber.org/zap"
)

// RecovConfig enums different recovery strategies for beelog...
type RecovConfig int

const (
	Naive RecovConfig = iota
	Descending
	Parallel
)

var recovConfig RecovConfig

func parseBeelogRecovConfigFromEnv() {
	rc, _ := strconv.Atoi(os.Getenv("ETCD_BEELOG_RECOV_CONFIG"))
	recovConfig = RecovConfig(rc)
}

func BeelogRecovery(lg *zap.Logger, waldir string) (*wal.WAL, types.ID, types.ID, raftpb.HardState, []raftpb.Entry) {
	switch recovConfig {
	case Naive:
		return naiveRecovery(lg, waldir)

	default:
		lg.Fatal("unknow beelog recovery config")
	}
	return nil, 0, 0, raftpb.HardState{}, nil
}

func naiveRecovery(lg *zap.Logger, waldir string) (*wal.WAL, types.ID, types.ID, raftpb.HardState, []raftpb.Entry) {
	// TODO
	return nil, 0, 0, raftpb.HardState{}, nil
}
