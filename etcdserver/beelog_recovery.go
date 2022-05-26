package etcdserver

import (
	"log"
	"os"
	"sort"
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
		FatalIfLog(lg, "unknow beelog recovery config", nil)
	}
	return nil, 0, 0, raftpb.HardState{}, nil
}

func naiveRecovery(lg *zap.Logger, waldir string, snap walpb.Snapshot) (*wal.WAL, types.ID, types.ID, raftpb.HardState, []raftpb.Entry) {
	names, err := readBeelogFileNamesOnDir(lg, waldir)
	if err != nil {
		FatalIfLog(lg, "failed reading beelog wal names", err)
	}

	w, err := wal.OpenBeelog(lg, waldir, names, snap)
	if err != nil {
		FatalIfLog(lg, "failed to open WAL", err)
	}

	_, st, ents, err := w.ReadAllBeelog()
	if err != nil {
		FatalIfLog(lg, "error on reading beelog WAL:", err)
	}

	// TODO: check metadata and return proper ids?
	return w, 0, 0, st, ents
}

func readBeelogFileNamesOnDir(lg *zap.Logger, waldir string) ([]string, error) {
	dir, err := os.Open(waldir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	sort.Sort(SortByWALName(names))
	return names, nil
}

func FatalIfLog(lg *zap.Logger, msg string, err error) {
	if lg != nil {
		lg.Fatal(msg, zap.Error(err))
	} else {
		plog.Fatalf("%s: %v", msg, err)
	}
}

type SortByWALName []string

func (a SortByWALName) Len() int      { return len(a) }
func (a SortByWALName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortByWALName) Less(i, j int) bool {
	idxI, _ := strconv.Atoi(strings.Split(a[i], "-")[0])
	idxJ, _ := strconv.Atoi(strings.Split(a[j], "-")[0])
	return idxI < idxJ
}
