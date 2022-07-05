package etcdserver

import (
	"os"
	"sort"
	"strconv"
	"strings"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/expconfig"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

func BeelogRecovery(lg *zap.Logger, snap walpb.Snapshot) (*wal.WAL, types.ID, types.ID, raftpb.HardState, []raftpb.Entry) {
	switch expconfig.BeelogRecovConfig {
	case expconfig.Naive:
		InfoIfLog(lg, "running NAIVE recovery")
		return naiveRecovery(lg, expconfig.BeelogRecovDir, snap)

	case expconfig.Descending:
		InfoIfLog(lg, "running DESCENDING recovery")
		return descendingRecovery(lg, expconfig.BeelogRecovDir, snap)

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

	// sort names in ASCENDING order
	sort.Sort(SortByWALNameAsc(names))

	w, err := wal.OpenBeelog(lg, waldir, names, snap)
	if err != nil {
		FatalIfLog(lg, "failed to open WAL", err)
	}

	wmetadata, st, ents, err := w.ReadAllBeelog()
	if err != nil {
		FatalIfLog(lg, "error on reading beelog WAL:", err)
	}

	if len(ents) > 0 && ents[len(ents)-1].Index != st.Commit {
		// add an empty entry with the index matching the last commit
		ents = append(ents, raftpb.Entry{
			Term:  st.Term,
			Type:  raftpb.EntryNormal,
			Index: st.Commit,
			Data:  []byte{},
		})
	}

	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)
	return w, types.ID(metadata.NodeID), types.ID(metadata.ClusterID), st, ents
}

func descendingRecovery(lg *zap.Logger, waldir string, snap walpb.Snapshot) (*wal.WAL, types.ID, types.ID, raftpb.HardState, []raftpb.Entry) {
	names, err := readBeelogFileNamesOnDir(lg, waldir)
	if err != nil {
		FatalIfLog(lg, "failed reading beelog wal names", err)
	}

	// sort names in DESCENDING order
	sort.Sort(SortByWALNameDesc(names))

	w, err := wal.OpenBeelog(lg, waldir, names, snap)
	if err != nil {
		FatalIfLog(lg, "failed to open WAL", err)
	}

	wmetadata, st, ents, err := w.ReadAllBeelogIgnoringSameKeys()
	if err != nil {
		FatalIfLog(lg, "error on reading beelog WAL:", err)
	}

	// reverse the entry slice in-place, so that entries are applied
	// on ascending order
	reverseEntrySlice(ents)

	if len(ents) > 0 && ents[len(ents)-1].Index != st.Commit {
		// add an empty entry with the index matching the last commit
		ents = append(ents, raftpb.Entry{
			Term:  st.Term,
			Type:  raftpb.EntryNormal,
			Index: st.Commit,
			Data:  []byte{},
		})
	}

	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)
	return w, types.ID(metadata.NodeID), types.ID(metadata.ClusterID), st, ents
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
	return names, nil
}

func InfoIfLog(lg *zap.Logger, msg string) {
	if lg != nil {
		lg.Info(msg)
	} else {
		plog.Info(msg)
	}
}

func FatalIfLog(lg *zap.Logger, msg string, err error) {
	if lg != nil {
		lg.Fatal(msg, zap.Error(err))
	} else {
		plog.Fatalf("%s: %v", msg, err)
	}
}

type SortByWALNameAsc []string

func (a SortByWALNameAsc) Len() int      { return len(a) }
func (a SortByWALNameAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortByWALNameAsc) Less(i, j int) bool {
	idxI, _ := strconv.Atoi(strings.Split(a[i], "-")[0])
	idxJ, _ := strconv.Atoi(strings.Split(a[j], "-")[0])
	return idxI < idxJ
}

type SortByWALNameDesc []string

func (a SortByWALNameDesc) Len() int      { return len(a) }
func (a SortByWALNameDesc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortByWALNameDesc) Less(i, j int) bool {
	idxI, _ := strconv.Atoi(strings.Split(a[i], "-")[0])
	idxJ, _ := strconv.Atoi(strings.Split(a[j], "-")[0])
	return idxI > idxJ
}

func reverseEntrySlice(sl []raftpb.Entry) {
	for i, j := 0, len(sl)-1; i < j; i, j = i+1, j-1 {
		sl[i], sl[j] = sl[j], sl[i]
	}
}
