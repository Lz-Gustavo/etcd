package etcdserver

// LGX:
//   TODO: describe this storage implementation...
//
//   TODO2: later modify the current initialization of storage to utilize this implementation.

import (
	"context"
	"log"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/Lz-Gustavo/beemport"
	beelogpb "github.com/Lz-Gustavo/beemport/pb"
)

const (
	beelogBatchSize uint32 = 1000
	beelogConcLevel        = 2
	logsDir                = "/tmp/beelog/"
)

func configBeelog() *beemport.LogConfig {
	// NOTE: zero values are only declared for documentation purposes
	return &beemport.LogConfig{
		Sync:       false,
		Measure:    false,
		ParallelIO: false,
		Tick:       beemport.Interval,
		Period:     beelogBatchSize,
		KeepAll:    true,
		Fname:      logsDir + "beelog.log",
	}
}

// beelogStorage...
type beelogStorage struct {
	wal *beemport.ConcTable
}

func NewBeelogStorage() Storage {
	ct, err := beemport.NewConcTableWithConfig(context.Background(), beelogConcLevel, configBeelog())
	if err != nil {
		log.Fatalln("failed initializing ConcTable structure")
	}
	return &beelogStorage{ct}
}

func (bs *beelogStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	for _, ent := range ents {
		// non-command entries (i.e. config changes) wont be logged within beelog. This wont
		// be a problem for a while, since the first exemperiments will consider a stable
		// cluster. Also, the first throughput measurements are always discard due to system
		// setup, which will ensure a fair comparison between both strategies. Once we start
		// measuring recovery, we'll think of a better way to log these.
		if ent.Type != raftpb.EntryNormal {
			continue
		}

		bent := convertRaftEntryIntoBeelogEntry(ent)
		if bent == nil {
			log.Println("could not convert entry", ent.Index, "ignoring...")
			continue
		}

		if err := bs.wal.Log(bent); err != nil {
			return err
		}
	}
	return nil
}

func (bs *beelogStorage) Close() error {
	bs.wal.Shutdown()
	return nil
}

func (bs *beelogStorage) SaveSnap(snap raftpb.Snapshot) error {
	return nil
}

func (bs *beelogStorage) Release(snap raftpb.Snapshot) error {
	return nil
}

func (bs *beelogStorage) Sync() error {
	return nil
}

func convertRaftEntryIntoBeelogEntry(entry raftpb.Entry) *beelogpb.Entry {
	// NOTE: unmarshaling here is excessively expensive since it is also unmarshaled
	// later when request is applied. This will result in a significant performance
	// overhead which will compromise our experiments.
	//
	// TODO: think of a better way to log this...
	var raftReq pb.InternalRaftRequest
	if !pbutil.MaybeUnmarshal(&raftReq, entry.Data) {
		return nil
	}

	if raftReq.Put == nil && raftReq.Range == nil {
		return nil
	}

	var bent *beelogpb.Entry
	isWriteOp := raftReq.Put != nil
	if isWriteOp {
		bent = &beelogpb.Entry{
			Id:      entry.Index,
			Key:     string(raftReq.Put.Key),
			WriteOp: isWriteOp,
			Command: entry.Data,
		}
		return bent
	}

	bent = &beelogpb.Entry{
		Id:      entry.Index,
		Key:     string(raftReq.Range.Key),
		WriteOp: false,
	}
	return bent
}
