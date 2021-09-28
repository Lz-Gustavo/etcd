package etcdserver

// LGX:
//   TODO: describe this storage implementation...
//
//   TODO2: later modify the current initialization of storage to utilize this implementation.

import (
	"context"
	"errors"
	"log"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/Lz-Gustavo/beemport"
	beelogpb "github.com/Lz-Gustavo/beemport/pb"
)

const (
	beelogBatchSize uint32 = 100
	beelogConcLevel        = 2
	logsDir                = "./tmp"
)

func configBeelog() *beemport.LogConfig {
	return &beemport.LogConfig{
		Sync:    false,
		Measure: false,
		Tick:    beemport.Interval,
		Period:  beelogBatchSize,
		KeepAll: true,
		Fname:   logsDir + "beelog.log",
	}
}

// beelogStorage...
type beelogStorage struct {
	wal *beemport.ConcTable
}

func NewBeelogStorage() Storage {
	ct, err := beemport.NewConcTableWithConfig(context.Background(), beelogConcLevel, configBeelog())
	if err != nil {
		log.Fatalln("")
	}
	return &beelogStorage{ct}
}

func (bs *beelogStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	for _, ent := range ents {
		if ent.Type != raftpb.EntryNormal {
			continue
		}

		bent, err := convertRaftEntryIntoBeelogEntry(ent)
		if err != nil {
			return err
		}

		if err = bs.wal.Log(bent); err != nil {
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

func convertRaftEntryIntoBeelogEntry(entry raftpb.Entry) (*beelogpb.Entry, error) {
	// NOTE: unmarshaling here is excessively expensive since it is also unmarshaled
	// later when request is applied. This will result in a significant performance
	// overhead which will compromise our experiments.
	//
	// TODO: think of a better way to log this...
	var raftReq pb.InternalRaftRequest
	if !pbutil.MaybeUnmarshal(&raftReq, entry.Data) {
		return nil, errors.New("failed unmarshaling entry into raft request")
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
		return bent, nil
	}

	bent = &beelogpb.Entry{
		Id:      entry.Index,
		Key:     string(raftReq.Range.Key),
		WriteOp: false,
	}
	return bent, nil
}
