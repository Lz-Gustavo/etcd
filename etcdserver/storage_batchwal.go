package etcdserver

import (
	"fmt"
	"time"

	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
)

// LGX: describe this batch wal wrapper over the standard etcd wal

type batchWALStorage struct {
	buff  []raftpb.Entry
	count int
	batch int
	wal   *wal.WAL
}

func NewBatchWALStorage(w *wal.WAL) Storage {
	return &batchWALStorage{
		buff:  make([]raftpb.Entry, 0, logBatchSize),
		batch: int(logBatchSize),
		wal:   w,
	}
}

func (sb *batchWALStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	sb.count += len(ents)
	if sb.count < sb.batch {
		sb.buff = append(sb.buff, ents...)
		return nil
	}

	err := sb.wal.Save(st, sb.buff)
	sb.count = 0
	// keeps the underlying array
	sb.buff = sb.buff[:0]

	fmt.Fprintln(latBuff, time.Now().UnixNano())
	return err
}

func (sb *batchWALStorage) SaveSnap(snap raftpb.Snapshot) error {
	return nil
}

func (sb *batchWALStorage) Close() error {
	return sb.wal.Close()
}

func (sb *batchWALStorage) Release(snap raftpb.Snapshot) error {
	return nil
}

func (sb *batchWALStorage) Sync() error {
	return sb.wal.Sync()
}
