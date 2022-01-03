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

	lastState raftpb.HardState
	wal       *wal.WAL
}

func NewBatchWALStorage(w *wal.WAL) Storage {
	return &batchWALStorage{
		buff:  make([]raftpb.Entry, 0, logBatchSize),
		batch: int(logBatchSize),
		wal:   w,
	}
}

// NOTE: we still dont understand exactly:
//  (i) why different sizes of 'ents' slice during execution, and why does
//  it even batches commands on this Raft implementation
//  (ii) the implications of calling a single wal.Save() with a configured number
//  of entries at once
//
// The ideia is to evaluate this scenarios during experimentation. Because of both
// (i) and (ii), we currently implement batching on the standard WAL by logging
// the batched commands if the size of 'ents' exceeds the current count within the
// batch size. As an example, if a call to 'Save()' informs an 'ents' slice with 5
// entries, and the current batch of size 10 already has 7 (so exceeding the size by 2)
// the idea is that only the batched 7 are logged and the remaining 5 are allocated
// into the next batch.
func (sb *batchWALStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	sb.count += len(ents)
	if sb.count < sb.batch {
		sb.lastState = st
		sb.buff = append(sb.buff, ents...)
		return nil
	}

	err := sb.wal.Save(sb.lastState, sb.buff)
	sb.lastState = st
	sb.count = len(ents)
	// keeps the underlying array
	sb.buff = append(sb.buff[:0], ents...)

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
