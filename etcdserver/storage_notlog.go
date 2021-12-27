package etcdserver

import (
	"fmt"
	"time"

	"go.etcd.io/etcd/raft/raftpb"
)

// LGX: notWALStorage is an empty implementation of Storage interface,
// used only as a baseline comparison to measure etcd logging costs
type notWALStorage struct{}

func NewNotWALStorage() Storage {
	return &notWALStorage{}
}

func (nw *notWALStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	fmt.Fprintln(latBuff, time.Now().UnixNano())
	return nil
}

func (nw *notWALStorage) SaveSnap(snap raftpb.Snapshot) error {
	return nil
}

func (nw *notWALStorage) Close() error {
	return nil
}

func (nw *notWALStorage) Release(snap raftpb.Snapshot) error {
	return nil
}

func (nw *notWALStorage) Sync() error {
	return nil
}
