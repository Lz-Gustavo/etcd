package common

import (
	"bytes"
	"encoding/binary"
	"errors"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/raft/raftpb"
)

var ErrUnknowBeelogOp = errors.New("unknow operation informed for beelog")

// TODO: study the possibility of a more efficient unmarshal. Maybe theres no need to
// unmarshal the entire structure since we only need the operation key
func GetKeyFromRaftEntry(ent raftpb.Entry) (int64, error) {
	if ent.Type != raftpb.EntryNormal {
		return 0, ErrUnknowBeelogOp
	}

	raftReq := &pb.InternalRaftRequest{}
	if err := raftReq.Unmarshal(ent.Data); err != nil {
		return 0, ErrUnknowBeelogOp
	}

	if raftReq.Put == nil && raftReq.Range == nil {
		return 0, ErrUnknowBeelogOp
	}

	var rd *bytes.Reader
	if raftReq.Put != nil {
		rd = bytes.NewReader(raftReq.Put.Key)
	} else {
		rd = bytes.NewReader(raftReq.Range.Key)
	}

	key, err := binary.ReadVarint(rd)
	if err != nil {
		return 0, err
	}
	return key, nil
}
