package raft

// LGX: raft/beelog_config ...

import pb "go.etcd.io/etcd/raft/raftpb"

func (ms *MemoryStorage) AppendBeelogEntries(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.Lock()
	defer ms.Unlock()

	// intentionally not checking for index offsets
	ms.ents = append(ms.ents, entries...)
	return nil
}
