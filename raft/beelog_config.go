package raft

// LGX: raft/beelog_config ...

import (
	pb "go.etcd.io/etcd/raft/raftpb"
)

// TODO: parse from ENV config later
const BeelogEnabled = false

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

func (l *raftLog) nextEntsBeelog() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 <= off {
		return nil
	}

	if off < l.unstable.offset {
		last := l.lastIndex()

		// same diff of last and unstable offset
		ini := min(off, last-(l.unstable.offset-l.applied)+1)
		end := min(l.committed+1, last+1)

		if end < ini {
			l.logger.Fatalf("invalid indexes during stored log retrieval, %d must be <= %d", ini, end)
		}

		storedEnts, err := l.storage.Entries(ini, end, l.maxNextEntsSize)
		if err != nil {
			l.logger.Fatal("error retrieving stored entries", err)
		}
		ents = storedEnts
	}

	if l.committed+1 > l.unstable.offset {
		unstable := l.unstable.slice(max(off, l.unstable.offset), l.committed+1)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined

		} else {
			ents = unstable
		}
	}
	return ents
}
