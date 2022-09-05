package raft

// LGX: raft/beelog_config ...

import pb "go.etcd.io/etcd/raft/raftpb"

func (l *raftLog) nextEntsBeelog() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 <= off {
		return nil
	}
	return l.sliceBeelog(off)
}

func (l *raftLog) sliceBeelog(off uint64) []pb.Entry {
	var ents []pb.Entry
	if off < l.unstable.offset {
		last := l.lastIndex()

		// used to calculate initial index like this:
		//   ini := min(off, (l.unstable.offset - last))
		//
		// TODO: investigate later

		end := min(l.committed+1, last+1)
		if end < off {
			l.logger.Fatalf("invalid indexes during stored log retrieval, %d must be <= %d", off, end)
		}

		storedEnts, err := l.storage.Entries(off, end, noLimit)
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

// LGX: describe this procedure
func (ms *MemoryStorage) LastIndexBeelog() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.ents[len(ms.ents)-1].Index, nil
}

func (l *raftLog) lastIndexBeelog() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndexBeelog()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}
