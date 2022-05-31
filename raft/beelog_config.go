package raft

// LGX: raft/beelog_config ...

import pb "go.etcd.io/etcd/raft/raftpb"

func (ms *MemoryStorage) AppendBeelogEntries(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.Lock()
	defer ms.Unlock()

	// intentionally not checking for compacted entries, but verifying the
	// first entry index.
	//
	// TODO: it currently works because the first entries will never be discarded
	// by beelog (theyre conf entries). Is it ok to keep for all use cases? investigate later

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		raftLogger.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}

func (l *raftLog) nextEntsBeelog() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 <= off {
		return nil
	}

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

		storedEnts, err := l.storage.Entries(off, end, l.maxNextEntsSize)
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
