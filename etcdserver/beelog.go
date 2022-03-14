package etcdserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/raft/raftpb"
)

const numTables int = 2

type StateTable map[int64]raftpb.Entry

type BeelogWr struct {
	state [numTables]StateTable
	mu    [numTables]*sync.Mutex
	cur   int

	isParallelIO bool
	writers      [numTables]chan *beelogSaveRequest
	writersMu    [numTables]*sync.Mutex
}

func NewBeelogWr(isParallelIO bool) *BeelogWr {
	s := [numTables]StateTable{}
	m := [numTables]*sync.Mutex{}

	for i := 0; i < numTables; i++ {
		s[i] = make(StateTable)
		m[i] = &sync.Mutex{}
	}

	wrs := [numTables]chan *beelogSaveRequest{make(chan *beelogSaveRequest)}
	bw := &BeelogWr{
		state: s,
		mu:    m,

		isParallelIO: isParallelIO,
		writers:      wrs,
	}

	if isParallelIO {
		wMu := [numTables]*sync.Mutex{{}}
		for i := 1; i < numTables; i++ {
			wMu[i] = &sync.Mutex{}
			wMu[i].Lock()
			bw.writers[i] = make(chan *beelogSaveRequest)
		}
		bw.writersMu = wMu
	}
	return bw
}

// A call with 'filled' resulting in a nil error, inccurs that bw.Entries()
// will be called...
// TODO: describe API and how to prevent data race...
//
// IMPORTANT: Log should never be called concurrently
func (bw *BeelogWr) Log(ents []raftpb.Entry, filled bool) error {
	cur := bw.cur
	bw.mu[cur].Lock()

	for _, ent := range ents {
		k, err := getKeyFromRaftEntry(ent)
		if err != nil {
			bw.mu[cur].Unlock()
			return err
		}
		bw.state[cur][k] = ent
	}

	if !filled {
		bw.mu[cur].Unlock()
	}
	return nil
}

// FilledBatch informs beelog to persist the current table state, scheduling 'req'
// to its proper writer channel and advancing the cursor to the next available table.
// The current table stays blocked until a call to Entries() is made.
func (bw *BeelogWr) FilledBatch(req *beelogSaveRequest) {
	req.cur = bw.switchCur()
	if !bw.isParallelIO {
		bw.writers[0] <- req
		return
	}
	bw.writers[req.cur] <- req
}

// Entries reads the table state identified by 'cur', returning a compacted slice of
// its entries and allowing it to receive new ones.
func (bw *BeelogWr) Entries(cur int) []raftpb.Entry {
	defer bw.mu[cur].Unlock()

	ents := make([]raftpb.Entry, 0, len(bw.state[cur]))
	for _, ent := range bw.state[cur] {
		ents = append(ents, ent)
	}
	return ents
}

func (bw *BeelogWr) WaitToApply(cur int) {
	if !bw.isParallelIO {
		return
	}
	bw.writersMu[cur].Lock()
}

func (bw *BeelogWr) Applied(cur int) {
	if !bw.isParallelIO {
		return
	}
	next := modInt(cur-numTables+1, numTables)
	bw.writersMu[next].Unlock()
}

func (bw *BeelogWr) Shutdown() {
	close(bw.writers[0])
	if bw.isParallelIO {
		for i := 1; i < numTables; i++ {
			close(bw.writers[i])
		}
	}
}

func (bw *BeelogWr) switchCur() int {
	cur := bw.cur
	bw.cur = modInt(bw.cur-numTables+1, numTables)
	return cur
}

func modInt(a, b int) int {
	a = a % b
	if a >= 0 {
		return a
	}
	return a + b
}

// TODO: study the possibility of a more efficient unmarshal. Maybe theres no need to
// unmarshal the entire structure since we only need the operation key
func getKeyFromRaftEntry(ent raftpb.Entry) (int64, error) {
	if ent.Type != raftpb.EntryNormal {
		return 0, errors.New("requested entry is not from EntryNormal type")
	}

	raftReq := &pb.InternalRaftRequest{}
	if err := raftReq.Unmarshal(ent.Data); err != nil {
		return 0, err
	}

	if raftReq.Put == nil && raftReq.Range == nil {
		return 0, errors.New("requested entry is not a Put nor Range operation")
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
