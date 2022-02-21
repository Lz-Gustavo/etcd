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
	curMu *sync.RWMutex
}

func NewBeelogWr() *BeelogWr {
	s := [numTables]StateTable{}
	m := [numTables]*sync.Mutex{}

	for i := 0; i < numTables; i++ {
		s[i] = make(StateTable)
		m[i] = &sync.Mutex{}
	}

	return &BeelogWr{
		state: s,
		mu:    m,
		curMu: &sync.RWMutex{},
	}
}

// A call with 'filled' resulting in a nil error, inccurs that bw.Entries()
// will be called...
// TODO: describe API and how to prevent data race...
func (bw *BeelogWr) Log(ents []raftpb.Entry, filled bool) error {
	bw.curMu.RLock()
	cur := bw.cur
	bw.mu[cur].Lock()
	bw.curMu.RUnlock()

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

func (bw *BeelogWr) Switch() int {
	bw.curMu.Lock()
	cur := bw.cur
	bw.advance()
	bw.curMu.Unlock()
	return cur
}

func (bw *BeelogWr) Entries(cur int) []raftpb.Entry {
	defer bw.mu[cur].Unlock()

	ents := make([]raftpb.Entry, 0)
	for _, ent := range bw.state[cur] {
		ents = append(ents, ent)
	}
	return ents
}

func (bw *BeelogWr) advance() {
	bw.cur = modInt(bw.cur-numTables+1, numTables)
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
