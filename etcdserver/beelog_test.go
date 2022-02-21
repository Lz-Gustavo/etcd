package etcdserver

import (
	"encoding/binary"
	"math/rand"
	"testing"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
	keySize  = 128
	diffKeys = 10000
)

func TestBeelog(t *testing.T) {
	batchSize := 10
	loggedEntries := make([]raftpb.Entry, 0)
	bw := NewBeelogWr()

	for i := 0; i < batchSize-1; i++ {
		ents := []raftpb.Entry{getRandEntry(i)}
		if err := bw.Log(ents, false); err != nil {
			t.Fatal(err)
		}
		loggedEntries = append(loggedEntries, ents[0])
	}

	// log last command on the batch
	ents := []raftpb.Entry{getRandEntry(batchSize - 1)}
	if err := bw.Log(ents, true); err != nil {
		t.Fatal(err)
	}
	loggedEntries = append(loggedEntries, ents[0])

	oldCur := bw.Switch()
	reducedEntries := make(chan []raftpb.Entry)

	go func() {
		lEnts := bw.Entries(oldCur)
		reducedEntries <- lEnts
	}()

	// log invocations on the new cursor should not block execution from
	// a concurrent call to bw.Entries
	ents = []raftpb.Entry{getRandEntry(batchSize)}
	if err := bw.Log(ents, false); err != nil {
		t.Fatal(err)
	}

	rEnts := <-reducedEntries
	if !entriesWereReducedCorrectly(loggedEntries, rEnts) {
		t.Fatal()
	}
}

func getRandEntry(index int) raftpb.Entry {
	randKey := make([]byte, keySize)
	binary.PutVarint(randKey, rand.Int63n(diffKeys))

	req := &pb.InternalRaftRequest{Put: &pb.PutRequest{
		Key: randKey,
	}}
	return raftpb.Entry{Index: uint64(index), Data: pbutil.MustMarshal(req)}
}

func entriesWereReducedCorrectly(logged, reduced []raftpb.Entry) bool {
	if len(logged) < len(reduced) {
		return false
	}

	// TODO: compare keys from within both sets
	return true
}
