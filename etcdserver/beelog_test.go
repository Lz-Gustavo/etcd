package etcdserver

import (
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
	keySize  = 128
	diffKeys = 10000
)

func TestBeelogAPI(t *testing.T) {
	batchSize := 10
	loggedEntries := make([]raftpb.Entry, 0)
	bw := NewBeelogWr(2, false, []string{"/tmp"}, nil, nil)

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

	oldCur := bw.switchCur()
	reducedEntries := make(chan []raftpb.Entry)

	go func() {
		re := bw.entries(oldCur)
		reducedEntries <- re
	}()

	// log invocations on the new cursor should not block execution from
	// a concurrent call to bw.Entries
	ents = []raftpb.Entry{getRandEntry(batchSize)}
	if err := bw.Log(ents, false); err != nil {
		t.Fatal(err)
	}

	rEnts := <-reducedEntries
	if !entriesWereReducedCorrectly(loggedEntries, rEnts) {
		t.Fatal("logs were not reduced correctly, states are not equivalent")
	}
}

func TestBeelogExecutionOnRaft(t *testing.T) {
	batchSize := 10
	bw := NewBeelogWr(2, false, []string{"/tmp"}, nil, nil)

	// generate 10 * batchsize entries on raft channel
	raftReady := make(chan raft.Ready)
	go generateRaftReady(raftReady, 10*batchSize)

	count := 0
	for rd := range raftReady {
		count += len(rd.Entries)
		if count < batchSize {
			if err := bw.Log(rd.Entries, false); err != nil {
				t.Fatal(err)
			}
			break
		}

		if err := bw.Log(rd.Entries, true); err != nil {
			t.Fatal(err)
		}

		// just calling entries for now, testing possible data race
		go func(cur int) {
			bw.entries(cur)
		}(bw.switchCur())

		// TODO: delay repply on commited entries
	}
}

func TestIsValidBeelogConfig(t *testing.T) {
	testCases := []struct {
		name         string
		numTables    int
		isParallelIO bool
		dirs         []string
		expectedOut  bool
	}{
		{
			"two tables, no parallelism",
			2,
			false,
			[]string{"/tmp"},
			true,
		},
		{
			"two tables, parallelism, two dirs",
			2,
			true,
			[]string{"/tmp", "/tmp2"},
			true,
		},
		{
			"ten tables, parallelism, a single dir",
			10,
			true,
			[]string{"/tmp"},
			false,
		},
		{
			"empty dir",
			2,
			false,
			[]string{},
			false,
		},
	}

	for _, tc := range testCases {
		out := isValidBeelogConfig(tc.numTables, tc.isParallelIO, tc.dirs)
		if out != tc.expectedOut {
			t.Fatal("failed on test", tc.name)
		}
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
	if len(reduced) > len(logged) {
		return false
	}

	loggedState := make(StateTable)
	for _, ent := range logged {
		key, err := getKeyFromRaftEntry(ent)
		if err != nil {
			return false
		}
		loggedState[key] = ent
	}

	reducedState := make(StateTable)
	for _, ent := range reduced {
		key, err := getKeyFromRaftEntry(ent)
		if err != nil {
			return false
		}
		reducedState[key] = ent
	}
	return reflect.DeepEqual(reducedState, loggedState)
}

func generateRaftReady(rds chan<- raft.Ready, numRds int) {
	// starts at 2 so that the first index is 1
	for i := 2; i < numRds+2; i++ {
		rds <- raft.Ready{
			CommittedEntries: []raftpb.Entry{
				getRandEntry(i - 1),
			},
			Entries: []raftpb.Entry{
				getRandEntry(i),
			},
		}
	}
	close(rds)
}
