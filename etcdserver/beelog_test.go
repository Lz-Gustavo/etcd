package etcdserver

import (
	"encoding/binary"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

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

func BenchmarkBatchTimerApproaches(b *testing.B) {
	n := 100000
	batchSize, count := 10, 0
	dur := 10 * time.Millisecond

	b.Run("initializing new timer on each batch", func(b *testing.B) {
		var batchTimer *time.Timer

	FOR:
		for i := 0; i < n; i++ {
			if batchTimer == nil {
				batchTimer = time.NewTimer(dur)
			}

			select {
			case <-batchTimer.C:
			default:
				if count++; count < batchSize {
					continue FOR
				}
			}
			batchTimer = nil
		}
	})

	b.Run("stop and reset timer on each batch", func(b *testing.B) {
		batchTimer := time.NewTimer(time.Second)
		batchTimer.Stop()
		mustResetTimer := true

	FOR:
		for i := 0; i < n; i++ {
			if mustResetTimer {
				batchTimer.Stop()
				batchTimer.Reset(dur)
				mustResetTimer = false
			}

			select {
			case <-batchTimer.C:
			default:
				if count++; count < batchSize {
					continue FOR
				}
			}
			mustResetTimer = true
		}
	})
}

type _writerRequestOnTest struct {
	persisted chan struct{}
}

func BenchmarkApplyCoordinationApproaches(b *testing.B) {
	numRequests := 100
	numWriters := 4

	applyDuration := time.Millisecond
	persistenceDuration := 10 * time.Millisecond

	b.Run("circular mutex array for writers", func(b *testing.B) {
		b.StopTimer()

		raftReady := make(chan raft.Ready)
		go generateRaftReady(raftReady, numRequests)

		mu := make([]*sync.Mutex, numWriters)
		mu[0] = &sync.Mutex{}
		for i := 1; i < numWriters; i++ {
			mu[i] = &sync.Mutex{}
			mu[i].Lock()
		}

		// spawn writer routines
		writerChans := make([]chan *_writerRequestOnTest, numWriters)
		for i := 0; i < numWriters; i++ {
			writerChans[i] = make(chan *_writerRequestOnTest, 1)
			go func(idx int) {
				for req := range writerChans[idx] {
					// emulate a disk io
					time.Sleep(persistenceDuration)

					// wait to apply and emulate apply
					mu[idx].Lock()
					time.Sleep(applyDuration)
					doNothing(req)

					// signal applied req (circular manner)
					next := modInt(idx-numWriters+1, numWriters)
					mu[next].Unlock()
				}
			}(i)
		}

		// emulate raft execution
		b.StartTimer()
		count := 0
		for range raftReady {
			writerChans[count%numWriters] <- &_writerRequestOnTest{}
			count++
		}
		b.StopTimer()

		for i := 0; i < numWriters; i++ {
			close(writerChans[i])
		}
	})

	b.Run("sync channel on apply requests", func(b *testing.B) {
		b.StopTimer()

		raftReady := make(chan raft.Ready)
		go generateRaftReady(raftReady, numRequests)

		// spawn apply routine
		applyChannel := make(chan *applyEntriesRequest, numWriters)
		go func(apChan chan *applyEntriesRequest) {
			for ap := range apChan {
				<-ap.persisted

				// emulate apply
				time.Sleep(applyDuration)
				close(ap.persisted)
			}
		}(applyChannel)

		// spawn writer routines
		writerChans := make([]chan *_writerRequestOnTest, numWriters)
		for i := 0; i < numWriters; i++ {
			writerChans[i] = make(chan *_writerRequestOnTest, 1)
			go func(idx int) {
				for req := range writerChans[idx] {
					// emulate a disk io
					time.Sleep(persistenceDuration)

					// signal io persistence to apply
					req.persisted <- struct{}{}
				}
			}(i)
		}

		// emulate raft execution
		b.StartTimer()
		count := 0
		for range raftReady {
			ch := make(chan struct{}, 1)
			req := &_writerRequestOnTest{persisted: ch}

			writerChans[count%numWriters] <- req
			applyChannel <- &applyEntriesRequest{persisted: ch}
			count++
		}
		b.StopTimer()

		for i := 0; i < numWriters; i++ {
			close(writerChans[i])
		}
		close(applyChannel)
	})
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

func doNothing(req *_writerRequestOnTest) {}
