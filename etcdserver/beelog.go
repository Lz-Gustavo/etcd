package etcdserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.uber.org/zap"
)

type StateTable map[int64]raftpb.Entry

type BeelogWr struct {
	state     []StateTable
	mu        []*sync.Mutex
	cur       int
	numTables int

	isParallelIO bool
	writers      []chan *beelogSaveRequest
	applyReqs    chan applyEntriesRequest
}

func NewBeelogWrFromEnv(r *raftNode, rh *raftReadyHandler) *BeelogWr {
	numTables, isParisParallelIO, dirs := parseBeelogConfigFromEnv()
	return NewBeelogWr(numTables, isParisParallelIO, dirs, r, rh)
}

func NewBeelogWr(numTables int, isParallelIO bool, dirs []string, r *raftNode, rh *raftReadyHandler) *BeelogWr {
	if !isValidBeelogConfig(numTables, isParallelIO, dirs) {
		return nil
	}

	s := make([]StateTable, numTables)
	m := make([]*sync.Mutex, numTables)

	for i := 0; i < numTables; i++ {
		s[i] = make(StateTable)
		m[i] = &sync.Mutex{}
	}

	wrs := make([]chan *beelogSaveRequest, numTables)
	wrs[0] = make(chan *beelogSaveRequest, 1)
	bw := &BeelogWr{
		state:     s,
		mu:        m,
		numTables: numTables,

		isParallelIO: isParallelIO,
		writers:      wrs,
		applyReqs:    make(chan applyEntriesRequest, numTables),
	}
	go bw.applyEntries(r, rh)
	go bw.saveEntries(r, rh, dirs[0], wrs[0])

	if isParallelIO {
		for i := 1; i < numTables; i++ {
			bw.writers[i] = make(chan *beelogSaveRequest, 1)
			go bw.saveEntries(r, rh, dirs[i], bw.writers[i])
		}
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

// beelogSaveRequest represents an executed raft state, storing all the necessary
// information for a concurrent routine can assync. persist that state on stable storage.
type beelogSaveRequest struct {
	cur   int
	first uint64
	last  uint64

	rd        raft.Ready
	islead    bool
	notifyc   chan<- struct{}
	persisted chan<- struct{}
}

type applyEntriesRequest struct {
	applies   []apply
	persisted chan struct{}
}

// FilledBatch informs beelog to persist the current table state, scheduling 'req'
// to its proper writer channel and advancing the cursor to the next available table.
// The current table stays blocked until a call to Entries() is made.
func (bw *BeelogWr) FilledBatch(req *beelogSaveRequest, applies []apply) {
	req.cur = bw.switchCur()
	ch := make(chan struct{}, 1)

	req.persisted = ch
	bw.applyReqs <- applyEntriesRequest{
		applies:   applies,
		persisted: ch,
	}

	if !bw.isParallelIO {
		bw.writers[0] <- req
		return
	}
	bw.writers[req.cur] <- req
}

// entries reads the table state identified by 'cur', returning a compacted slice of
// its entries and allowing it to receive new ones.
func (bw *BeelogWr) entries(cur int) []raftpb.Entry {
	ents := make([]raftpb.Entry, 0, len(bw.state[cur]))
	for _, ent := range bw.state[cur] {
		ents = append(ents, ent)
	}

	bw.state[cur] = make(StateTable)
	bw.mu[cur].Unlock()
	return ents
}

// saveEntries acts as a writer routine for beelog approach, receiving save requests and
// concurrently flushing them into stable storage. To ensure safety, applies (and by consequence,
// client responses) are only notified after entries are sucessfully persisted.
func (bw *BeelogWr) saveEntries(r *raftNode, rh *raftReadyHandler, dirpath string, reqs <-chan *beelogSaveRequest) {
	for req := range reqs {
		ents := bw.entries(req.cur)
		w, err := wal.CreateBeelogWAL(r.lg, dirpath, req.first, req.last, len(ents))
		if err != nil {
			if r.lg != nil {
				r.lg.Fatal("failed creating new WAL for batch", zap.Error(err))
			} else {
				plog.Fatalf("failed creating new WAL for batch: %v", err)
			}
		}

		if err := w.Save(req.rd.HardState, ents); err != nil {
			if r.lg != nil {
				r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
			} else {
				plog.Fatalf("failed to save state and entries error: %v", err)
			}
		}
		w.Close()

		// for now, r.storage is utilized within processRaftEntriesAfterSave instead of
		// the actual WAL utilized for that batch. Since storage is utilized only within
		// snapshot procedures, and we completely disabled snapshots for our approach,
		// maybe theres no need to worry about that :)
		r.processRaftEntriesAfterSave(req.islead, req.rd, req.notifyc)

		req.persisted <- struct{}{}
	}
}

func (bw *BeelogWr) applyEntries(r *raftNode, rh *raftReadyHandler) {
	for req := range bw.applyReqs {
		select {
		case <-req.persisted:
			for _, apl := range req.applies {
				updateCommittedIndex(&apl, rh)
				select {
				case r.applyc <- apl:
				case <-r.stopped:
					return
				}
			}
			close(req.persisted)

		case <-r.stopped:
			return
		}
	}
}

func (bw *BeelogWr) Shutdown() {
	close(bw.writers[0])
	if bw.isParallelIO {
		for i := 1; i < bw.numTables; i++ {
			close(bw.writers[i])
		}
	}
}

func (bw *BeelogWr) switchCur() int {
	cur := bw.cur
	bw.cur = modInt(bw.cur-bw.numTables+1, bw.numTables)
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

func parseBeelogConfigFromEnv() (concLevel int, isParallelIO bool, dirs []string) {
	concLevel, _ = strconv.Atoi(os.Getenv("ETCD_BEELOG_CONC_LEVEL"))
	if concLevel <= 0 {
		log.Println("using default value for ETCD_BEELOG_CONC_LEVEL")
		concLevel = defaultBeelogConcLevel
	}

	dir, exists := os.LookupEnv("ETCD_BEELOG_LOGS_DIR")
	if !exists {
		log.Println("using /tmp as value for ETCD_BEELOG_LOGS_DIR")
		dir = "/tmp"
	}
	dirs = strings.Split(dir, ",")

	isParallelIO, _ = strconv.ParseBool(os.Getenv("ETCD_BEELOG_PARALLEL_IO"))
	return
}

func isValidBeelogConfig(numTables int, isParallelIO bool, dirs []string) bool {
	if len(dirs) == 0 || numTables < 1 {
		return false
	}

	if isParallelIO && (len(dirs) < numTables) {
		return false
	}
	return true
}
