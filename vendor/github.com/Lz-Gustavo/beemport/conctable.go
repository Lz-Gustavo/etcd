package beemport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Lz-Gustavo/beemport/pb"
)

const (
	// ConcTable.concLevel sets the number of different views of the same structure.
	defaultConcLvl int = 2

	// number of commands to wait until a complete state reset for Immediately
	// reduce period.
	resetOnImmediately int = 4000
)

var (
	ErrInvalidConcLevel     = errors.New("must inform a positive value for 'concLevel' argument")
	ErrInvalidRecovInterval = errors.New("invalid interval request, 'n' must be >= 'p'")
)

// logEvent represents a event metadata passed to logger routines signalling a persistence
// to a certain table, and the array position to store the measurement data.
type logEvent struct {
	table, measure int
}

// stateTable is a minimal format of an ordinary stateTable, storing only
// the lates state for each key.
type stateTable map[string]*pb.Entry

// ConcTable ...
type ConcTable struct {
	views  []stateTable
	mu     []sync.Mutex
	logs   []logData
	cancel context.CancelFunc

	concLevel int
	loggerReq chan logEvent
	cursorMu  sync.Mutex
	cursor    int
	prevLog   int32 // atomic
	logFolder string

	isMeasuringLat bool
	latMeasure     *latencyMeasure
}

// NewConcTable ...
func NewConcTable(ctx context.Context) *ConcTable {
	c, cancel := context.WithCancel(ctx)
	ct := &ConcTable{
		cancel:    cancel,
		loggerReq: make(chan logEvent, defaultConcLvl),
		concLevel: defaultConcLvl,

		views: make([]stateTable, defaultConcLvl),
		mu:    make([]sync.Mutex, defaultConcLvl),
		logs:  make([]logData, defaultConcLvl),
	}

	def := *DefaultLogConfig()
	for i := 0; i < defaultConcLvl; i++ {
		ct.logs[i] = logData{config: &def}
		ct.views[i] = make(stateTable)
	}
	ct.logFolder = extractLocation(def.Fname)

	// Measure disabled in default config
	go ct.handleReduce(c, false)
	return ct
}

// NewConcTableWithConfig ...
func NewConcTableWithConfig(ctx context.Context, concLvl int, cfg *LogConfig) (*ConcTable, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}
	if concLvl < 0 {
		return nil, ErrInvalidConcLevel
	}

	c, cancel := context.WithCancel(ctx)
	ct := &ConcTable{
		cancel:    cancel,
		loggerReq: make(chan logEvent, concLvl),
		concLevel: concLvl,

		views: make([]stateTable, concLvl),
		mu:    make([]sync.Mutex, concLvl),
		logs:  make([]logData, concLvl),
	}

	for i := 0; i < concLvl; i++ {
		ct.logs[i] = logData{config: cfg}
		ct.views[i] = make(stateTable)
	}
	ct.logFolder = extractLocation(cfg.Fname)

	if cfg.Measure {
		ct.isMeasuringLat = true
		ct.latMeasure, err = newLatencyMeasure(concLvl, int(cfg.Period), cfg.MeasureFname)
		if err != nil {
			return nil, err
		}
	}
	go ct.handleReduce(c, false)

	// launch another reduce for secondary disk
	if cfg.ParallelIO {
		go ct.handleReduce(c, true)
	}
	return ct, nil
}

// Len returns the length of the current active view. A structure lenght
// is defined as the number of inserted elements on its underlying container,
// which disregards read operations. To interpret the absolute number of cmds
// safely discarded on ConcTable structures, just compute:
//   ct.logs[ct.current].last - ct.logs[ct.current].first + 1
func (ct *ConcTable) Len() uint64 {
	return uint64(len(ct.views[ct.cursor]))
}

// Log records the occurence of command 'cmd' on the provided index.
func (ct *ConcTable) Log(cmd *pb.Entry) error {
	ct.cursorMu.Lock()
	cur := ct.cursor

	// first command
	if ct.isMeasuringLat {
		ct.latMeasure.notifyReceivedCommandRand()
	}

	willReduce, advance := ct.willRequireReduceOnView(cmd.WriteOp, cur)
	if advance {
		ct.advanceCurrentView()
	}

	// must acquire view mutex before releasing cursor to ensure safety
	ct.mu[cur].Lock()
	ct.cursorMu.Unlock()

	if ct.isMeasuringLat {
		ct.latMeasure.notifyCommandWrite()
	}

	// adjust first structure index
	if !ct.logs[cur].logged {
		ct.logs[cur].first = cmd.Id
		ct.logs[cur].logged = true
	}

	if cmd.WriteOp {
		// update current state for that particular key
		ct.views[cur][cmd.Key] = cmd
	}
	// adjust last index
	ct.logs[cur].last = cmd.Id

	if willReduce {
		// mutext will be later unlocked by the logger routine
		if ct.isMeasuringLat {
			ct.latMeasure.notifyTableFill()
			ct.loggerReq <- logEvent{cur, ct.latMeasure.msrIndex}

		} else {
			ct.loggerReq <- logEvent{cur, -1}
		}
		return nil
	}

	ct.mu[cur].Unlock()
	return nil
}

// LogAndMeasureLat records the occurence of command 'cmd' on the provided
// index and measuremes the persisted timestamp for the informed batch if
// 'mustMeasureLat' is informed. This procedure allows the randomness of
// deciding wheter a batch must be measured be implemented outside of this
// library (i.e. on the caller scope), which allows for a complementary latency
// measurement rather than an isolated one. The structure must be initialized
// with Measure flag enabled, or else LogAndMeasureLat will panic.
func (ct *ConcTable) LogAndMeasureLat(cmd *pb.Entry, mustMeasureLat bool) (bool, error) {
	ct.cursorMu.Lock()
	cur := ct.cursor

	// mark this table as 'measured'
	if mustMeasureLat {
		ct.latMeasure.notifyReceivedCommandOnTable(cur)
	}

	willReduce, advance := ct.willRequireReduceOnView(cmd.WriteOp, cur)
	if advance {
		ct.advanceCurrentView()
	}

	// must acquire view mutex before releasing cursor to ensure safety
	ct.mu[cur].Lock()
	ct.cursorMu.Unlock()

	// adjust first structure index
	if !ct.logs[cur].logged {
		ct.logs[cur].first = cmd.Id
		ct.logs[cur].logged = true
	}

	if cmd.WriteOp {
		// update current state for that particular key
		ct.views[cur][cmd.Key] = cmd
	}
	// adjust last index
	ct.logs[cur].last = cmd.Id

	if willReduce {
		// mutext will be later unlocked by the logger routine
		if ct.latMeasure.mustMeasurePersistenceOnTable(cur) {
			ct.loggerReq <- logEvent{cur, ct.latMeasure.msrIndex}
			ct.latMeasure.msrIndex++

		} else {
			ct.loggerReq <- logEvent{cur, -1}
		}
		return true, nil
	}

	ct.mu[cur].Unlock()
	return false, nil
}

// Recov returns a compacted log of commands, following the requested [p, n]
// interval if 'Delayed' reduce is configured. On different period configurations,
// the entire reduced log is always returned. On persistent configuration (i.e.
// 'inmem' false) the entire log is loaded and then unmarshaled, consider using
// 'RecovBytes' calls instead. On CircBuff structures, indexes [p, n] are ignored.
func (ct *ConcTable) Recov(p, n uint64) ([]*pb.Entry, error) {
	if n < p {
		return nil, ErrInvalidRecovInterval
	}
	cur := ct.readAndAdvanceCurrentView()

	// sequentially reduce since 'Recov' will already be called concurrently
	exec, err := ct.mayExecuteLazyReduce(cur)
	if err != nil {
		return nil, err
	}

	var cmds []*pb.Entry
	if exec {
		defer ct.mu[cur].Unlock()

		// executed a lazy reduce, must read from the 'cur' log
		cmds, err = ct.logs[cur].retrieveLog()
		if err != nil {
			return nil, err
		}

	} else {
		// didnt execute, must read from the previous log cursor
		prev := atomic.LoadInt32(&ct.prevLog)
		cmds, err = ct.logs[prev].retrieveLog()
		if err != nil {
			return nil, err
		}
	}
	return cmds, nil
}

// RecovBytes returns an already serialized log, parsed from persistent storage
// or marshaled from the in-memory state. Its the most efficient approach on persistent
// configuration, avoiding an extra marshaling step during recovery. The command
// interpretation from the byte stream follows a simple slicing protocol, where
// the size of each command is binary encoded before the raw pbuff.
func (ct *ConcTable) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, ErrInvalidRecovInterval
	}
	cur := ct.readAndAdvanceCurrentView()

	// sequentially reduce since 'Recov' will already be called concurrently
	exec, err := ct.mayExecuteLazyReduce(cur)
	if err != nil {
		return nil, err
	}

	var raw []byte
	if exec {
		defer ct.mu[cur].Unlock()

		// executed a lazy reduce, must read from the 'cur' log
		raw, err = ct.logs[cur].retrieveRawLog(ct.logs[cur].first, ct.logs[cur].last)
		if err != nil {
			return nil, err
		}

	} else {
		// didnt execute, must read from the previous log cursor
		prev := atomic.LoadInt32(&ct.prevLog)
		raw, err = ct.logs[prev].retrieveRawLog(ct.logs[prev].first, ct.logs[prev].last)
		if err != nil {
			return nil, err
		}
	}
	return raw, nil
}

// RecovEntireLog ...
func (ct *ConcTable) RecovEntireLog() ([]byte, int, error) {
	fp := ct.logFolder + "*.log"
	fs, err := filepath.Glob(fp)
	if err != nil {
		return nil, 0, err
	}

	// sorts by lenght and lexicographically for equal len
	sort.Sort(byLenAlpha(fs))
	buf := bytes.NewBuffer(nil)

	for _, fn := range fs {
		fd, err := os.OpenFile(fn, os.O_RDONLY, 0400)
		if err != nil && err != io.EOF {
			return nil, 0, fmt.Errorf("failed while opening log '%s', err: '%s'", fn, err.Error())
		}
		defer fd.Close()

		// read the retrieved log interval
		var f, l uint64
		_, err = fmt.Fscanf(fd, "%d\n%d\n", &f, &l)
		if err != nil {
			return nil, 0, fmt.Errorf("failed while reading log '%s', err: '%s'", fn, err.Error())
		}

		// reset cursor
		_, err = fd.Seek(0, io.SeekStart)
		if err != nil {
			return nil, 0, fmt.Errorf("failed while reading log '%s', err: '%s'", fn, err.Error())
		}

		// each copy stages through a temporary buffer, copying to dest once completed
		_, err = io.Copy(buf, fd)
		if err != nil {
			return nil, 0, fmt.Errorf("failed while copying log '%s', err: '%s'", fn, err.Error())
		}
	}
	return buf.Bytes(), len(fs), nil
}

// persistTable applies the configured algorithm on a specific view and updates
// the latest log state into a new file.
func (ct *ConcTable) persistTable(id int, secDisk bool) error {
	cmds := generateLogFromTable(&ct.views[id])
	return ct.logs[id].updateLogState(cmds, ct.logs[id].first, ct.logs[id].last, secDisk)
}

func (ct *ConcTable) reduceLog(cur int, count *int, secDisk bool) error {
	err := ct.persistTable(cur, secDisk)
	if err != nil {
		return err
	}

	// always log, but reset persistent state only after 'resetOnImmediately' cmds
	if ct.logs[cur].config.Tick == Immediately {
		*count++
		if *count < resetOnImmediately {
			ct.mu[cur].Unlock()
			return nil
		}
		*count = 0
	}

	// update the last reduced index, its concurrently accessed by Recov procedures.
	// TODO: uncomment once Recov are evaluated...
	// atomic.StoreInt32(&ct.prevLog, int32(cur))

	// clean 'cur' view state
	ct.resetViewState(cur)
	ct.mu[cur].Unlock()
	return nil
}

func (ct *ConcTable) handleReduce(ctx context.Context, secDisk bool) {
	var count int
	for {
		select {
		case <-ctx.Done():
			return

		case event := <-ct.loggerReq:
			err := ct.reduceLog(event.table, &count, secDisk)
			if err != nil {
				log.Fatalln("failed during reduce procedure, err:", err.Error())
			}

			// requested latency measurement for persist
			if event.measure != -1 {
				ct.latMeasure.notifyTablePersistence(event.measure)
			}
		}
	}
}

// readAndAdvanceCurrentView reads the current view id then advances it to the next
// available identifier, returning the old observed value.
func (ct *ConcTable) readAndAdvanceCurrentView() int {
	ct.cursorMu.Lock()
	cur := ct.cursor
	ct.advanceCurrentView()
	ct.cursorMu.Unlock()
	return cur
}

// advanceCurrentView advances the current view to its next id.
func (ct *ConcTable) advanceCurrentView() {
	d := (ct.cursor - ct.concLevel + 1)
	ct.cursor = modInt(d, ct.concLevel)
}

// willRequireReduceOnView informs if a reduce procedure will later be trigged on a log procedure,
// and if the current view cursor must be advanced, following some specific rules:
//
// TODO: describe later...
func (ct *ConcTable) willRequireReduceOnView(wrt bool, id int) (bool, bool) {
	// write operation and immediately config
	if wrt && ct.logs[id].config.Tick == Immediately {
		return true, false
	}

	// read on immediately or delayed config, wont need reduce
	if ct.logs[id].config.Tick != Interval {
		return false, false
	}
	ct.logs[id].count++

	// reached reduce period
	if ct.logs[id].count >= ct.logs[id].config.Period {
		ct.logs[id].count = 0
		return true, true
	}
	return false, false
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'config.Period' wasnt reached yet. Returns true if reduce was executed, false otherwise.
//
// TODO: currently 'false' is always passed to persist procedure, which basically flushes
// and stores to the primary disk even if config.ParallelIO is set. Adjust recovery procedure
// implications later.
func (ct *ConcTable) mayExecuteLazyReduce(id int) (bool, error) {
	if ct.logs[id].config.Tick == Delayed {
		ct.mu[id].Lock()
		err := ct.persistTable(id, false)
		if err != nil {
			return true, err
		}

	} else if ct.logs[id].config.Tick == Interval && !ct.logs[id].firstReduceExists() {
		ct.mu[id].Lock()
		err := ct.persistTable(id, false)
		if err != nil {
			return true, err
		}

	} else {
		return false, nil
	}
	return true, nil
}

// retrieveCurrentViewCopy returns a copy of the current view, without advancing it.
// Used only for test purposes.
func (ct *ConcTable) retrieveCurrentViewCopy() stateTable {
	ct.cursorMu.Lock()
	defer ct.cursorMu.Unlock()
	return ct.views[ct.cursor]
}

// resetViewState cleans the current state of the informed view. Must be called from mutual
// exclusion scope.
func (ct *ConcTable) resetViewState(id int) {
	ct.views[id] = make(stateTable)

	// reset log data
	ct.logs[id].first, ct.logs[id].last = 0, 0
	ct.logs[id].logged = false
}

// Shutdown ...
func (ct *ConcTable) Shutdown() {
	ct.cancel()
	if ct.isMeasuringLat {
		ct.latMeasure.flush()
		ct.latMeasure.close()
	}
}

// generateLogFromTable applies the iterative compaction algorithm on a conflict-free view,
// mutual exclusion is done by outer scope.
func generateLogFromTable(tbl *stateTable) []*pb.Entry {
	log := make([]*pb.Entry, 0, len(*tbl))
	for _, c := range *tbl {
		log = append(log, c)
	}
	return log
}

// computes the modulu operation, returning the dividend signal result. In all cases
// b is ALWAYS a non-negative constant, which allows a minor optimization (one less
// comparison for b signal).
func modInt(a, b int) int {
	a = a % b
	if a >= 0 {
		return a
	}
	return a + b
}

// extractLocation returns the folder location specified in 'fn', searching for the
// occurence of slash characters ('/'). If none slash is found, "./" is returned instead.
//
// Example:
//   "/path/to/something/content.log" -> "/path/to/something/"
//   "foo.bar"                        -> "./"
func extractLocation(fn string) string {
	ind := strings.LastIndex(fn, "/") + 1
	if ind != -1 {
		return fn[:ind]
	}
	return "./"
}

type byLenAlpha []string

func (a byLenAlpha) Len() int      { return len(a) }
func (a byLenAlpha) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byLenAlpha) Less(i, j int) bool {
	// lenght order prio
	if len(a[i]) < len(a[j]) {
		return true
	}
	// alphabetic
	if len(a[i]) == len(a[j]) {
		return strings.Compare(a[i], a[j]) == -1
	}
	return false
}
