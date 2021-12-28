package beemport

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	// Every measureInitLat invocation has a '1/measureChance' chance to set 'hold'
	// value, and capturing timestamps for latency analysis until latency tuple is
	// recorded.
	measureChance int = 1

	// 1kk measurements on 4 arrays of 64bits values, 32MB total
	arraySize = 1000000
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// latencyMeasure holds auxiliar variables to implement an in-deep latency analysis
// on ConcTable operations.
type latencyMeasure struct {
	drawn     bool
	absIndex  int
	msrIndex  int
	interval  int
	tableMark []bool
	outFile   *os.File

	initLat  [arraySize]int64
	writeLat [arraySize]int64
	fillLat  [arraySize]int64
	perstLat [arraySize]int64
	perstMu  sync.Mutex
}

func newLatencyMeasure(concLvl, interval int, filename string) (*latencyMeasure, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &latencyMeasure{
		interval:  interval,
		outFile:   fd,
		tableMark: make([]bool, concLvl),
	}, nil
}

func (lm *latencyMeasure) notifyReceivedCommandRand() {
	if lm.msrIndex >= arraySize {
		return
	}

	lm.absIndex++
	if (lm.absIndex%lm.interval == 1 || lm.interval == 1) && rand.Intn(measureChance) == 0 {
		lm.initLat[lm.msrIndex] = time.Now().UnixNano()
		lm.drawn = true
	}
}

func (lm *latencyMeasure) notifyCommandWrite() {
	if !lm.drawn {
		return
	}

	if lm.absIndex%lm.interval == 1 {
		// first command was written into table
		lm.writeLat[lm.msrIndex] = time.Now().UnixNano()

	} else if lm.interval == 1 {
		// special case of first and last command, which does not fall
		// on first condition
		lm.writeLat[lm.msrIndex] = time.Now().UnixNano()
		lm.fillLat[lm.msrIndex] = time.Now().UnixNano()

	} else if lm.absIndex%lm.interval == 0 {
		// last command, table filled
		lm.fillLat[lm.msrIndex] = time.Now().UnixNano()
	}
}

func (lm *latencyMeasure) notifyTableFill() {
	if !lm.drawn {
		return
	}

	lm.msrIndex++
	lm.drawn = false
}

func (lm *latencyMeasure) notifyTablePersistence(msrIndex int) {
	if msrIndex >= arraySize {
		return
	}

	t := time.Now().UnixNano()
	lm.perstMu.Lock()
	lm.perstLat[msrIndex] = t
	lm.perstMu.Unlock()
}

func (lm *latencyMeasure) notifyReceivedCommandOnTable(tableID int) {
	lm.tableMark[tableID] = true
}

func (lm *latencyMeasure) mustMeasurePersistenceOnTable(tableID int) bool {
	if lm.tableMark[tableID] {
		lm.tableMark[tableID] = false
		return true
	}
	return false
}

func (lm *latencyMeasure) flush() error {
	var err error
	buff := bytes.NewBuffer(nil)

	lm.perstMu.Lock()
	defer lm.perstMu.Unlock()

	for i, init := range lm.initLat {
		w := lm.writeLat[i]
		f := lm.fillLat[i]
		p := lm.perstLat[i]

		// got the maximum number of unique-size tuples, since p is always the last
		// one to be written
		if p == 0 {
			break
		}

		// measure only p if other values are not present
		if init == 0 || w == 0 || f == 0 {
			if _, err = fmt.Fprintf(buff, "%d\n", p); err != nil {
				return err
			}
			continue
		}

		if _, err = fmt.Fprintf(buff, "%d,%d,%d,%d\n", init, w, f, p); err != nil {
			return err
		}
	}

	if _, err = buff.WriteTo(lm.outFile); err != nil {
		return err
	}
	return nil
}

func (lm *latencyMeasure) close() {
	lm.outFile.Close()
}
