package beemport

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"time"
)

const (
	// Every measureInitLat invocation has a '1/measureChance' chance to set 'hold'
	// value, and capturing timestamps for latency analysis until latency tuple is
	// recorded.
	measureChance int = 1

	initArraySize = 10000000
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// latencyMeasure holds auxiliar variables to implement an in-deep latency analysis
// on ConcTable operations.
type latencyMeasure struct {
	drawn    bool
	absIndex int
	msrIndex int
	interval int
	outFile  *os.File

	counter    int
	fillState  int
	perstState int

	initLat  [initArraySize]int64
	writeLat [initArraySize]int64
	fillLat  [initArraySize]int64
	perstLat [initArraySize]int64
}

func newLatencyMeasure(concLvl, interval int, filename string) (*latencyMeasure, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &latencyMeasure{
		interval: interval,
		outFile:  fd,
	}, nil
}

func (lm *latencyMeasure) notifyReceivedCommandRand() {
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

func (lm *latencyMeasure) notifyTablePersistence(tableID int) {
	lm.perstLat[tableID] = time.Now().UnixNano()
}

// notifyReceivedCommandETCD measures the received timestamp of any command within the
// configured batch, and not only on the first command. This behavior differs from
// notifyReceivedCommand() and its utilized as a server-side latency measurement instead
// of conctable evaluation.
func (lm *latencyMeasure) notifyReceivedCommandETCD() {
	lm.initLat[lm.counter] = time.Now().UnixNano()
}

func (lm *latencyMeasure) notifyCommandWriteETCD() {
	lm.writeLat[lm.counter] = time.Now().UnixNano()
	lm.counter++
}

func (lm *latencyMeasure) notifyTableFillETCD() {
	for i := lm.fillState; i < lm.counter; i++ {
		lm.fillLat[i] = time.Now().UnixNano()
	}
	lm.fillState = lm.counter
}

func (lm *latencyMeasure) notifyTablePersistenceETCD() {
	for i := lm.perstState; i < lm.counter; i++ {
		lm.perstLat[i] = time.Now().UnixNano()
	}
	lm.perstState = lm.counter
}

func (lm *latencyMeasure) flush() error {
	var err error
	buff := bytes.NewBuffer(nil)

	for i, init := range lm.initLat {
		w := lm.writeLat[i]
		f := lm.fillLat[i]
		p := lm.perstLat[i]

		// got the maximum number of unique-size tuples
		if init == 0 || w == 0 || f == 0 || p == 0 {
			break
		}

		_, err = fmt.Fprintf(buff, "%d,%d,%d,%d\n", init, w, f, p)
		if err != nil {
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
