package etcdserver

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
)

// LGX: describe this batch wal wrapper over the standard etcd wal

const defaultBatchWALLatFile = "/tmp/bw-latency.out"

type batchWALStorage struct {
	buff  []raftpb.Entry
	count int
	batch int

	latBuff *bytes.Buffer
	latFile *os.File

	lastState raftpb.HardState
	wal       *wal.WAL
}

func NewBatchWALStorage(w *wal.WAL) Storage {
	sb := &batchWALStorage{
		buff:  make([]raftpb.Entry, 0, logBatchSize),
		batch: int(logBatchSize),
		wal:   w,
	}

	if isMeasuringLatency {
		sb.setupLatencyMeasurement()
	}
	return sb
}

// NOTE: we still dont understand exactly:
//  (i) why different sizes of 'ents' slice during execution, and why does
//  it even batches commands on this Raft implementation
//  (ii) the implications of calling a single wal.Save() with a configured number
//  of entries at once
//
// The ideia is to evaluate this scenarios during experimentation. Because of both
// (i) and (ii), we currently implement batching on the standard WAL by logging
// the batched commands if the size of 'ents' exceeds the current count within the
// batch size. As an example, if a call to 'Save()' informs an 'ents' slice with 5
// entries, and the current batch of size 10 already has 7 (so exceeding the size by 2)
// the idea is that only the batched 7 are logged and the remaining 5 are allocated
// into the next batch.
func (sb *batchWALStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	sb.count += len(ents)
	if sb.count < sb.batch {
		sb.lastState = st
		sb.buff = append(sb.buff, ents...)
		return nil
	}

	err := sb.wal.Save(sb.lastState, sb.buff)
	sb.lastState = st
	sb.count = len(ents)
	// keeps the underlying array
	sb.buff = append(sb.buff[:0], ents...)

	if isMeasuringLatency {
		// 0 on the etcd global latBuff indicates that the previous measurement was the
		// last command of that batch
		fmt.Fprintln(latBuff, 0)
		fmt.Fprintln(sb.latBuff, time.Now().UnixNano())
	}
	return err
}

func (sb *batchWALStorage) SaveSnap(snap raftpb.Snapshot) error {
	return nil
}

func (sb *batchWALStorage) Close() error {
	sb.flushLatBufferIntoFile()
	return sb.wal.Close()
}

func (sb *batchWALStorage) Release(snap raftpb.Snapshot) error {
	return nil
}

func (sb *batchWALStorage) Sync() error {
	return sb.wal.Sync()
}

func (sb *batchWALStorage) setupLatencyMeasurement() {
	var fn string
	if fn = os.Getenv("ETCD_BATCHWAL_LAT_FILE"); fn == "" {
		log.Println("using default value for ETCD_BATCHWAL_LAT_FILE")
		fn = defaultBatchWALLatFile
	}

	fd, err := os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalln("failed initializing batchWAL latency measurement, err:", err.Error())
	}
	sb.latFile = fd
	sb.latBuff = &bytes.Buffer{}
}

func (sb *batchWALStorage) flushLatBufferIntoFile() {
	if _, err := sb.latBuff.WriteTo(sb.latFile); err != nil {
		log.Fatalln("failed copying batchWAL latBuff into file, err:", err.Error())
	}
	sb.latFile.Close()
}
