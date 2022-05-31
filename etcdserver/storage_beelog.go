package etcdserver

// LGX: DEPRECATED
// This Storage implementation is not utilized anymore while evaluating etcd's performance
// with beelog configuration. Current experiments regarding beelog are done through its
// own implementation of the Raft handle function in etcdserver/raft.go/startBeelog(),
// and by utilizing an internal implementation (i.e. instead of an external pkg) defined
// on etcdserver/beelog.go. This beelogStorage used to be initilized on server.go/NewServer()
// in case a Beelog logconfig ENV was set, and is deprecated since commit
// https://github.com/Lz-Gustavo/etcd/commit/9e2e6773a57629e8a7a0d378e1fb7dfaba56786c.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/expconfig"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/Lz-Gustavo/beemport"
	beelogpb "github.com/Lz-Gustavo/beemport/pb"
)

const (
	defaultBeelogConcLevel = 2
	defaultBeelogLogsDir   = "/tmp/beelog"
	defaultBeelogLatFile   = "/tmp/bl-latency.out"
)

func getBeelogConfig() *beemport.LogConfig {
	latFname := os.Getenv("ETCD_BEELOG_LAT_FILE")
	if latFname == "" {
		log.Println("using default value for ETCD_BEELOG_LAT_FILE")
		latFname = defaultBeelogLatFile
	}

	logsDir, exists := os.LookupEnv("ETCD_BEELOG_LOGS_DIR")
	if !exists {
		log.Println("using default value for ETCD_BEELOG_LOGS_DIR")
		logsDir = defaultBeelogLogsDir
	}

	syncIO, _ := strconv.ParseBool(os.Getenv("ETCD_SYNC_IO"))
	if syncIO {
		log.Println("ETCD_SYNC_IO enabled for beelog")
	}

	var secondDiskLogsDir string
	parallelIO, _ := strconv.ParseBool(os.Getenv("ETCD_BEELOG_PARALLEL_IO"))
	if parallelIO {
		log.Println("ETCD_BEELOG_PARALLEL_IO enabled")

		secondDiskLogsDir, exists = os.LookupEnv("ETCD_BEELOG_SECOND_DISK_LOGS_DIR")
		if !exists {
			log.Println("using default value for ETCD_BEELOG_SECOND_DISK_LOGS_DIR")
			secondDiskLogsDir = defaultBeelogLogsDir
		}
	}

	return &beemport.LogConfig{
		Sync:         syncIO,
		Tick:         beemport.Interval,
		Period:       uint32(expconfig.LogBatchSize),
		KeepAll:      true,
		Fname:        logsDir + "/beelog.log",
		ParallelIO:   parallelIO,
		SecondFname:  secondDiskLogsDir + "/beelog.log",
		Measure:      isMeasuringLatency,
		MeasureFname: latFname,
	}
}

// beelogStorage...
type beelogStorage struct {
	wal *beemport.ConcTable
}

func NewBeelogStorage() Storage {
	concLevel, err := strconv.Atoi(os.Getenv("ETCD_BEELOG_CONC_LEVEL"))
	if err != nil {
		log.Println("using default value for ETCD_BEELOG_CONC_LEVEL")
		concLevel = defaultBeelogConcLevel
	}

	ct, err := beemport.NewConcTableWithConfig(context.Background(), concLevel, getBeelogConfig())
	if err != nil {
		log.Fatalln("failed initializing ConcTable structure, err:", err.Error())
	}
	return &beelogStorage{ct}
}

func (bs *beelogStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	for _, ent := range ents {
		// non-command entries (i.e. config changes) wont be logged within beelog. This wont
		// be a problem for a while, since the first exemperiments will consider a stable
		// cluster. Also, the first throughput measurements are always discard due to system
		// setup, which will ensure a fair comparison between both strategies. Once we start
		// measuring recovery, we'll think of a better way to log these.
		if ent.Type != raftpb.EntryNormal {
			continue
		}

		ts, msr := mayMeasureLat()
		bent := convertRaftEntryIntoBeelogEntry(ent)
		if bent == nil {
			log.Println("could not convert entry", ent.Index)
			continue
		}

		// NOTE: the idea is to save the previously taken timestamp only if an entry is
		// successfully converted to a beelog entry, avoiding the necessity to erase the
		// buffer if it isnt
		if msr {
			fmt.Fprintln(latBuff, ts)
		}

		isLastCommand, err := bs.wal.LogAndMeasureLat(bent, msr)
		if err != nil {
			return err
		}

		// 0 indicates that the previous measurement was the last command of that batch,
		// must log the mark even if 'msr' is unset
		if isLastCommand {
			fmt.Fprintln(latBuff, 0)
		}
	}
	return nil
}

func (bs *beelogStorage) Close() error {
	bs.wal.Shutdown()
	return nil
}

func (bs *beelogStorage) SaveSnap(snap raftpb.Snapshot) error {
	return nil
}

func (bs *beelogStorage) Release(snap raftpb.Snapshot) error {
	return nil
}

func (bs *beelogStorage) Sync() error {
	return nil
}

func convertRaftEntryIntoBeelogEntry(entry raftpb.Entry) *beelogpb.Entry {
	// NOTE: unmarshaling here is excessively expensive since it is also unmarshaled
	// later when request is applied. This will result in a significant performance
	// overhead which will compromise our experiments.
	//
	// TODO: think of a better way to log this...
	var raftReq pb.InternalRaftRequest
	if !pbutil.MaybeUnmarshal(&raftReq, entry.Data) {
		return nil
	}

	if raftReq.Put == nil && raftReq.Range == nil {
		return nil
	}

	var bent *beelogpb.Entry
	isWriteOp := raftReq.Put != nil
	if isWriteOp {
		key, err := binary.ReadVarint(bytes.NewReader(raftReq.Put.Key))
		if err != nil {
			return nil
		}

		bent = &beelogpb.Entry{
			Id:      entry.Index,
			Key:     key,
			WriteOp: isWriteOp,
			Command: entry.Data,
		}
		return bent
	}

	key, err := binary.ReadVarint(bytes.NewReader(raftReq.Range.Key))
	if err != nil {
		return nil
	}

	bent = &beelogpb.Entry{
		Id:      entry.Index,
		Key:     key,
		WriteOp: false,
	}
	return bent
}
