package etcdserver

// LGX:
//   TODO: describe this storage implementation...
//
//   TODO2: later modify the current initialization of storage to utilize this implementation.

import (
	"context"
	"log"
	"os"
	"strconv"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/Lz-Gustavo/beemport"
	beelogpb "github.com/Lz-Gustavo/beemport/pb"
)

const (
	defaultBeelogConcLevel = 2
	defaultBeelogBatchSize = 1000
	defaultBeelogLogsDir   = "/tmp/beelog"
)

var (
	beelogBatchSize uint64
	beelogConcLevel int
	beelogLogsDir   string

	beelogParallelIO         bool
	beeelogSecondDiskLogsDir string
)

func init() {
	var err error
	bs := os.Getenv("ETCD_BEELOG_BATCH_SIZE")
	if beelogBatchSize, err = strconv.ParseUint(bs, 10, 32); err != nil {
		log.Println("using default value for ETCD_BEELOG_BATCH_SIZE")
		beelogBatchSize = defaultBeelogBatchSize
	}

	cl := os.Getenv("ETCD_BEELOG_CONC_LEVEL")
	if beelogConcLevel, err = strconv.Atoi(cl); err != nil {
		log.Println("using default value for ETCD_BEELOG_CONC_LEVEL")
		beelogConcLevel = defaultBeelogConcLevel
	}

	exists := false
	if beelogLogsDir, exists = os.LookupEnv("ETCD_BEELOG_LOGS_DIR"); !exists {
		log.Println("using default value for ETCD_BEELOG_LOGS_DIR")
		beelogLogsDir = defaultBeelogLogsDir
	}

	beelogParallelIO, _ = strconv.ParseBool(os.Getenv("ETCD_BEELOG_PARALLEL_IO"))
	if !beelogParallelIO {
		return
	}

	log.Println("ETCD_BEELOG_PARALLEL_IO enabled")
	if beeelogSecondDiskLogsDir, exists = os.LookupEnv("ETCD_BEELOG_SECOND_DISK_LOGS_DIR"); !exists {
		log.Println("using default value for ETCD_BEELOG_SECOND_DISK_LOGS_DIR")
		beeelogSecondDiskLogsDir = defaultBeelogLogsDir
	}
}

func configBeelog() *beemport.LogConfig {
	// NOTE: zero values are only declared for documentation purposes
	return &beemport.LogConfig{
		Sync:        false,
		Measure:     isMeasuringLatency,
		Tick:        beemport.Interval,
		Period:      uint32(beelogBatchSize),
		KeepAll:     true,
		Fname:       beelogLogsDir + "/beelog.log",
		ParallelIO:  beelogParallelIO,
		SecondFname: beeelogSecondDiskLogsDir + "/beelog.log",
	}
}

// beelogStorage...
type beelogStorage struct {
	wal *beemport.ConcTable
}

func NewBeelogStorage() Storage {
	ct, err := beemport.NewConcTableWithConfig(context.Background(), beelogConcLevel, configBeelog())
	if err != nil {
		log.Fatalln("failed initializing ConcTable structure")
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

		msr := mayMeasureCurrentEntry(ent.Index)
		bent := convertRaftEntryIntoBeelogEntry(ent)
		if bent == nil {
			log.Fatalln("could not convert entry", ent.Index)
		}

		if err := bs.wal.LogAndMeasureLat(bent, msr); err != nil {
			return err
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
		bent = &beelogpb.Entry{
			Id:      entry.Index,
			Key:     string(raftReq.Put.Key),
			WriteOp: isWriteOp,
			Command: entry.Data,
		}
		return bent
	}

	bent = &beelogpb.Entry{
		Id:      entry.Index,
		Key:     string(raftReq.Range.Key),
		WriteOp: false,
	}
	return bent
}
