package wal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

// LGX: utilized only to analyze O_SYNC flag implications on etcd standard
// WAL performance. The syncIO global is only utilized on wal.go to inform
// the O_SYNC flag to os.Open() if enabled.
var syncIO bool

func init() {
	syncIO, _ = strconv.ParseBool(os.Getenv("ETCD_SYNC_IO"))
	if syncIO {
		log.Println("ETCD_SYNC_IO enabled for standard WAL")
	}
}

// LGX:
// CreateBeelogWAL executes the same procedures as Create(), except that:
//   * WALs are named after the paremeters 'firstIdx' and 'lastIdx' informed,
//     and with .log extension
//   * there can be multiple WALs on the same dirpath
//   * an empty metadata is always used
//   * a temporary dir, and a later rename to the actual filepath, are not done.
//     (the previous instructions for this procedure are intentionally left commented for now)
func CreateBeelogWAL(lg *zap.Logger, dirpath string, firstIdx, lastIdx uint64, logSize int) (*WAL, error) {
	// utilizing an always empty metadata for beelog WAL
	metadata := []byte{}

	// keep temporary wal directory so WAL initialization appears atomic
	// tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	// if fileutil.Exist(tmpdirpath) {
	// 	if err := os.RemoveAll(tmpdirpath); err != nil {
	// 		return nil, err
	// 	}
	// }

	// if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
	// 	if lg != nil {
	// 		lg.Warn(
	// 			"failed to create a temporary WAL directory",
	// 			zap.String("tmp-dir-path", tmpdirpath),
	// 			zap.String("dir-path", dirpath),
	// 			zap.Error(err),
	// 		)
	// 	}
	// 	return nil, err
	// }

	p := filepath.Join(dirpath, fmt.Sprintf("%d-%d-%d.log", firstIdx, lastIdx, logSize))

	// LGX: modified LockFile flag parameters to evaluate O_SYNC implications
	// on wal performance.
	flags := os.O_WRONLY | os.O_CREATE
	if syncIO {
		flags = flags | os.O_SYNC
	}

	f, err := fileutil.LockFile(p, flags, fileutil.PrivateFileMode)
	if err != nil {
		if lg != nil {
			lg.Warn(
				"failed to flock an initial WAL file",
				zap.String("path", p),
				zap.Error(err),
			)
		}
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		if lg != nil {
			lg.Warn(
				"failed to seek an initial WAL file",
				zap.String("path", p),
				zap.Error(err),
			)
		}
		return nil, err
	}
	// if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
	// 	if lg != nil {
	// 		lg.Warn(
	// 			"failed to preallocate an initial WAL file",
	// 			zap.String("path", p),
	// 			zap.Int64("segment-bytes", SegmentSizeBytes),
	// 			zap.Error(err),
	// 		)
	// 	}
	// 	return nil, err
	// }

	w := &WAL{
		lg:       lg,
		dir:      dirpath,
		metadata: metadata,
	}
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	w.locks = append(w.locks, f)
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}

	// NOTE: why saving an empty Snapshot? maybe propose a PR avoiding this
	// cost on oficial etcd
	//
	// if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
	// 	return nil, err
	// }

	// if w, err = w.renameWAL(tmpdirpath); err != nil {
	// 	if lg != nil {
	// 		lg.Warn(
	// 			"failed to rename the temporary WAL directory",
	// 			zap.String("tmp-dir-path", tmpdirpath),
	// 			zap.String("dir-path", w.dir),
	// 			zap.Error(err),
	// 		)
	// 	}
	// 	return nil, err
	// }

	// LGX: also comment Fsync procedures, since WALs are not renamed
	//
	// var perr error
	// defer func() {
	// 	if perr != nil {
	// 		w.cleanupWAL(lg)
	// 	}
	// }()

	// // directory was renamed; sync parent dir to persist rename
	// pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	// if perr != nil {
	// 	if lg != nil {
	// 		lg.Warn(
	// 			"failed to open the parent data directory",
	// 			zap.String("parent-dir-path", filepath.Dir(w.dir)),
	// 			zap.String("dir-path", w.dir),
	// 			zap.Error(perr),
	// 		)
	// 	}
	// 	return nil, perr
	// }
	// start := time.Now()
	// if perr = fileutil.Fsync(pdir); perr != nil {
	// 	if lg != nil {
	// 		lg.Warn(
	// 			"failed to fsync the parent data directory file",
	// 			zap.String("parent-dir-path", filepath.Dir(w.dir)),
	// 			zap.String("dir-path", w.dir),
	// 			zap.Error(perr),
	// 		)
	// 	}
	// 	return nil, perr
	// }
	// walFsyncSec.Observe(time.Since(start).Seconds())

	// if perr = pdir.Close(); perr != nil {
	// 	if lg != nil {
	// 		lg.Warn(
	// 			"failed to close the parent data directory file",
	// 			zap.String("parent-dir-path", filepath.Dir(w.dir)),
	// 			zap.String("dir-path", w.dir),
	// 			zap.Error(perr),
	// 		)
	// 	}
	// 	return nil, perr
	// }

	return w, nil
}

// LGX:
// TODO: maybe implement beelog variant based on wal.ReadAll()?
// ReadAll would still be compatible, but this variant could be less expensive since
// WAL is split on different files (e.g. different lock strategy)
func (w *WAL) ReadAllBeelog() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	return nil, raftpb.HardState{}, nil, nil
}

// LGX: variant from Open(), openAtIndex() and selectWALFiles(). Open as ReadOnly, always starting
// on index 1.
func OpenBeelog(lg *zap.Logger, dirpath string) (*WAL, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, err
	}

	rs, ls, closer, err := openWALFiles(lg, dirpath, names, 1, false)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		lg:        lg,
		dir:       dirpath,
		start:     walpb.Snapshot{},
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}
	return w, nil
}
