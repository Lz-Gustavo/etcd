package wal

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/pbutil"
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
//   * a temporary dir, the later rename to the actual filepath, and fsync calls were removed.
func CreateBeelogWAL(lg *zap.Logger, dirpath string, firstIdx, lastIdx uint64, logSize int) (*WAL, error) {
	// utilizing an always empty metadata for beelog WAL
	metadata := []byte{}

	// LGX: removed temporary dir creation here

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

	// LGX: removed WAL file preallocation here

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

	// LGX: removed empty snapshot save, dir rename, and fsync calls here
	return w, nil
}

// LGX: variant from Open(), openAtIndex() and selectWALFiles(). Open as ReadOnly, always starting
// on index 1.
func OpenBeelog(lg *zap.Logger, dirpath string, names []string, snap walpb.Snapshot) (*WAL, error) {
	// NOTE: do we need to check for WAL name format as etcd does?
	rs, ls, closer, err := openWALFiles(lg, dirpath, names, 1, false)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		lg:        lg,
		dir:       dirpath,
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}
	return w, nil
}

// LGX: ReadAllBeelog() is a variant of ReadAll()
func (w *WAL) ReadAllBeelog() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{}

	if w.decoder == nil {
		return nil, state, nil, ErrDecoderNotFound
	}
	decoder := w.decoder

	for err = decoder.decodeBeelog(rec); err == nil; err = decoder.decodeBeelog(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)

			// LGX: removed index verification assuming WAls are read in ascending order
			ents = append(ents, e)
			w.enti = e.Index

		case stateType:
			state = mustUnmarshalState(rec.Data)

		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data

		case crcType:
			// LGX: removed crcType verification

		case snapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data)
			if snap.Index == w.start.Index {
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
			}

		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// LGX: removed tail() invocation and match variable

	// close decoder, disable reading
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}

	w.start = walpb.Snapshot{}
	w.metadata = metadata
	w.decoder = nil

	if err == ErrCRCMismatch || err == walpb.ErrCRCMismatch || err == io.EOF {
		err = nil
	}
	return metadata, state, ents, err
}

// LGX: variant from wal.decode
func (d *decoder) decodeBeelog(rec *walpb.Record) error {
	rec.Reset()
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.brs) == 0 {
		return io.EOF
	}

	l, err := readInt64(d.brs[0])
	if err == io.EOF || (err == nil && l == 0) {
		// hit end of file or preallocated space
		d.brs = d.brs[1:]
		if len(d.brs) == 0 {
			return io.EOF
		}
		d.lastValidOff = 0
		return d.decodeRecord(rec)
	}
	if err != nil {
		return err
	}

	recBytes, padBytes := decodeFrameSize(l)
	if recBytes >= maxWALEntrySizeLimit-padBytes {
		return ErrMaxWALEntrySizeLimitExceeded
	}

	data := make([]byte, recBytes+padBytes)
	if _, err = io.ReadFull(d.brs[0], data); err != nil {
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// LGX: removed crc validation here

	// record decoded as valid; point last valid offset to end of record
	d.lastValidOff += frameSizeBytes + recBytes + padBytes
	return nil
}
