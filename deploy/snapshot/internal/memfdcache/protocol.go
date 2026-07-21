// Package memfdcache implements the node-local memfd content cache server that
// runs inside the snapshot-agent DaemonSet. It holds populated, sealed memfds
// open across restores so a second restore of the same checkpoint on the same
// node can borrow the already-filled inode (one shared physical copy) instead
// of re-reading and re-copying its pages.
//
// The CRIU restore process is the client (criu/memfd-cache.c). One AF_UNIX
// SOCK_SEQPACKET socketpair brackets one restore: the agent holds one end and
// services it here; the other is inherited by criu via CRIU_MEMFD_CACHE_SOCK.
// SCM_RIGHTS over the inherited socketpair is namespace-agnostic, so the fd
// crosses the placeholder mount namespace with no bind-mounting. Closing the
// criu end (restore complete) releases every borrow taken on it.
//
// Sharing safety: only inodes sealed F_SEAL_FUTURE_WRITE are cached/shared. The
// kernel then forbids any writable mapping of the inode, so the single shared
// copy cannot be corrupted by any borrower. The server independently verifies
// the seal on the donated fd; it never trusts the request's declared seals.
package memfdcache

import (
	"encoding/binary"
	"fmt"
)

// Wire layout (little-endian, fixed; agent and criu always share a node/arch).
// Must match struct memfd_cache_{key,req,resp} in criu/include/memfd-cache.h.
const (
	cacheIDMax = 128

	keySize  = 8 + 4 + 4 + cacheIDMax // shmid + uid + gid + id[128] = 144
	reqSize  = 4 + 4 + 8 + keySize    // op + seals + size + key      = 160
	respSize = 4 + 4                  // status + pad                 = 8
)

// enum memfd_cache_op
const (
	opGet    uint32 = 1
	opDonate uint32 = 2
)

// enum memfd_cache_status
const (
	statusHit     uint32 = 1
	statusMiss    uint32 = 2
	statusOK      uint32 = 3
	statusDecline uint32 = 4
)

// F_SEAL_FUTURE_WRITE (linux/fcntl.h, Linux 5.1+). The sharing-safety gate.
const fSealFutureWrite = 0x0010

// request is the decoded struct memfd_cache_req.
type request struct {
	op    uint32
	seals uint32
	size  uint64
	id    string // "checkpointID:version"
	shmid uint64
	uid   uint32
	gid   uint32
}

func decodeRequest(b []byte) (request, error) {
	if len(b) < reqSize {
		return request{}, fmt.Errorf("short request: %d < %d", len(b), reqSize)
	}
	le := binary.LittleEndian
	r := request{
		op:    le.Uint32(b[0:4]),
		seals: le.Uint32(b[4:8]),
		size:  le.Uint64(b[8:16]),
		shmid: le.Uint64(b[16:24]),
		uid:   le.Uint32(b[24:28]),
		gid:   le.Uint32(b[28:32]),
	}
	// id[128] at offset 32, NUL-padded.
	idb := b[32 : 32+cacheIDMax]
	if n := indexByte(idb, 0); n >= 0 {
		idb = idb[:n]
	}
	r.id = string(idb)
	return r, nil
}

func encodeRequest(r request) []byte {
	b := make([]byte, reqSize)
	le := binary.LittleEndian
	le.PutUint32(b[0:4], r.op)
	le.PutUint32(b[4:8], r.seals)
	le.PutUint64(b[8:16], r.size)
	le.PutUint64(b[16:24], r.shmid)
	le.PutUint32(b[24:28], r.uid)
	le.PutUint32(b[28:32], r.gid)
	copy(b[32:32+cacheIDMax], r.id)
	return b
}

func encodeResp(status uint32) []byte {
	b := make([]byte, respSize)
	binary.LittleEndian.PutUint32(b[0:4], status)
	return b
}

func decodeRespStatus(b []byte) (uint32, error) {
	if len(b) < respSize {
		return 0, fmt.Errorf("short response: %d < %d", len(b), respSize)
	}
	return binary.LittleEndian.Uint32(b[0:4]), nil
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}
