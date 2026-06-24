package memfdcache

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// newSocketpair returns one AF_UNIX SOCK_SEQPACKET pair: the agent-side raw fd
// (close-on-exec so it never leaks into restored workloads) and the criu-side
// *os.File (handed to nsrestore via ExtraFiles, then to criu swrk by go-criu).
func newSocketpair() (agentFd int, criuEnd *os.File, err error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
	if err != nil {
		return -1, nil, fmt.Errorf("socketpair: %w", err)
	}
	unix.CloseOnExec(fds[0])
	return fds[0], os.NewFile(uintptr(fds[1]), "memfd-cache-criu"), nil
}

// recvMsg reads one SEQPACKET datagram into p; n is the data length.
func recvMsg(sock int, p []byte) (int, error) {
	n, _, _, _, err := unix.Recvmsg(sock, p, nil, 0)
	return n, err
}

// sendMsg writes p as one SEQPACKET datagram.
func sendMsg(sock int, p []byte) error {
	return unix.Sendmsg(sock, p, nil, nil, 0)
}

// sendFD passes one fd over the socket via SCM_RIGHTS with a 1-byte dummy iov,
// matching CRIU's recv_fds() expectation (criu/include/common/scm-code.c).
func sendFD(sock int, fd int) error {
	return unix.Sendmsg(sock, []byte{0}, unix.UnixRights(fd), nil, 0)
}

// recvFD receives one fd passed via SCM_RIGHTS. The caller owns the returned fd.
func recvFD(sock int) (int, error) {
	buf := make([]byte, 1)
	oob := make([]byte, unix.CmsgSpace(4)) // room for exactly one fd
	_, oobn, _, _, err := unix.Recvmsg(sock, buf, oob, 0)
	if err != nil {
		return -1, err
	}
	scms, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return -1, fmt.Errorf("parse SCM: %w", err)
	}
	if len(scms) == 0 {
		return -1, fmt.Errorf("no SCM_RIGHTS in donate message")
	}
	fds, err := unix.ParseUnixRights(&scms[0])
	if err != nil {
		return -1, fmt.Errorf("parse SCM_RIGHTS: %w", err)
	}
	if len(fds) != 1 {
		// Defensively close any unexpected extras so we never leak fds.
		for _, f := range fds {
			_ = unix.Close(f)
		}
		return -1, fmt.Errorf("expected 1 fd, got %d", len(fds))
	}
	return fds[0], nil
}
