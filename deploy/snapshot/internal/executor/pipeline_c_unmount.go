// Tiny syscall wrapper so pipeline_c_dispatch.go can call Unmount
// without bringing in the broader syscall surface. Mirrors the
// pattern in nsrestore.go where `syscall.Unmount("/dev/shm", 0)` is
// inlined; we just split it to a helper to keep the dispatch file
// import list narrow.

package executor

import "syscall"

func tryUnmount(target string) error {
	return syscall.Unmount(target, 0)
}
