//go:build linux

package memfdcache

import (
	"os"
	"os/exec"
	"testing"

	"github.com/go-logr/logr"
)

// TestClientIntegrationCachecli drives the REAL shipping criu C client
// (criu/memfd-cache.c, compiled into the cachecli driver) against the REAL Go
// Server over a live socketpair session. TestSessionRoundTrip plays the client
// in Go, so it cannot catch a divergence between the C client's framing or
// validation and the server; this test closes that seam.
//
// Hermetic by default: it skips unless MEMFDCACHE_CACHECLI points at a compiled
// cachecli binary, so plain `go test ./...` needs no criu-upstream checkout.
// Build the binary and run this via testdata/run_integration.sh.
//
// cachecli runs a fixed sequence and exits non-zero on any client-visible
// mismatch: GET(miss) -> DONATE(sealed) -> GET(hit, size match) -> GET(miss,
// other key) -> DONATE(unsealed). The unsealed donation is a clean exchange on
// the wire but the server must decline the store on its independent F_GET_SEALS
// check; we verify that here via Stats(): exactly the one sealed inode remains.
func TestClientIntegrationCachecli(t *testing.T) {
	cli := os.Getenv("MEMFDCACHE_CACHECLI")
	if cli == "" {
		t.Skip("set MEMFDCACHE_CACHECLI to a compiled cachecli binary (see testdata/run_integration.sh)")
	}

	s := New(0, 0, logr.Discard())
	defer s.Close()

	criuEnd, err := s.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	cmd := exec.Command(cli)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{criuEnd} // becomes fd 3 in the child
	cmd.Env = append(os.Environ(), "CRIU_MEMFD_CACHE_SOCK=3")

	if err := cmd.Start(); err != nil {
		criuEnd.Close()
		t.Fatalf("start cachecli %q: %v", cli, err)
	}
	// Drop the parent's copy so the agent side sees EOF when the child exits,
	// mirroring NewSession's contract (the child holds its own dup).
	criuEnd.Close()

	if err := cmd.Wait(); err != nil {
		t.Fatalf("cachecli reported a failure (see stderr above): %v", err)
	}

	// Donates are committed synchronously before the client reads each response,
	// so entry counts are final once Wait returns.
	const wantSize = 8192 // CACHE_TEST_SIZE in cachecli.c
	n, b := s.Stats()
	if n != 1 {
		t.Fatalf("Stats entries = %d, want 1 (only the sealed donation cached; unsealed declined)", n)
	}
	if b != wantSize {
		t.Fatalf("Stats bytes = %d, want %d (cachecli's CACHE_TEST_SIZE)", b, wantSize)
	}
}
