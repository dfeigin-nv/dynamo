// Command memfdcache-testserve wraps the SHIPPING memfd-cache Server
// (internal/memfdcache) around one or more criu invocations for the Tier-2 CRIU
// restore integration tests. It is a test harness, not a production tool.
//
// It creates one long-lived Server, then runs each "--"-separated argv group as
// a fresh cache session against that SAME server -- so cache state (donated
// inodes) persists across invocations. That is what makes a donate->HIT cycle
// observable: run 1 (MISS+donate) and run 2 (HIT) must share one server.
//
// Each session: open a socketpair via Server.NewSession, pass the criu end as
// ExtraFiles[0] (fd 3 in the child), set CRIU_MEMFD_CACHE_SOCK=3, exec the argv,
// wait. This mirrors the agent's executor.ExecuteRestore wiring exactly, but
// execs criu directly instead of going through nsenter+nsrestore.
//
// Stats() (entry count + cached bytes) are printed after each session and a
// final "STATS entries=N bytes=B" line goes to stdout for scripts to parse.
//
// Usage:
//
//	memfdcache-testserve [-max-bytes N] [-idle-ttl D] -- \
//	    criu restore --memfd-cache --memfd-cache-id ckpt:1 -D /img --shell-job
//
//	# two sessions against one server (donate -> HIT):
//	memfdcache-testserve -- criu restore ... -D /img -- criu restore ... -D /img
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/logging"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/memfdcache"
)

func main() {
	maxBytes := flag.Int64("max-bytes", 0, "cache RAM budget in bytes (0 = unlimited)")
	idleTTL := flag.Duration("idle-ttl", 0, "evict cold entries after this idle duration (0 = never)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: memfdcache-testserve [-max-bytes N] [-idle-ttl D] -- criu <args> [-- criu <args> ...]")
		flag.PrintDefaults()
	}
	flag.Parse()

	groups := splitGroups(flag.Args())
	if len(groups) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	log := logging.ConfigureLogger("stderr").WithName("memfdcache-testserve")
	srv := memfdcache.New(*maxBytes, *idleTTL, log)
	defer srv.Close()

	for i, argv := range groups {
		if err := runSession(srv, argv); err != nil {
			fmt.Fprintf(os.Stderr, "memfdcache-testserve: session %d %v failed: %v\n", i+1, argv, err)
			os.Exit(1)
		}
		n, b := srv.Stats()
		fmt.Fprintf(os.Stderr, "memfdcache-testserve: after session %d: entries=%d bytes=%d\n", i+1, n, b)
	}

	n, b := srv.Stats()
	fmt.Printf("STATS entries=%d bytes=%d\n", n, b)
}

// splitGroups splits argv on standalone "--" tokens into command groups. The
// flag package already consumes the first lone "--" (the flag/command boundary),
// so only separators between groups reach here.
func splitGroups(args []string) [][]string {
	var groups [][]string
	var cur []string
	for _, a := range args {
		if a == "--" {
			if len(cur) > 0 {
				groups = append(groups, cur)
				cur = nil
			}
			continue
		}
		cur = append(cur, a)
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups
}

func runSession(srv *memfdcache.Server, argv []string) error {
	criuEnd, err := srv.NewSession()
	if err != nil {
		return fmt.Errorf("new cache session: %w", err)
	}

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{criuEnd} // criu end -> fd 3 in the child
	cmd.Env = append(os.Environ(), "CRIU_MEMFD_CACHE_SOCK=3")

	if err := cmd.Start(); err != nil {
		criuEnd.Close()
		return fmt.Errorf("start %s: %w", argv[0], err)
	}
	// Drop the parent's copy so the agent side sees EOF when criu exits (the
	// child holds its own dup), releasing every borrow taken on this session.
	criuEnd.Close()
	return cmd.Wait()
}
