#!/usr/bin/env bash
#
# Tier-1 cross-language test: compile the SHIPPING criu memfd-cache client
# (criu/memfd-cache.c) into the self-asserting cachecli driver and run it against
# the REAL Go cache Server via the gated integration test. This exercises the one
# seam Go-only tests cannot: the C client's framing/validation vs the server.
#
# LOCAL, unprivileged: no CAP_CHECKPOINT_RESTORE, no GPU, no criu binary build.
#
# Usage:
#   bash internal/memfdcache/testdata/run_integration.sh
#   CRIU_SRC=/path/to/criu-upstream bash .../run_integration.sh   # override checkout
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # internal/memfdcache/testdata
PKG_DIR="$(dirname "$HERE")"                          # internal/memfdcache

# criu-upstream is a sibling of the dynamo checkout by default (both under the
# checkpoints workspace root); override with CRIU_SRC.
DEFAULT_CRIU="$(cd "$HERE/../../../../../.." && pwd)/criu-upstream"
CRIU_SRC="${CRIU_SRC:-$DEFAULT_CRIU}"

CC_SRC="$CRIU_SRC/test/memfdcache"
CLIENT="$CRIU_SRC/criu/memfd-cache.c"
for f in "$CC_SRC/cachecli.c" "$CC_SRC/scm_shim.c" "$CLIENT"; do
	if [ ! -f "$f" ]; then
		echo "error: missing $f" >&2
		echo "set CRIU_SRC to the criu-upstream checkout (currently: $CRIU_SRC)" >&2
		exit 1
	fi
done

CC="${CC:-cc}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
BIN="$WORK/cachecli"

echo ">> building cachecli (real criu client linked to the Go-server protocol)"
# -iquote, NOT -I: criu ships its own criu/include/{string,fcntl}.h. -iquote keeps
# <string.h>/<fcntl.h> resolving to libc while "memfd-cache.h" / "common/scm.h" /
# "images/memfd.pb-c.h" resolve into the criu tree -- the same scheme criu's own
# Makefile uses. Plain -I would shadow libc headers and miscompile silently.
"$CC" -Wall -Wextra -Werror \
	-iquote "$CRIU_SRC/criu/include" \
	-iquote "$CRIU_SRC/include" \
	-iquote "$CRIU_SRC" \
	"$CC_SRC/cachecli.c" "$CC_SRC/scm_shim.c" "$CLIENT" \
	-o "$BIN"

echo ">> running the gated Go integration test"
cd "$PKG_DIR"
MEMFDCACHE_CACHECLI="$BIN" go test -run TestClientIntegration -v .
