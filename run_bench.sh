#!/usr/bin/env bash
# Profile a Criterion bench under `perf` and emit an LLM-friendly hotspot report.
# Usage: ./run_bench.sh <bench_name>
# Env: PROFILE_TIME=<secs>  DELAY_MS=<ms>  FREQ=<hz>  ADDR2LINE=<path>
set -euo pipefail

BENCH="${1:?Usage: $0 <bench_name>}"
PROFILE_TIME="${PROFILE_TIME:-30}"
DELAY_MS="${DELAY_MS:-70000}"
FREQ="${FREQ:-300}"

PERF_DIR=".perf"
mkdir -p "$PERF_DIR"
PERF_DATA="$PERF_DIR/${BENCH}.perf.data"
PERF_REPORT="$PERF_DIR/${BENCH}.report.txt"
OUTPUT="$PERF_DIR/${BENCH}_hotspots.md"

command -v perf  >/dev/null || { echo "perf not found on PATH" >&2; exit 1; }
command -v cargo >/dev/null || { echo "cargo not found on PATH" >&2; exit 1; }

# Require gimli-rs addr2line; the GNU one has the regression from
# https://github.com/flamegraph-rs/flamegraph/issues/294.
ADDR2LINE="${ADDR2LINE:-$HOME/.cargo/bin/addr2line}"
if [ ! -x "$ADDR2LINE" ]; then
    echo "gimli-rs addr2line not found at $ADDR2LINE — install with:" >&2
    echo "    cargo install addr2line --features=bin"                  >&2
    exit 1
fi

echo "Building bench '$BENCH' (with frame pointers)..." >&2
# -Cforce-frame-pointers=yes makes the DWARF unwinder more reliable on deep
# tokio stacks: frames have a uniform shape so .eh_frame rows always apply.
# This isn't a stable Cargo.toml profile field, so we pass it via RUSTFLAGS;
# the bench profile already has debug=1, lto=off, codegen-units=1.
RUSTFLAGS="${RUSTFLAGS:-} -Cforce-frame-pointers=yes" \
    cargo bench --no-run --bench "$BENCH" >&2

BIN=$(ls -t target/release/deps/${BENCH}-* 2>/dev/null \
        | grep -vE '\.(d|o|rmeta)$' \
        | head -n1 || true)
[ -n "${BIN:-}" ] && [ -x "$BIN" ] \
    || { echo "Failed to locate bench binary for '$BENCH' in target/release/deps/" >&2; exit 1; }
echo "Binary: $BIN" >&2

delay_arg=()
[ "$DELAY_MS" -gt 0 ] && delay_arg=(-D "$DELAY_MS")
# dwarf,32768: 32 KiB user-stack snapshot — enough for tokio+libc chains
#   without exhausting the per-CPU ring buffer like 65528 did.
# -m 8: 32 KiB perf ring per CPU. Perf's default (~512 KiB) eats most of the
#   ~8 MiB RLIMIT_MEMLOCK budget, which makes io_uring benches (read_bench's
#   `uring/*`) fail with ENOMEM at io_uring_setup. 8 pages keeps perf's
#   footprint small enough that io_uring can still register its rings.

echo "perf record -F $FREQ --call-graph dwarf,32768 -m 32 ${delay_arg[*]:-} ($PROFILE_TIME s of bench iters)..." >&2
rm -f "$PERF_DATA"
perf record -F "$FREQ" --call-graph dwarf,32768 -m 32 "${delay_arg[@]}" \
    -o "$PERF_DATA" -- "$BIN" --bench --profile-time "$PROFILE_TIME" >&2

# Surface lost-sample count up-front so a degraded run is obvious.
perf report -i "$PERF_DATA" --header-only 2>/dev/null | grep -E 'Total Lost Samples' >&2 || true

perf report -i "$PERF_DATA" --stdio --no-children \
    --percent-limit 1 -F overhead,symbol --addr2line="$ADDR2LINE" > "$PERF_REPORT"

# Caller-direction tree (who reaches each hot symbol).
TREE_REPORT="$PERF_DIR/${BENCH}.calltree.txt"
perf report -i "$PERF_DATA" --stdio \
    -g 'graph,0.5,caller,function' \
    --percent-limit 1 -F overhead,symbol \
    --addr2line="$ADDR2LINE" > "$TREE_REPORT"

# Parse "  12.34%  [.] symbol" lines (also handles [k] kernel symbols).
mapfile -t HOT < <(awk '/^[[:space:]]*[0-9]+\.[0-9]+%/ {
        pct = $1; gsub("%", "", pct);
        idx = index($0, "[.]"); if (!idx) idx = index($0, "[k]");
        if (idx) {
            sym = substr($0, idx + 4);
            sub(/^[[:space:]]+/, "", sym);
            print pct "\t" sym;
        }
    }' "$PERF_REPORT")

{
    echo "# perf hotspots — $BENCH"
    echo
    echo "- Binary: \`$BIN\`"
    echo "- Recorded: $(date -Iseconds)"
    echo "- Sample rate: ${FREQ} Hz, profile time: ${PROFILE_TIME}s, start delay: ${DELAY_MS}ms"
    echo
    echo "## Top functions (>= 1% self-time)"
    echo
    echo '| % | symbol |'
    echo '|---|--------|'
    for line in "${HOT[@]}"; do
        pct="${line%%$'\t'*}"; sym="${line#*$'\t'}"
        printf '| %s%% | `%s` |\n' "$pct" "$sym"
    done
    echo
    echo "## Call trees (caller direction; nodes >= 0.5%)"
    echo
    echo "Read each block top-down: the leftmost percentage is the hot symbol's"
    echo "share; indented entries are the callers reaching it."
    echo
    echo '```'
    # Drop perf comments, blank lines, and 0.00% inlined-symbol sections
    # (perf emits those even with --percent-limit; their trees are duplicates
    #  of what's already shown under the parent hot symbol).
    awk '
        /^#/ || NF == 0 { next }
        /^[[:space:]]+[0-9]+\.[0-9]+%[[:space:]]+\[/ { skip = ($1 == "0.00%") }
        !skip
    ' "$TREE_REPORT"
    echo '```'
    echo
    echo "## Annotated disassembly"
    echo
    for line in "${HOT[@]}"; do
        pct="${line%%$'\t'*}"; sym="${line#*$'\t'}"
        echo "### ${pct}% — \`${sym}\`"
        echo '```asm'
        perf annotate -i "$PERF_DATA" --stdio --no-source --symbol="$sym" \
                      --addr2line="$ADDR2LINE" 2>&1 \
            | sed -n '/^Percent\|^[[:space:]]*:/,$p' \
            || echo "(annotate failed for $sym)"
        echo '```'
        echo
    done
} > "$OUTPUT"

echo "$OUTPUT"
