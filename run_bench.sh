#!/usr/bin/env bash
# Profile a Criterion bench with pprof-rs and emit an LLM-friendly hotspot report.
# Usage: ./run_bench.sh <bench_name>
# Env: PROFILE_TIME=<secs per benchmark>  PEEK_TOP=<n>  DISASM_TOP=<n, 0 to skip>  PPROF_FREQ=<hz>
set -euo pipefail

BENCH="${1:?Usage: $0 <bench_name>}"
PROFILE_TIME="${PROFILE_TIME:-30}"
PEEK_TOP="${PEEK_TOP:-8}"
DISASM_TOP="${DISASM_TOP:-3}"
# Resolve via cargo metadata: respects .cargo/config.toml build.target-dir.
TARGET_DIR=$(cargo metadata --format-version 1 --no-deps \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
CRITERION_DIR="$TARGET_DIR/criterion"
OUT=".pprof/${BENCH}_hotspots.md"
mkdir -p .pprof

# Top-N function names from a pprof -top listing (drop the 5 numeric columns).
top_syms() {
    go tool pprof -top -nodecount="$1" "${@:2}" 2>/dev/null \
        | sed -n '/flat%/,$p' | tail -n +2 | sed -E 's/^ *([^ ]+ +){5}//'
}

regex_escape() { printf '%s' "$1" | sed -E 's/[][^$.*+?(){}|\\]/\\&/g'; }

STAMP=$(mktemp); trap 'rm -f "$STAMP"' EXIT
cargo bench --bench "$BENCH" -- --profile-time "$PROFILE_TIME" >&2

mapfile -t PROFILES < <(find "$CRITERION_DIR" -path '*/profile/profile.pb' -newer "$STAMP" | sort)
[ "${#PROFILES[@]}" -gt 0 ] || { echo "no profile.pb produced — profiler hooked up?" >&2; exit 1; }

{
    echo "# pprof hotspots — $BENCH ($(date -Iseconds), ${PROFILE_TIME}s/benchmark, ${PPROF_FREQ:-500} Hz)"
    for PB in "${PROFILES[@]}"; do
        ID=${PB#"$CRITERION_DIR/"}; ID=${ID%/profile/profile.pb}
        echo; echo "## $ID"
        echo; echo "### Top functions (self time)"
        echo '```'; go tool pprof -top -nodecount=25 "$PB" 2>/dev/null; echo '```'
        echo; echo "### Callers/callees of the top $PEEK_TOP (callers above the | line, callees below)"
        echo '```'
        top_syms "$PEEK_TOP" "$PB" | while IFS= read -r SYM; do
            go tool pprof -peek "^$(regex_escape "$SYM")\$" "$PB" 2>/dev/null \
                | sed -n '/context/,$p' | tail -n +3
        done
        echo '```'
    done
    if [ "$DISASM_TOP" -gt 0 ]; then
        BIN=$(cargo bench --bench "$BENCH" --no-run --message-format=json 2>/dev/null \
              | grep -o '"executable":"[^"]*"' | cut -d'"' -f4 | grep "/${BENCH}-" | head -n1)
        ASM=$(mktemp); objdump -dC --no-show-raw-insn "$BIN" > "$ASM"
        echo; echo "## Disassembly of top $DISASM_TOP functions (objdump; unannotated)"
        # pprof-rs profiles carry no addresses, so `pprof -disasm` can't work; match
        # objdump blocks by demangled name instead, with C++ params and generic args
        # stripped on both sides (pprof reports `Encoder<T>`, objdump `Encoder<f32>`).
        top_syms "$DISASM_TOP" "${PROFILES[@]}" | while IFS= read -r SYM; do
            echo; echo "### \`$SYM\`"; echo '```asm'
            awk -v sym="$SYM" '
                # strip C++ params and generic args (ident-preceded <...>, so the
                # outer bracket of `<X as Trait>::f` survives and keeps X distinct)
                function norm(s) {
                    sub(/\(.*/, "", s)
                    while (match(s, /[A-Za-z0-9_]<[^<>]*>/)) s = substr(s, 1, RSTART) substr(s, RSTART + RLENGTH)
                    return s
                }
                BEGIN { target = norm(sym) }
                /^[0-9a-f]+ </ {
                    name = $0; sub(/^[0-9a-f]+ </, "", name); sub(/>:$/, "", name)
                    p = (norm(name) == target)
                }
                p && NF == 0 { p = 0; next }
                p
            ' "$ASM" | grep . || echo "(no standalone code block — fully inlined into callers)"
            echo '```'
        done
        rm -f "$ASM"
    fi
} > "$OUT"

echo "$OUT"
