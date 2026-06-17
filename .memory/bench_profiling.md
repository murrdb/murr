# Bench profiling: perf -> pprof-rs

`run_bench.sh` used to profile benches under `perf record` and post-process with
`perf report`/`perf annotate`. Replaced with in-process sampling: pprof-rs
(`prost-codec` feature) hooked into Criterion's `--profile-time` mode via a
hand-written `criterion::profiler::Profiler` impl in `benches/common/profiler.rs`,
post-processed with `go tool pprof` (`-top`, plus `-peek` per hot symbol for
caller/callee edges) into an LLM-readable markdown report under `.pprof/`.

Decisions and reasoning:

* **Hand-written Profiler impl instead of pprof-rs's `criterion` feature**: that
  feature pins criterion 0.5 while we're on 0.8, so its trait impl targets the
  wrong crate version. Our ~40-line wrapper decouples the two versions.
* **`-peek` per hot symbol instead of `-tree` for the caller graph**: `-tree`
  with a small nodecount keeps only the top-flat nodes, which are usually not
  adjacent in the call graph, so it renders zero edges. `-peek` on each hot
  symbol (regex-escaped, anchored) always shows its callers and callees.
* **objdump instead of `pprof -disasm` for the top-N disassembly section**:
  `Report::pprof()` emits no Mapping table and no addresses (only symbolized
  function names), so address-based pprof features can never work on these
  profiles. `-top`/`-peek` work standalone without the binary; disassembly falls
  back to `objdump -dC` matched by demangled name — unannotated (no
  per-instruction sample %), the one capability lost vs `perf annotate`.
  Names are normalized on both sides (C++ params and ident-preceded generic
  args stripped) because pprof reports generics as `Encoder<T>` while objdump
  shows concrete instantiations like `Encoder<f32>`. Hot leaf functions are
  often fully inlined (e.g. rocksdb `GetVarint32Ptr`) and get a "fully inlined"
  note instead of a code block — their codegen lives in the callers shown by
  the peek section.
* **ProfilerGuard blocklist (`libc`, `libgcc`, `pthread`, `vdso`)**: benches run
  jemalloc as global allocator; unwinding through allocator/libc frames inside
  the SIGPROF handler can crash or deadlock (pprof-rs README recommendation).
* **Removed perf plumbing**: `-Cforce-frame-pointers` RUSTFLAGS, the gimli
  addr2line requirement, and the perf ring-buffer sizing that worked around
  io_uring RLIMIT_MEMLOCK conflicts are all obsolete — sampling is in-process.
  `[profile.bench]` debug settings are retained because objdump and pprof
  file/line info still need them.
* **Known limitation**: pprof-rs samples CPU time via ITIMER_PROF, so off-CPU
  time (io_uring waits, blocking syscalls) is invisible in the profile, unlike
  perf. Fine for hotspot work; use perf manually if wall-clock attribution is
  ever needed.
* **Linux-only gating**: pprof depends on Unix-only `nix`/`libc` and fails to
  build on Windows MSVC (where `cargo test` compiles the bench targets and their
  dev-deps), so the `pprof` dev-dependency and the `profiler` module are both
  gated to `cfg(target_os = "linux")`. Bench files build their Criterion via
  `common::criterion()`, which attaches `PProfProfiler` on Linux and returns a
  plain `Criterion` elsewhere — mirroring the jemalloc Linux gating in
  `allocator.md`.
