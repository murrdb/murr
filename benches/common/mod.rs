#![allow(dead_code)]

pub mod data;
pub mod dataset;
pub mod read_bench;

#[cfg(target_os = "linux")]
pub mod profiler;

use criterion::Criterion;

/// Criterion configured with the pprof profiler on Linux. pprof is Unix-only
/// and does not build on Windows, so elsewhere we return a plain Criterion
/// (profiling is a Linux-only concern here).
pub fn criterion() -> Criterion {
    let criterion = Criterion::default();
    #[cfg(target_os = "linux")]
    let criterion = criterion.with_profiler(profiler::PProfProfiler::new());
    criterion
}
