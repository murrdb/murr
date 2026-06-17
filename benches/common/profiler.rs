use std::fs::File;
use std::io::Write;
use std::path::Path;

use criterion::profiler::Profiler;
use pprof::protos::Message;
use pprof::{ProfilerGuard, ProfilerGuardBuilder};

/// Hooks pprof-rs into Criterion's `--profile-time` mode, writing a
/// pprof-format `profile.pb` into each benchmark's `profile/` dir.
pub struct PProfProfiler {
    frequency: i32,
    guard: Option<ProfilerGuard<'static>>,
}

impl PProfProfiler {
    pub fn new() -> Self {
        let frequency = std::env::var("PPROF_FREQ")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);
        Self { frequency, guard: None }
    }
}

impl Profiler for PProfProfiler {
    fn start_profiling(&mut self, _id: &str, _dir: &Path) {
        // Blocklist per pprof-rs README: unwinding through these in the
        // SIGPROF handler can crash/deadlock (we run jemalloc).
        let guard = ProfilerGuardBuilder::default()
            .frequency(self.frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .expect("failed to start pprof profiler");
        self.guard = Some(guard);
    }

    fn stop_profiling(&mut self, _id: &str, dir: &Path) {
        let report = self
            .guard
            .take()
            .expect("profiler was not started")
            .report()
            .build()
            .expect("pprof report failed");
        let profile = report.pprof().expect("pprof encoding failed");
        let mut buf = Vec::new();
        profile.encode(&mut buf).expect("protobuf encode failed");
        std::fs::create_dir_all(dir).expect("create profile dir");
        File::create(dir.join("profile.pb"))
            .and_then(|mut f| f.write_all(&buf))
            .expect("write profile.pb");
    }
}
