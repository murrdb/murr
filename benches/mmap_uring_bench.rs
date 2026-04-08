use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use io_uring::{IoUring, opcode, types};
use memmap2::MmapOptions;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;

const FILE_PATH: &str = "/home/shutty/tmp/testdata.bin";
const FILE_SIZE: usize = 128 * 1024 * 1024 * 1024;
const PAGE_SIZE: usize = 4096;
const NUM_OFFSETS: usize = 1024;
const QUEUE_DEPTHS: &[u32] = &[1, 2, 4, 8, 16, 32, 64, 128, 256];

trait RandomReader {
    fn read(&mut self, offsets: &[usize]) -> Vec<u32>;
}

// -- MmapReader --

struct MmapReader {
    mmap: memmap2::Mmap,
}

impl MmapReader {
    fn setup() -> Self {
        let file = File::open(FILE_PATH).expect("failed to open test file");
        let mmap = unsafe { MmapOptions::new().map(&file).expect("failed to mmap file") };
        mmap.advise(memmap2::Advice::Random).expect("madvise failed");
        Self { mmap }
    }
}

impl RandomReader for MmapReader {
    fn read(&mut self, offsets: &[usize]) -> Vec<u32> {
        let base = self.mmap.as_ptr();
        let mut result = Vec::with_capacity(offsets.len());
        for &off in offsets {
            let val = unsafe { *(base.add(off) as *const u32) };
            result.push(val);
        }
        result
    }
}

// -- IoUringReader --

/// Page-aligned buffer for O_DIRECT I/O.
struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
}

impl AlignedBuffer {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, PAGE_SIZE).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "aligned alloc failed");
        Self { ptr, len }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len, PAGE_SIZE).unwrap();
        unsafe { std::alloc::dealloc(self.ptr, layout) };
    }
}

struct IoUringReader {
    queue_depth: u32,
    file: File,
    ring: IoUring,
    buffers: Vec<AlignedBuffer>,
    reg_files: bool,
    reg_buffers: bool,
}

impl IoUringReader {
    fn setup(queue_depth: u32, reg_files: bool, reg_buffers: bool) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(FILE_PATH)
            .expect("failed to open test file with O_DIRECT");
        let ring = IoUring::new(queue_depth).expect("failed to create io_uring");
        let mut buffers: Vec<AlignedBuffer> = (0..queue_depth as usize)
            .map(|_| AlignedBuffer::new(PAGE_SIZE))
            .collect();

        if reg_files {
            ring.submitter()
                .register_files(&[file.as_raw_fd()])
                .expect("failed to register files");
        }

        if reg_buffers {
            let iovecs: Vec<libc::iovec> = buffers
                .iter_mut()
                .map(|buf| libc::iovec {
                    iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                    iov_len: PAGE_SIZE,
                })
                .collect();
            unsafe {
                ring.submitter()
                    .register_buffers(&iovecs)
                    .expect("failed to register buffers");
            }
        }

        Self {
            queue_depth,
            file,
            ring,
            buffers,
            reg_files,
            reg_buffers,
        }
    }

}

impl RandomReader for IoUringReader {
    fn read(&mut self, offsets: &[usize]) -> Vec<u32> {
        let fd = self.file.as_raw_fd();
        let qd = self.queue_depth as usize;
        let mut result = Vec::with_capacity(offsets.len());

        let mut i = 0;
        while i < offsets.len() {
            let batch_size = (offsets.len() - i).min(qd);

            // Submit batch
            unsafe {
                let mut sq = self.ring.submission_shared();
                for j in 0..batch_size {
                    let offset = offsets[i + j] as u64;
                    let buf = self.buffers[j].as_mut_ptr();
                    let entry = match (self.reg_files, self.reg_buffers) {
                        (false, false) => opcode::Read::new(types::Fd(fd), buf, PAGE_SIZE as u32)
                            .offset(offset)
                            .build(),
                        (true, false) => {
                            opcode::Read::new(types::Fixed(0), buf, PAGE_SIZE as u32)
                                .offset(offset)
                                .build()
                        }
                        (false, true) => opcode::ReadFixed::new(
                            types::Fd(fd),
                            buf,
                            PAGE_SIZE as u32,
                            j as u16,
                        )
                        .offset(offset)
                        .build(),
                        (true, true) => opcode::ReadFixed::new(
                            types::Fixed(0),
                            buf,
                            PAGE_SIZE as u32,
                            j as u16,
                        )
                        .offset(offset)
                        .build(),
                    }
                    .user_data(j as u64);
                    sq.push(&entry).expect("SQ full");
                }
            }

            self.ring
                .submit_and_wait(batch_size)
                .expect("submit_and_wait failed");

            // Reap completions
            let mut completed = 0;
            while completed < batch_size {
                for cqe in unsafe { self.ring.completion_shared() } {
                    debug_assert!(cqe.result() >= 0, "io_uring read failed: {}", cqe.result());
                    completed += 1;
                }
            }

            // Extract first u32 from each buffer
            for j in 0..batch_size {
                let val = unsafe { *(self.buffers[j].as_ptr() as *const u32) };
                result.push(val);
            }

            i += batch_size;
        }

        result
    }
}

// -- Offset generation --

fn generate_offsets(rng: &mut SmallRng, count: usize) -> Vec<usize> {
    let max_page = (FILE_SIZE - PAGE_SIZE) / PAGE_SIZE;
    (0..count)
        .map(|_| (rng.random::<u64>() as usize % max_page) * PAGE_SIZE)
        .collect()
}

// -- Benchmark --

fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");
    group.throughput(Throughput::Elements(NUM_OFFSETS as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    // mmap benchmark
/*    let mut mmap_reader = MmapReader::setup();
    let mut rng = SmallRng::seed_from_u64(42);
    group.bench_function("mmap", |b| {
        b.iter(|| {
            let offsets = generate_offsets(&mut rng, NUM_OFFSETS);
            mmap_reader.read(&offsets)
        })
    });*/

    // io_uring benchmarks per (queue_depth, reg_files, reg_buffers)
    let flag_combos: &[(bool, bool)] = &[(false, false), (true, false), (false, true), (true, true)];
    for &qd in QUEUE_DEPTHS {
        for &(reg_files, reg_buffers) in flag_combos {
            let label = format!(
                "qd={}/reg_files={}/reg_bufs={}",
                qd, reg_files, reg_buffers
            );
            let mut uring_reader = IoUringReader::setup(qd, reg_files, reg_buffers);
            let mut rng = SmallRng::seed_from_u64(42);
            group.bench_function(BenchmarkId::new("io_uring", &label), |b| {
                b.iter(|| {
                    let offsets = generate_offsets(&mut rng, NUM_OFFSETS);
                    uring_reader.read(&offsets)
                })
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_random_reads);
criterion_main!(benches);
