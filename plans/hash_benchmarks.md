# HashMap Bench: Performance Experiments Report

Benchmark setup: 10M rows, 10 Float32 columns, 1000 random key lookups per call.
All measurements on the `hashmap_bench` criterion benchmark.

## Baseline

**30ms** — `Vec<f32>` columns, no validity bitmap, `HashMap<String, usize>` with SipHash, `Column::get()` returns `Vec<f32>`, caller builds `Float32Array`.

Profile: SipHash dominates (~26%), values gather via `.collect()` is fast (~10%) due to `extend_trusted` optimization.

---

## Experiment 1: Add validity bitmap with bitvec + dyn Array return

**Idea:** Add `BitVec<u8, Lsb0>` validity field, change `Column::get()` to return `Arc<dyn Array>` directly. Gather validity bits into `Vec<bool>`, convert to `NullBuffer::from(Vec<bool>)`.

**Result: 70ms** (+133%)

**Why:** Three costs compounded:
- `bitvec` random access has heavy abstraction layers (~6-7 function calls per bit): `Index` -> `BitSlice` -> `BitSliceIndex::get` -> bounds check via `BitSpan::len` -> `get_unchecked` -> `BitPtr::add` -> `BitIdx::offset`. Showed up as **4.3%** in the profile.
- `NullBuffer::from(Vec<bool>)` repacks each bool back into bits via `set_bit_raw` — **3.1%**.
- Additional `Vec<bool>` allocation + `Arc` wrapping increased malloc/free overhead to **~7%**.

**Lesson:** bitvec is not suitable for random-access hot paths. The `Vec<bool>` intermediate is wasteful — unpacking bits to bools only to repack them.

---

## Experiment 2: Verify dyn Array overhead

**Idea:** Comment out validity bitmap code but keep `Arc<dyn Array>` return type.

**Result: 50ms** (+67%)

**Analysis:** The 30ms -> 50ms gap was attributed to Arrow `Float32Array::new()` + `ScalarBuffer` construction + `Arc` allocation/drop per column. However, later experiment (nullable=false with `None` nulls) returned to **30ms**, proving the `Arc<dyn Array>` wrapping itself is essentially free. The 50ms measurement likely still had validity-related code in the path.

**Lesson:** Arrow array construction overhead (`Float32Array::new`, `Arc::new`) is negligible for this workload.

---

## Experiment 3: Manual Vec<u8> bitmap, two separate loops

**Idea:** Replace bitvec with raw `Vec<u8>` bitmap. Store validity as `vec![0xFF; (n+7)/8]`. Use manual bit ops (`>> 3`, `& 7`, shift+mask) in a separate loop from values gather. Wrap result via zero-copy `Buffer::from_vec` -> `BooleanBuffer::new` -> `NullBuffer::new`.

**Implementation:**
```rust
let values: Vec<f32> = offsets.iter().map(|&i| self.data[i]).collect();
let mut validity_bytes = vec![0u8; (len + 7) / 8];
for (out_idx, &src_idx) in offsets.iter().enumerate() {
    let src_bit = (self.validity[src_idx >> 3] >> (src_idx & 7)) & 1;
    validity_bytes[out_idx >> 3] |= src_bit << (out_idx & 7);
}
```

**Result: 54ms** (+80%)

**Profile:** Bitmap loop self-time at **17.7%** (bit ops not vectorized), values collect at **6.5%** (good — `extend_trusted` works). The two-loop approach preserved compiler optimization for the values path.

**Lesson:** Manual bit ops are much faster than bitvec, but the bitmap gather loop is still expensive due to random cache misses into a 1.25MB bitmap.

---

## Experiment 4: Single fused loop (values + validity in one pass)

**Idea:** Combine values gather and validity gather into a single `for` loop with `values.push()`.

**Result: 54ms** (same wall clock, but worse profile)

**Profile:** `Vec::push` at **8.4%** — unlike `.collect()` which uses `extend_trusted` (knows exact size, single memcpy-style fill), `push()` checks capacity on every call. The compiler lost ability to vectorize. Float32Column::get total went from 29.5% -> **36%**.

**Lesson:** Never replace `.collect()` with a manual `push()` loop. The compiler optimizes `.collect()` on `ExactSizeIterator` via `extend_trusted`, which is significantly faster.

---

## Experiment 5: Sort indices before gathering

**Idea:** Sort offsets to convert random memory access into sequential access, enabling CPU prefetcher. Build a permutation array, gather in sorted order, write back to original positions.

**Implementation:**
```rust
let mut perm: Vec<usize> = (0..len).collect();
perm.sort_unstable_by_key(|&i| offsets[i]);
for &p in &perm {
    let src_idx = offsets[p];
    values[p] = self.data[src_idx];
    // bitmap gather...
}
```

**Result: worse** (profile showed 46% in Float32Column::get)

**Profile:** Sort itself consumed **26.2%** — `PartialOrd::lt` (8.8%), `copy_nonoverlapping` (9.5%), `copy` (7.4%), quicksort partitioning (6.2%). The gather body dropped from 17.7% -> 11.6% (sequential access did help), but sort overhead far exceeded savings.

**Lesson:** Sorting 1000 elements is O(n log n) ~ 10K comparisons+swaps, done 10 times (per column). Cache miss savings on 1000 random reads don't justify the sort cost. Would only help at much larger batch sizes (10K+) or if sort is amortized across columns.

---

## Experiment 6: Parallel column reads with rayon

**Idea:** Use `rayon::par_iter()` to gather from all 10 columns in parallel, hiding cache miss latency by overlapping memory stalls across threads.

**Result: worse** (rayon overhead dominated)

**Profile:** Rayon coordination overhead at **40.9%**:
- `atomic_compare_exchange` 4.3%
- `sched_yield` 5.8%
- crossbeam epoch GC 11.6%
- work stealing 5.2%
- kernel scheduling 5.6%

Actual column work was cheap per-thread, but each task (~5us) was far too small for rayon's work-stealing overhead.

**Lesson:** Rayon work-stealing is not suitable for microsecond-scale tasks. Need millisecond+ work per task for parallelism to pay off. At 1000 keys x 10 columns, serial is better.

---

## Experiment 7: Arrow-native BooleanBuffer::from_iter + NullBuffer storage

**Idea:** Store validity as Arrow's `NullBuffer` directly. Use `NullBuffer::is_valid(i)` for reads and `BooleanBuffer::from_iter()` for output construction. Eliminate all manual bit ops.

**Implementation:**
```rust
struct Float32Column {
    data: Vec<f32>,
    validity: NullBuffer,  // Arrow-native
}
// ...
let nulls = BooleanBuffer::from_iter(offsets.iter().map(|&i| self.validity.is_valid(i)));
```

**Result: 73ms** (+143%)

**Profile:** `usize::div_ceil` at **10.8%** — called by `BooleanBufferBuilder::advance()` on every bit append to check if another byte is needed. Plus `get_bit_raw` (5.1%), `set_bit_raw` (4.1%), `advance` (3.9%). Total validity overhead: **24%**.

**Lesson:** Arrow's `BooleanBufferBuilder` has per-element overhead (integer division + bounds check + function call) that makes it slower than manual bit ops for hot-path gather. Arrow builder APIs are designed for convenience, not tight loops.

---

## Experiment 8: Replace SipHash with AHash

**Idea:** Switch from `std::collections::HashMap` (SipHash, cryptographic) to `ahash::AHashMap` (AES-NI based, non-cryptographic).

**Result: 42ms** (-22% from 54ms two-loop baseline)

**Profile:** Hashing dropped from **26.4%** -> **~1.6%**. But string comparison (`SlicePartialEq::equal`) emerged as new bottleneck at **19.3%** — previously hidden behind SipHash cost. Every successful HashMap lookup compares the full key string via `memcmp`.

**Lesson:** AHash nearly eliminates hashing overhead. For string-keyed maps, key comparison becomes the bottleneck once hashing is fast. Also considered FxHash (multiply+rotate, even simpler) but it has worse distribution for strings, leading to more collisions and more comparisons.

---

## Experiment 9: Unsafe get_unchecked to eliminate bounds checks

**Idea:** Replace `self.data[i]`, `self.validity[i >> 3]`, and `validity_bytes[i >> 3]` with unsafe `get_unchecked` variants. Safe because offsets come from our own HashMap (always valid indices).

**Implementation:**
```rust
let values: Vec<f32> = offsets.iter().map(|&i| unsafe {
    *self.data.get_unchecked(i)
}).collect();
// ...
unsafe {
    let src_bit = (*self.validity.get_unchecked(src_idx >> 3) >> (src_idx & 7)) & 1;
    *validity_bytes.get_unchecked_mut(out_idx >> 3) |= src_bit << (out_idx & 7);
}
```

**Result: 39ms** (-7% from 42ms ahash)

**Profile:** `SliceIndex::index` dropped from 5.7% -> **0.0%**. Two remaining bottlenecks: bitmap gather loop (26.9%) and string comparison (23.2%).

---

## Experiment 10: Table-level sort (amortized across columns)

**Idea:** Move the sort from per-column (Experiment 5) to `SimpleTable::get()` — sort offsets once, pass `sorted_offsets` + `perm` (original output positions) to all 10 columns. Columns read sequentially from source, scatter-write to output positions. Amortizes O(n log n) sort across all columns.

**Implementation:**
```rust
// In SimpleTable::get() — sort once
let mut perm: Vec<usize> = (0..offsets.len()).collect();
perm.sort_unstable_by_key(|&i| offsets[i]);
let sorted_offsets: Vec<usize> = perm.iter().map(|&i| offsets[i]).collect();

// In Column::get_sorted() — sequential reads, scattered writes
for (&src_idx, &out_idx) in sorted_offsets.iter().zip(perm.iter()) {
    *values.get_unchecked_mut(out_idx) = *self.data.get_unchecked(src_idx);
    let src_bit = (*self.validity.get_unchecked(src_idx >> 3) >> (src_idx & 7)) & 1;
    *validity_bytes.get_unchecked_mut(out_idx >> 3) |= src_bit << (out_idx & 7);
}
```

**Result: 42ms** (+8% vs unsorted 39ms)

**Profile:** Sort cost at **13.3%** (down from 26.2% when per-column). Column self-time at **31.1%** (up from 26.9% unsorted). The fused loop with `vec![0.0f32; len]` + scatter writes lost the `.collect()`/`extend_trusted` optimization. `Zip` iterator added **4.9%** overhead.

**Why it didn't help:** At 1000 keys, the output buffer is only 4KB (fits in L1), so random writes were already free in the unsorted version. The sorted version traded sequential reads (prefetcher benefit) for: sort overhead (13.3%), loss of `extend_trusted` (fused loop), and `Zip` iterator overhead (4.9%). Net negative.

**Lesson:** Sort-based gather only helps when output is large enough to cause write cache misses, OR when batch sizes are large enough that sort cost is dwarfed by cache miss savings. At n=1000 with 4KB output, the unsorted two-loop approach wins. Even amortizing sort across 10 columns wasn't enough to overcome the overhead.

---

## Summary

| Experiment | Latency | vs Baseline | Key Finding |
|---|---|---|---|
| Baseline (no nulls) | 30ms | — | SipHash dominates |
| bitvec + Vec\<bool\> | 70ms | +133% | bitvec abstraction layers are expensive |
| Manual Vec\<u8\> two loops | 54ms | +80% | Manual bit ops >> bitvec, keep .collect() |
| Fused single loop | 54ms | +80% | .push() kills .collect() optimization |
| Sorted indices | worse | — | Sort cost > cache miss savings at n=1000 |
| Parallel rayon | worse | — | Rayon overhead >> microsecond tasks |
| Arrow-native from_iter | 73ms | +143% | BooleanBufferBuilder has per-element div_ceil |
| AHash | 42ms | +40% | Hashing eliminated, str compare exposed |
| AHash + unsafe | 39ms | +30% | Bounds checks removed |
| Table-level sort | 42ms | +40% | Sort amortized, but fused loop + Zip overhead > cache savings |

## Remaining Bottlenecks (at 39ms)

1. **Bitmap gather loop (26.9%)** — Random reads into 1.25MB bitmap. Cache-miss-bound.
   - Mitigation: null_count fast path — if column has zero nulls, skip bitmap entirely and pass `None`.

2. **String key comparison (23.2%)** — `memcmp` on every HashMap hit.
   - Mitigation: Key interning at API boundary — convert `String` -> `u32` once per request, use integer keys internally. Or use fixed-size `[u8; N]` key representation for SIMD comparison.

## Key Lessons

- `.collect()` with `ExactSizeIterator` is significantly faster than manual `.push()` loops due to `extend_trusted`.
- bitvec and Arrow's BooleanBufferBuilder have too much per-element overhead for hot-path random access. Manual bit ops win.
- Rayon work-stealing requires millisecond+ task granularity to amortize coordination costs.
- Sorting indices only pays off when batch sizes are large enough (10K+) to amortize O(n log n) cost. Even amortizing across 10 columns wasn't enough at n=1000.
- Fused gather loops (values + validity) lose `.collect()`/`extend_trusted` optimization and introduce `Zip` iterator overhead.
- Once hashing is fast (ahash), string comparison dominates HashMap lookups.
- `get_unchecked` gives small but real gains when bounds are guaranteed by construction.
- For dense data, the single biggest optimization is skipping the validity bitmap entirely (null_count fast path).
