# 0.1.1 candidate: Clojure performance

Status: **candidate, not published or tagged**. Branch: `0.1.1`, based on released
tag `0.1.0` (`a50c54014d677219254da6819e79b5e2731145ab`). Build version: `0.1.1`.
Measurements below compare that actual release with candidate production commit
`eef32bd410fb0322f30161db2f36b8a2b1a5258b`; subsequent release preparation changes
only benchmarks/tests, documentation and the build version, not production source.

## Release notes

- **Seekable CSR64 structural pages.** Compiled clean/patch runs replace repeated
  scans of the skipped row prefix. Arbitrary forward/backward numeric cursors
  binary-search visible run ends; surviving base spans retain mapped bulk copying.
  The run index scales with effective modifications, not untouched row degree.
- **Lower construction allocation.** Fixed-width payload builders derive block
  count from payload length instead of maintaining a redundant integer buffer.
  Frozen CSR indexes reuse snapshot-owned ordered column IDs rather than copying
  them into a second array.
- **Shared query traversal.** Copy/view traversal shares implementation; primitive
  payload IDs and direct generated dataset-method calls reduce avoidable overhead.
- **Consistent CSRSource validation.** Heap and mapped sources share range,
  cursor, destination-capacity and point-copy checks.
- **Overlay ordering and regression coverage.** Ordered replacement handling,
  snapshot ownership, generated/protocol parity, randomized exact-output models,
  arbitrary cursors and bounded skipped-prefix work are covered by tests.
- **Reproducible benchmarks.** Dedicated core and overlay suites validate results
  against exact independent expectations and report per-operation time/allocation.

Documented query signatures, numeric cursor semantics and CSR64 artifact format
are unchanged. Defensive copying of ledger blocks is retained. Internal primitive
arrays remain read-only snapshot storage; code mutating them was never supported.
The mapped base must remain open while its overlay is read. Recompile generated
`defsparse` code normally when upgrading; do not hot-redefine existing JVM types.

No Lean/native backend, Arrow/NPY/tech.ml prototype, experimental dependency or
historical review/zine is included in this candidate. This release does not add a
new cold-cache, concurrent-reader or production-tail latency guarantee. The older
README/design claims about column-order validation, empty pages and falsey ledger
records were stale: those fixes already shipped in `0.1.0`, not in this candidate.

### Cherry-picked production changes

| Source on `simplify` | Candidate commit | Scope |
|---|---|---|
| `8952895` | `7dd8707` | Core construction allocation and shared query traversal |
| `ee2863a` | `08d4c66` | Shared CSRSource validation |
| `bd05944` | `3c70552` | Ordered overlay merge/replacements |
| `ace8eea` | `eef32bd` | Seekable structural rows, tests and overlay benchmark |

## Operation comparison

**Times are medians of three fresh-JVM fork medians.** Speedup is baseline time /
candidate time, computed before rounding. A ratio below 1 is a regression. Heap
figures are medians of per-fork median allocated bytes/op, not retained heap or
mapped/native storage. All blocks contain **8 doubles**. These fixtures differ
from the older 295-double / 590 MiB measurements elsewhere in the project.

### Core construction and generated queries

Fixture: 256 rows × 32 stored columns = 8,192 entries; both CSR and CSC indexes.
Input tuples/arrays exist before timing. Ingest includes copying them into a new
builder; freeze-only reuses a pre-ingested builder. Queries hit a fixed middle
coordinate/row/column and return the whole eager copy or view result.

| Operation | 0.1.0 | 0.1.1 candidate | Observed speedup | Heap B/op: 0.1.0 → 0.1.1 |
|---|---:|---:|---:|---:|
| Ingest + freeze, 8,192 entries | 820.958 µs | 775.333 µs | 1.06× | 2,482,848 → 2,384,080 |
| Freeze pre-ingested builder | 303.334 µs | 296.375 µs | 1.02× | 1,001,752 → 968,912 |
| Generated point block copy, 8 doubles | 50.333 ns | 46.428 ns | 1.08× | 120 → 120 |
| Generated row copy, 32 blocks | 0.839 µs | 0.732 µs | 1.15× | 5,960 → 5,960 |
| Generated row views, 32 blocks | 0.564 µs | 0.487 µs | 1.16× | 3,400 → 3,400 |
| Generated column views, 256 blocks | 4.438 µs | 3.604 µs | 1.23× | 24,248 → 24,248 |

Construction allocation fell about 4% for ingest+freeze and 3.3% for freeze alone.
The query allocation medians did not change on this JVM. The small timing gains
are observations, not confidence-bounded claims of a universal improvement.

### CSR64 structural paging

Wide fixtures have 100,000 base entries. Tail-only adds one entry; mixed edits
combine deletions, insertions and repeated puts with last-record-wins semantics.
Each early/middle/late call copies 64 visible entries into reused destinations.

| Operation | 0.1.0 | 0.1.1 candidate | Observed speedup | Heap B/op: 0.1.0 → 0.1.1 |
|---|---:|---:|---:|---:|
| Tail insertion: early 64-entry page | 5.083 µs | 0.916 µs | 5.55× | 1,280 → 1,192 |
| Tail insertion: middle 64-entry page | 2,047.250 µs | 0.833 µs | ≈2,458× | 2,399,768 → 1,240 |
| Tail insertion: late 64-entry page | 4,089.542 µs | 0.834 µs | ≈4,904× | 4,796,720 → 1,240 |
| Mixed edits: early 64-entry page | 5.708 µs | 0.875 µs | 6.52× | 1,280 → 1,192 |
| Mixed edits: middle 64-entry page | 2,096.584 µs | 0.833 µs | ≈2,517× | 2,399,792 → 1,240 |
| Mixed edits: late 64-entry page | 4,054.167 µs | 0.792 µs | ≈5,119× | 4,796,768 → 1,240 |
| 4,096-entry mixed row: drain in 64-entry pages | 5,902.791 µs | 37.875 µs | ≈156× | 6,367,664 → 76,512 |
| 1,024 mixed rows × 16 base entries: drain | 2,185.750 µs | 836.125 µs | 2.61× | 3,299,192 → 1,545,384 |
| Unmodified 4,096-entry row: full copy | 13.458 µs | 6.458 µs | control; not attributed | 1,288 → 1,288 |

The late tail page fell from **4.090 ms to 0.834 µs**, while allocating **4.80 MB →
1.24 KB** per call. The bounded drain improved about **156×**. The large ratios
come from removing work proportional to the skipped prefix, not from increasing
raw memory-copy bandwidth. Regression tests independently bound scalar reads and
requested base-copy volume for nonsequential cursors.

### Dense edits and compilation tradeoffs

The dense fixture replaces every other coordinate in a 4,096-entry base row and
adds one tail insertion: 2,049 records, 4,097 visible entries and alternating
one-entry clean/patch runs. Compilation includes defensive copies and consumption
of a metadata hash; artifact creation/opening and oracle checks are untimed.

| Operation | 0.1.0 | 0.1.1 candidate | Observed speedup | Heap B/op: 0.1.0 → 0.1.1 |
|---|---:|---:|---:|---:|
| Dense structural row: full 4,097-entry copy | 354.125 µs | 157.166 µs | 2.25× | 334,880 → 144,184 |
| Dense ledger: 2,049 records compiled | 924.000 µs | 1,133.625 µs | 0.82× (23% slower) | 3,054,600 → 3,679,152 |
| Single tail insertion compiled | 1.875 µs | 2.209 µs | 0.85× (18% slower) | 5,552 → 6,416 |

Dense copy improved **2.25×**, but dense compilation became **23% slower** and
allocated **20% more heap**. Single-insert compilation became **18% slower** and
allocated **16% more**. This is a read-optimised tradeoff, not an across-the-board
win: applications that frequently compile cuts but rarely read them should
measure their own replay/read ratio. Retained run-index size was not measured.
Dense value-only replacement packing/compaction remains future work.

## Measurement protocol and limitations

- Host: macOS ARM64 (`aarch64`), 10 reported processors, OpenJDK 64-Bit Server VM
  **25.0.3**, Clojure **1.12.4**; `-Xms256m -Xmx1g`.
- Three suites × two revisions × three forks = **18 fresh JVMs**, run serially.
  Revision order was baseline/candidate, candidate/baseline, baseline/candidate.
  The actual tag and candidate were checked out; identical saved harness text was
  loaded in memory on both. No baseline source was emulated or partially replaced.
- At least **1 second warmup per operation**, **31 rotating samples** per fork.
  Core batches: ingest/freeze 1; point 1,024; row copy/view 64; column view 16.
  Overlay read/cost batches: 1. Each sample is divided by its operation count.
- Core results escape through a volatile sink. Overlay reads consume first/last
  lanes and the final column. Full exact-output checks run before and after the
  timed section. Overlay checks include raw double bits and untouched sentinels.
- All 18 exact-output oracles passed. Source/environment fingerprints and raw
  samples are emitted by the harnesses; `--smoke` is not performance evidence.
- Sub-microsecond batch-one timings are sensitive to timer/JIT effects. The
  unchanged clean-copy control moved from **13.458 to 6.458 µs** in the mixed
  suite; this is not attributed to a base-copy code change. Suite-wide allocation,
  GC, cache state and JIT behaviour can affect even unchanged operations.
- Baseline late-tail fork medians ranged **3.821–5.112 ms**; candidate medians
  were **0.833–0.834 µs**. Core freeze medians ranged **303.250–305.208 µs** versus
  **294.000–296.458 µs**. Three local forks do not establish statistical confidence,
  deployment p99, cold-start, cold-cache or concurrent performance.
- These measurements supersede the intermediate-baseline numbers on `simplify`
  for release comparison. They must not be mixed with those older results.

## Reproduce

From the candidate checkout, run each command in a fresh JVM three times, serially:

```sh
clojure -J-Xms256m -J-Xmx1g -Sdeps '{:paths ["src" "bench"]}' -M -m sparse-layout.release-bench
clojure -J-Xms256m -J-Xmx1g -Sdeps '{:paths ["src" "bench"]}' -M -m sparse-layout.csr64-overlay-bench
clojure -J-Xms256m -J-Xmx1g -Sdeps '{:paths ["src" "bench"]}' -M -m sparse-layout.csr64-overlay-bench --costs
```

Both harnesses accept `--smoke` and `--check`; the overlay cost mode accepts these
after `--costs`. Reports include raw samples, median/p95 and heap allocation.
To reproduce the baseline in the same checkout, first preserve local changes,
check out `0.1.0` detached, and load the **candidate** harness with `git show` rather
than expecting it to exist in the old tag. For example, after the candidate is
committed on branch `0.1.1`:

```sh
git switch --detach 0.1.0
clojure -J-Xms256m -J-Xmx1g -Sdeps '{:paths ["src" "bench"]}' -M -e '(require (quote clojure.java.shell)) (let [r (clojure.java.shell/sh "git" "show" "0.1.1:bench/sparse_layout/release_bench.clj")] (assert (zero? (:exit r))) (load-string (:out r))) (prn (sparse-layout.release-bench/run-benchmark {})) (shutdown-agents)'
git switch 0.1.1
```

For overlay reads/costs, load `csr64_overlay_bench.clj` instead and call
`sparse-layout.csr64-overlay-bench/run-benchmark` or `run-cost-benchmark` with `{}`.
Keep all other settings identical and alternate revision order across forks.

## Candidate verification

- Production suite: **87 tests / 8,112 assertions**, zero failures/errors.
- Dedicated benchmark suites: **10 tests / 52 assertions**, zero failures/errors.
- Packaged JAR: the same **87 production tests / 8,112 assertions** pass with
  production namespaces verified to load from the JAR, not the source directory.
- Project lint, formatting, benchmark smoke and whitespace/diff checks pass.
- `bb bridge next`: **All Clear**, with unit/lint/format/benchmark evidence refreshed.
- `target/sparse-layout-0.1.1.jar` builds with POM version `0.1.1`; all eight packaged
  Clojure source files match the checkout exactly, with no experimental backend.
  SHA-256: `929c53ba9e9c7bd691e2213784afc7ee625cc401e2703af5fa7b61fc294f2e0f`.
  The existing `0.1.0` JAR is preserved; candidate classes used separate build staging.

The production source tree is unchanged from the measured `eef32bd` candidate.
The standard rebuild command is `clojure -T:build jar` (it clears `target`, so
preserve older local artifacts first). JAR timestamps can change the checksum on
rebuild without changing source content.

No tag, push, Clojars deployment or hosted release is performed by this preparation.
