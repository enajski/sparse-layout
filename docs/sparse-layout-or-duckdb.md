# Sparse layout or DuckDB?

## Hammock notes on ownership, shape, and the cost of owning a storage engine

**Decision memo · 4 September 2026 · repository at `a1b1054`**

**Scope used here:** “arena” means arrays preallocated and owned by a downstream
system. A `CSRSource` bulk copy fills selected IDs and double blocks directly;
the older callback remains for custom transforms. COO/CSR construction may
allocate.

> **Short answer after measuring:** do not replace the hot CSR traversal with a
> DuckDB 1.5.5 Parquet query for this 295-double workload. Keep Parquet/DuckDB as
> the canonical-data side and generate a narrow CSR serving value when a snapshot
> will receive enough visits. The arena path now allocates roughly 248–320 JVM
> bytes per *call*, rather than roughly 200–209 bytes per edge through the generic
> callback.

The useful choice is not “clever CSR versus clever database.” It is:

1. let DuckDB own the data and stream selected result chunks into the consumer's
   preallocated arrays; or
2. keep the small CSR bulk-transfer boundary when repeated sparse traversal is
   measurably cheaper, optionally letting DuckDB own upstream data work.

Do not expand both systems after deciding. The bounded comparison in
[The experiment that earns the next line of code](#the-experiment-that-earns-the-next-line-of-code)
now supports one narrow hybrid: Parquet/DuckDB owns canonical data; CSR owns only
the amortized repeated-drain path.

---

## The verdict before the tour

### My recommendation

Use **DuckDB 1.5.5 upstream and a small CSR derivative downstream**. The matched
benchmark now copies exact-equal output from three Parquet encodings and
`CSRSource` into the same preallocated arrays. Bulk CSR was 26–49× faster for
full snapshot materialization and 489–3,917× faster for one-row selection across
the existing small/medium/large shapes. Preserve the adjacency kernel; do not use
those numbers to preserve every macro, facade, overlay, and persistence feature
around it.

```text
                         ┌─ CSRSource bulk copy ───────────────┐
input / snapshot ───────┤                                     ├─→ same preallocated arrays
                         └─ DuckDB → prepared chunked query ───┘
```

That recommendation has four parts:

- **Keep construction allocation out of the acceptance criterion.** Measure it
  separately for snapshot cadence and peak-memory impact.
- **Put the consumer arrays outside the timed scan** and reuse them in both
  implementations.
- **Use flattened scalar result columns at the Java boundary.** The current
  chunked-result API avoids per-row JDBC overhead but does not read nested
  values. Treat direct Arrow IPC/ADBC as a separate source-format experiment,
  not as an invisible optimization of Parquet.
- **Keep only the part DuckDB did not replace:** ordered row pointers, column
  IDs, contiguous 295-double blocks, and a caller-buffer scan.

### Why this is not yet “use DuckDB and go home”

The clarified contract permits construction to allocate. `sparse-layout`
therefore does implement its central serving idea. `csr-copy-ranges!` copies row
IDs, column IDs, and contiguous fixed-width blocks into caller-owned arrays with
one destination entry offset. Heap sources use `System/arraycopy`; mmap sources
use bulk `IntBuffer`/`DoubleBuffer` reads. Capacities are checked before any
write. The generic per-edge callback remains for custom transforms and overlays,
but it is no longer the allocation-sensitive arena API.

DuckDB can implement the same *outcome*: stream a prepared query in columnar
chunks and loop over primitive vectors, writing into the destination arrays.
Those query chunks remain DuckDB-owned and each query has execution state, but
their address and lifetime no longer have to match the destination arena. A copy
was already part of the intended operation.

The decisive question is therefore:

> **How many entries does one visit select, and how many times is the frozen
> snapshot visited before it is replaced?**

The measured time-only crossover is already small for bulk work: roughly 3–5
full drains repay Parquet→CSR construction. For isolated rows it is about
154–270 visits per snapshot, and for 64-row ranges about 44–180. Use the actual
production selection mix and snapshot lifetime to decide whether to materialize
the derivative at all.

---

## First hammock: name the thing

An implementation can be impressive and still solve a neighboring problem.
Separate the desired outcome from the mechanism already built.

| Phrase | A testable meaning | What exists today |
|---|---|---|
| **Efficient ingestion** | Selected sparse blocks are materialized into the downstream system's arrays | `csr-copy-ranges!` fills row IDs, column IDs, and dense blocks in CSR order |
| **Low allocation** | After destination allocation, a scan creates no payload arrays and stays within a measured budget | bulk copy allocates about 248–320 B/call; generic visiting remains about 200–209 B/edge |
| **Preallocated arena** | The downstream system sizes and owns result arrays; source storage may allocate independently | This is supported for fixed double blocks; notebook helpers currently allocate arrays but callers need not |
| **Sparse / dense mixed** | A defined physical choice for sparse and dense regions, with a density crossover | Sparse outer coordinates with one uniform payload kind; “dense” means a dense vector inside each present cell |
| **Zero-copy** | Not the goal here: a deliberate copy into owned destination storage is acceptable | Payload bytes are copied; allocation of a fresh destination per entry is avoided |

This changes the framing. Construction allocation is a cost to amortize, not a
contract violation. The repository now has both the intended fixed-block
materialization boundary and matched evidence against a DuckDB chunk adapter.

### The product invariant test

Fill in these sentences before choosing a backend:

```text
The destination arrays are sized by ____________________________.
One visit writes approximately __________ entries / __________ doubles.
One frozen snapshot receives approximately __________ visits.
The visitor may allocate at most __________ bytes per visit / entry.
The p99 materialization target is ______________________________.
The snapshot is replaced every _________________________________ .
```

Those numbers determine whether paying once for CSR construction is valuable or
whether a database query can fill the same destination cheaply enough.

---

## What `sparse-layout` actually is

The repository describes a `defsparse` compiler from nested maps to COO and
then primitive CSR/CSC ([README](../README.md#sparse-layout)). The generated
surface includes a builder, ingestion/freeze functions, a frozen type, point
lookups, row/column APIs, views, and collection facades
([`core.clj`](../src/sparse_layout/core.clj#L1467)). A separate fixed-block
`CSRSource` protocol backs heap and mmap implementations
([`protocols.clj`](../src/sparse_layout/csr_source/protocols.clj#L3)).

### Actual flow

```text
already-materialized Clojure records
    │ generated loop over rows and columns
    ▼
HashMap dictionaries + ArrayList reverse dictionaries
    │ first-seen integer IDs
    ▼
growable row IDs + column IDs + payload buffers (mutable COO)
    │ exact copies + two stable counting-sort passes
    ▼
duplicate coalescing (:last / :sum / :merge / :error)
    │ payload gather + CSR + optional CSC + dictionary copies
    ▼
generated frozen value
    ├── convenient APIs: values, pairs, maps, views
    └── fixed-double `CSRSource`
          ├── heap arrays
          └── custom little-endian mmap artifact
                ├── prefix-range index
                └── mutable DOK delta overlay
```

That is a specialized immutable **serving representation** built by a batch
compiler. The clarified arena contract is exactly the bottom boundary:
`csr-copy-block!` copies a fixed-width block into a caller-provided `double[]`;
the heap implementation uses `System/arraycopy`
([`csr_source.clj`](../src/sparse_layout/csr_source.clj#L171)).

### “Mixed sparse/dense” is narrower than it sounds

Each layout chooses one payload representation: scalar double, scalar long,
object, fixed double block, or variable double block
([`core.clj`](../src/sparse_layout/core.clj#L27)). The coordinates are sparse;
a fixed/variable block is dense inside an occupied coordinate.

It does **not** currently provide:

- different payload physical types for different columns in one layout;
- adaptive dense rows or dense columns;
- a null bitmap or a density-triggered switch to dense storage;
- a representation for logical rows containing no entries; or
- heterogeneous payload lanes inside one fixed block.

`CSRSource`, overlay, and artifact support narrow the general payload surface to
positive-width fixed double blocks
([`csr_source.clj`](../src/sparse_layout/csr_source.clj#L11)). A more accurate
description is therefore:

> **dictionary-encoded sparse edges carrying uniform numerical blocks**

That is a good primitive. It is not a general mixed-record ingestion engine.

---

## Allocation anatomy

### Ingestion

The primitive buffers start with capacity 16 and double when full, allocating a
new array and copying the old contents
([`core.clj`](../src/sparse_layout/core.clj#L73)). `SparseBuilder` also owns two
`HashMap`s and two `ArrayList`s; IDs are boxed in the maps and assigned in
first-seen order ([`core.clj`](../src/sparse_layout/core.clj#L508)). Fixed
payload blocks are copied into the growable payload buffer as entries arrive.

There is no public `expected-rows`, `expected-columns`, or `expected-nnz` sizing
hint. That affects build time and peak memory, but it does not violate the
clarified serving contract; construction is allowed to allocate.

### Freeze

Freeze is sensibly linear in entries plus row/column cardinality, but it is not
an in-place compaction:

1. growable row/column buffers are copied to exact arrays;
2. stable counting sorts allocate permutations and count arrays;
3. duplicate coalescing allocates compact coordinates/payload references;
4. payloads are gathered into final storage;
5. CSR and optional CSC arrays are allocated; and
6. dictionaries and reverse dictionaries are copied for the frozen value.

The stable sort is semantically useful: it lets `:last` mean the last observed
duplicate while producing ordered adjacency. It is still a transient allocation
phase ([`core.clj`](../src/sparse_layout/core.clj#L547),
[`core.clj`](../src/sparse_layout/core.clj#L666),
[`core.clj`](../src/sparse_layout/core.clj#L934)).

### Retained primitive storage

For:

- `R` rows,
- `C` columns,
- `N` coalesced nonzeros, and
- a fixed block width `d` doubles,

the approximate final primitive-array payload/index footprint is:

| Layout | Approximate bytes, excluding dictionaries and object headers |
|---|---:|
| CSR, no retained COO | `4(R + 1) + N(4 + 8d)` |
| CSR + retained COO | `4(R + 1) + N(12 + 8d)` |
| CSR + CSC, no retained COO | `4(R + 1) + 4(C + 1) + N(12 + 8d)` |
| CSR + CSC + retained COO | `4(R + 1) + 4(C + 1) + N(20 + 8d)` |

CSC shares payload storage through payload IDs; it does not duplicate the dense
blocks. Normalization defaults to CSR and retained COO. The transient freeze
peak is materially higher than this retained size and has not been measured.

### Reads

The allocation story depends entirely on the API:

| API shape | Payload copy? | Per-result objects? |
|---|---:|---:|
| value-returning block lookup | yes, new `double[]` | yes |
| `DoubleBlockView` lookup | no | a view object |
| row/column convenience APIs | mode-dependent | vectors/pairs/views |
| map facade sequence/reduce | mode-dependent | entries and usually views |
| heap `csr-copy-block!` | yes, into caller array | reusable output; callback boxing remains possible |
| mmap `csr-copy-block!` | yes, double by double | reusable output |
| `csr-copy-ranges!` | yes, directly into caller arrays | measured 248–320 B per call |

The useful claim is not “zero allocation.” It is:

> **The fixed-block CSR source bulk-copies ranges into reused primitive output
> arrays with bounded per-call allocation rather than per-entry allocation.**

That claim is narrower, clearer, and benchmarkable.

The generic visitor still deserves a narrower claim. `csr-scan-row!` calls an
untyped Clojure function with three numeric IDs plus the source for every entry,
and the benchmark measures roughly 200–209 B/edge at that `IFn` boundary. It is
useful for arbitrary transforms and overlays, not for the arena fast path.
`csr-copy-ranges!` avoids that boundary: heap sources bulk-copy columns and
payloads, mmap sources use bulk buffer reads, and both fill row IDs directly.

### Ownership is not fully frozen

The frozen APIs expose raw backing arrays through `SparseInternals` and
`block-view-array` ([`core.clj`](../src/sparse_layout/core.clj#L1146),
[`core.clj`](../src/sparse_layout/core.clj#L1645)). Callers can mutate the
supposedly frozen value. Builders and deltas are unsynchronized; there is no
thread-safety or lifetime contract.

The mmap format keeps row pointers, column IDs, and payload mapped, but rebuilds
keys and lookup maps on heap. It has GC-managed unmapping, a single-buffer
`Integer/MAX_VALUE` file ceiling, and block reads still copy to the destination
([`artifact.clj`](../src/sparse_layout/csr_source/artifact.clj#L840),
[`artifact.clj`](../src/sparse_layout/csr_source/artifact.clj#L906)). It is a
portable-file mechanism, not an arena or FFI boundary.

---

## The semantic assets worth naming

Replacing mechanics is easy; accidentally replacing meaning is expensive.
These are the current semantics a migration must either preserve or explicitly
discard:

- row and column IDs follow **first observation**, not key sort order;
- duplicate policies are `:last`, `:sum`, arbitrary Clojure `:merge`, or error;
- stable sorting preserves ingestion order inside duplicate runs;
- CSR columns and CSC rows are strictly ordered after coalescing;
- point lookup is dictionary lookup plus binary search in an adjacency span;
- prefix selection can represent disjoint matching row ranges;
- delta entries override base values; deletes are tombstones;
- unknown delta keys use sentinel IDs and defined merge ordering; and
- numeric range selection intentionally cannot discover delta-only rows.

Some are product semantics. Some are consequences of the present mechanism.
Treating every consequence as a promise is how a prototype becomes a storage
engine by accident.

Ask of each invariant:

> Would a user notice, or only an implementation test?

Stable user-visible duplicate resolution probably matters. The integer identity
assigned to a first-seen keyword may not. An arbitrary Clojure merge function
may be essential—or it may be policy that prevents a clean data boundary.

---

## DuckDB, accurately dated

As of **3 September 2026**, DuckDB 2.0.0 is not a stable release.

- The official calendar lists **1.5.5** (22 July 2026) as the latest stable
  release and 2.0.0 for the **second half of October 2026**
  ([release calendar](https://duckdb.org/release_calendar)).
- A **2.0 alpha** was announced on 2 September 2026. DuckDB explicitly says the
  alpha clients are not production-ready
  ([alpha announcement](https://duckdb.org/2026/09/02/try-duckdb-20-alpha)).

So “especially since 2.0.0” should sharpen the exploration, not choose the
production dependency.

### What 2.0 is relevant to this decision

The official preview describes a new storage format, parser, asynchronous I/O,
broader pruning, `VARIANT` improvements, a server option, and a revamped C API
([2.0 highlights](https://duckdb.org/2026/08/17/duckdb-20-highlights)). The new
C API direction includes a versioned ABI, streaming results, clearer ownership,
and vector-buffer access.

But the merged design work was still labeled an initial draft with an explicit
warning not to build against it yet; Arrow/extension surfaces were outside the
first half of that work
([C API v2 PR #24702](https://github.com/duckdb/duckdb/pull/24702)). One telling
detail: a length-delimited input string can avoid the caller creating a
null-terminated copy, while DuckDB still makes its own internal copy.

**Inference:** 2.0 should make a future native integration cleaner. It is not
needed for the clarified contract: stable 1.5.x already has a chunked Java result
API from which primitive values can be copied into downstream-owned arrays.

---

## What DuckDB can absorb

DuckDB is an embedded analytical database: vectorized, columnar, transactional,
and good at bulk scans, joins, aggregates, persistence, and out-of-core work.
That overlaps a surprising amount of the repository because caller ownership of
the *destination* does not require caller ownership of the database's storage.

| Current concern | DuckDB replacement | Important difference |
|---|---|---|
| COO-like edge ingestion | long-form table through Java Appender, chunks, or Arrow | DuckDB owns persisted table storage |
| sorting/coalescing | `ORDER BY`, grouping, windows | ordering must be explicit; arbitrary Clojure merge is not automatic |
| row/column/prefix filtering | SQL predicates, unnesting, joins | no stable CSR span or entry ID contract |
| point lookup | predicates, zone maps, optional ART | engine execution overhead; ART index scans have eligibility limits |
| DOK overlay | transactional table updates/deletes | different snapshot and ordering semantics |
| mmap artifact | DuckDB file or Parquet | not the existing raw CSR mapping or `.slcsr` compatibility |
| schema/type validation | table schema, constraints, casts | key equality/serialization must be defined |
| ad hoc query/aggregation | native strength | this is where custom CSR is weakest |
| visitor/materialization | prepared chunked result, then an application loop | query chunks are engine-owned; destination arrays remain caller-owned |

The stable Java Appender is designed for high-throughput bulk loading and accepts
primitive values, arrays/lists, maps, structs, and unions
([Java data import](https://duckdb.org/docs/current/clients/java/data_import)).
Java result handling supports Arrow export and chunked results, although current
chunked Java results document basic-type limitations for nested values
([Java result handling](https://duckdb.org/docs/current/clients/java/result_handling)).
Vectorized Java table functions operate on chunks of 2,048 rows and write into
DuckDB-provided outputs
([Java functions](https://duckdb.org/docs/current/clients/java/functions)).

This is a far broader ingestion and query surface than the custom code—and it
comes with semantics, maintenance, and testing that you no longer own.

### The closest DuckDB analogue to the visitor

Use a prepared query whose result contains only basic scalar columns:

```text
SELECT row_id, col_id, p0, p1, ... p[d-1]
FROM edge
WHERE <selection>
ORDER BY row_id, col_id
```

`DuckDBChunkedResult` exposes a lazily fetched sequence of columnar chunks and
explicitly avoids the per-row overhead of JDBC `ResultSet`. For each chunk, take
the readable vectors once, then write their primitive values directly into the
already-allocated row/column/payload arrays. That is behaviorally analogous to
the current visitor, only chunk-oriented rather than callback-per-entry.

The first experiment should flatten a fixed block to `p0 … p[d-1]`: the Java
chunk reader currently supports basic types, not composites such as `LIST` and
`STRUCT`. Arrow export can preserve nested vectors, but it brings an Arrow
allocator and another lifetime surface. Test it second.

### What still differs

DuckDB does not document a public contract for:

- native CSR/CSC physical storage or sparse algebra;
- constant-time acquisition of a precomputed row span;
- stable entry IDs shared across calls;
- no JVM/native allocation while preparing and executing a query; or
- predictable micro-latency for thousands of tiny independent point queries.

DuckDB's own workload guide says many small transactions/queries are not its
primary design target
([workload tuning](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads)).
Its `memory_limit` is not a whole-process cap; some structures allocate outside
the buffer manager
([resource limits](https://duckdb.org/docs/current/operations_manual/limits),
[OOM guidance](https://duckdb.org/docs/current/guides/performance/oom)).

None prevents copying a result into the destination arena. They may determine
whether doing so repeatedly is cheaper than retaining CSR.

---

## Model the data before comparing engines

DuckDB documents fixed `ARRAY`, variable `LIST`, `MAP`, `STRUCT`, `UNION`, and
heterogeneous `VARIANT` types, but no dedicated sparse vector/matrix physical
type ([type catalog](https://duckdb.org/docs/current/sql/data_types/overview)).

### Candidate encodings

| Encoding | Shape | Fit |
|---|---|---|
| **Long edge table** | `(row_key, col_key, ingest_seq, p0 … p[d-1])` | Best first comparator; directly readable through Java chunks |
| Parallel lists | one row plus `col_ids[]` and `payloads[]` | Similar logical row grouping; more awkward column scans and updates |
| `MAP` | one row plus `MAP(col, payload)` | Convenient sparse value, but no duplicate keys and requires unnesting for many operations |
| Wide nullable table | one column per feature | Excellent when the feature universe is small/stable; poor for large/dynamic cardinality |
| `VARIANT` | heterogeneous self-describing values | Flexible ingest, weak contract for the fixed numerical hot path |

Use the **long edge table** first. It makes the comparison honest and preserves
room for both DuckDB and CSR:

```sql
CREATE TABLE edge (
  row_key     ...,          -- normalized, not arbitrary JVM identity
  col_key     ...,
  ingest_seq  BIGINT,       -- makes :last deterministic
  p0          DOUBLE,
  ...                       -- one scalar column per fixed block lane
  p_d_minus_1 DOUBLE
);
```

An `ARRAY` is a cleaner logical schema for a fixed block and Arrow can represent
it. Flattening is deliberately less elegant: it tests the shortest stable Java
path to the consumer arrays before introducing Arrow. The important design is
one sparse coordinate per row and an explicit ingestion sequence.

### Translating current semantics

- Preserve `:last` with an explicit `ingest_seq` and a window that selects the
  greatest sequence per `(row_key, col_key)`.
- Implement `:sum` with grouping and element-wise aggregation.
- Implement `:error` with a uniqueness check/constraint or preflight grouping.
- Keep arbitrary `:merge` outside SQL unless a small, stable algebra can replace
  the Clojure function.
- Define ID assignment explicitly. If first-seen IDs matter, derive them from
  minimum `ingest_seq`; otherwise use sorted or surrogate IDs and declare the
  old IDs non-semantic.
- Always specify `ORDER BY row_id, col_id` at the CSR export boundary. SQL result
  order is otherwise not a contract.

Arbitrary JVM-object keys are another boundary. The artifact already supports
only nil, booleans, integers, doubles, strings, keywords, and nested vectors.
Choose a portable key schema rather than smuggling Clojure equality into the
database.

### Indexes are not CSR

DuckDB automatically maintains zone maps; data ordering improves their pruning.
Adaptive Radix Tree indexes help very selective single-column predicates, but
the documented index-scan path is limited to single-column, non-expression ART
indexes; multi-column ART does not currently qualify
([indexing guide](https://duckdb.org/docs/current/guides/performance/indexing),
[indexes](https://duckdb.org/docs/current/sql/indexes)).

For `(row_id, col_id)` point lookup, test:

1. sorted storage and zone maps;
2. an ART on `row_id` followed by `col_id` filtering; and
3. a packed scalar coordinate only if profiling justifies it.

Do not assume “database has an index” means “same inner loop as CSR binary
search.”

---

## The desired operation is an intentional copy

DuckDB's Arrow integration is officially described as zero-copy, streaming,
parallel, and pushdown-aware
([DuckDB–Arrow](https://duckdb.org/2021/12/03/duck-arrow)). That is valuable, but
the phrase hides boundaries:

| Boundary | The question |
|---|---|
| Decode | Did bytes become typed values without a temporary object graph? |
| Ingest API | Did the engine copy the supplied vectors? |
| Persistent table | Were values rewritten into managed segments? |
| Query input | Can operators scan the original Arrow buffers? |
| Operators | Did sort/join/grouping allocate state? |
| Result | Can the consumer retain or directly use engine vectors? |

Arrow can make **query input** zero-copy when layout and lifetime rules align.
It does not make the plan, aggregation, sort, output, or persistence free of
allocation. More importantly, zero-copy is not the goal at the serving boundary:
both contenders deliberately move selected values into arrays the downstream
system already owns.

`csr-copy-block!` performs that move with `System/arraycopy`. A DuckDB chunk
adapter performs it by reading primitive column vectors. The comparison is
therefore unusually clean: same selected entries, same order, same preallocated
destination, different source representation and traversal overhead.

The right metric is not ideological purity. It is total copies, bytes allocated,
peak RSS, and consumer latency across the complete boundary.

The current C API exposes data chunks/vectors and Arrow conversion; chunks can
be reset and reused, while pointers must be reacquired
([data chunks](https://duckdb.org/docs/current/clients/c/data_chunk),
[vectors](https://duckdb.org/docs/lts/clients/c/vector),
[C API](https://duckdb.org/docs/current/clients/c/api)). ADBC also supports
Arrow-stream bulk binding
([ADBC](https://duckdb.org/docs/current/clients/adbc)). These are optional
integration surfaces; chunked Java results are sufficient for the first test.

---

## Three coherent architectures

### A. DuckDB only — plausible direct replacement

```text
source → Appender → DuckDB table → prepared chunk result → preallocated arrays
```

Choose this when:

- persistence, schema evolution, filtering, joins, and aggregation matter;
- the visitor's work can be expressed as a selection and explicit order;
- the consumer can drain engine-sized chunks into its arrays;
- microsecond-scale point serving is not the dominant workload; and
- repeated query execution stays within the allocation and p99 budget.

What disappears: custom sorting/coalescing, overlay rules, query facades, prefix
indexes, binary format, corruption validation, and much of the generated API.

### B. CSR bulk transfer — when repeated traversal is the product

```text
allocated build/freeze → CSRSource bulk copy → downstream preallocated arrays
```

Choose this when measurements require:

- one snapshot serves many row/range materializations;
- predictable row-span traversal and buffer reuse; or
- a raw mmap layout consumed without a database engine.

This is the narrow `CSRSource` design. `csr-copy-ranges!` fits the clarified
requirement; the generic per-entry visitor stays available for transforms. The
minimal retained product is row pointers, column IDs, fixed-width payloads, and
only the dictionaries required for selection.

### C. DuckDB plus a CSR serving index — only if A and B each win something

```text
                        ┌──────────── ad hoc SQL / analytics
                        │
source → DuckDB canonical edge table
                        │ count + ORDER BY row_id, col_id
                        ▼
                  derived CSR serving index
                        │
                        └──────────── bulk copy → destination arrays
```

DuckDB handles the high-entropy work: decoding boundaries, types, validation,
deduplication, relational transformation, persistence, updates, and ordering.
The packer handles one low-entropy contract: write a sorted fixed-block stream
into exact CSR storage.

A count pass provides `N`; row/column cardinalities provide pointer and
dictionary sizes. A sorted stream then fills `row_ptrs`, `col_ids`, and payloads
once. If upstream ordering is guaranteed, this avoids growable COO buffers,
counting-sort permutations, and several freeze copies.

This hybrid is justified only if DuckDB's data/query advantages matter *and* its
repeated materialization misses the serving gate. Otherwise it pays for two
representations to solve one job. The reverse integration—exposing CSR through
Arrow or a table function—is similarly optional, not a default roadmap.

---

## Decision table

| Requirement | DuckDB chunk adapter | Current `CSRSource` | DuckDB + CSR index |
|---|:---:|:---:|:---:|
| fill caller-preallocated arrays | yes, explicit copy from vectors | yes, explicit range copy | yes |
| no fresh payload array per entry | expected; must measure Java path | yes in `csr-copy-ranges!` | yes |
| no per-entry boxing/wrapper allocation | chunk-oriented; measure getters | yes on bulk path; no on generic visitor | yes on bulk path |
| deterministic adjacency span | not as public contract | yes | yes |
| thousands of tiny point reads | workload mismatch; measure | designed for it | designed for it |
| bulk dynamic selection | strong | range/prefix-specific | both paths available |
| ad hoc filters/joins/aggregates | excellent | bespoke/limited | excellent upstream |
| transactions and concurrent writers | built in, within documented process model | no | built in upstream |
| out-of-core operators | yes | no | yes upstream |
| durable format/schema evolution | built in | custom v1 format | built in upstream |
| heterogeneous nested data | rich types | one payload kind per layout | rich upstream, fixed serving index |
| native sparse matrix storage | no | CSR/CSC | CSR |
| implementation you maintain | schema + drain loop | ~4,257 source lines plus format | adapter + derived-index lifecycle |

DuckDB supports multiple writer threads/connections within one process, while
multi-process writes are not the primary model
([concurrency](https://duckdb.org/docs/current/connect/concurrency)). This may be
more than the custom builder offers, but it is not a server-style concurrency
model unless the separate 2.0-era server work is deliberately adopted.

---

## Are you overthinking it?

### Yes, in scope

The runtime has grown to roughly 4,257 source lines and includes five payload
kinds, four duplicate policies, CSR and CSC, code generation, two facade modes,
heap and mmap sources, a custom file format, prefix indexes, and a DOK overlay.

The motivating fixed-block copy contract now has a direct materialization
benchmark, an allocation profile, a visits-per-snapshot amortization model, and
a DuckDB chunked-result comparison. Production p99/allocation gates, real-data
skew, and the density crossover remain unspecified. The measurements justify a
narrow serving kernel, not every mechanism surrounding it.

Particularly suspect surfaces are retained COO by default, generated marker
protocols with no internal consumer, a 437-line collection facade around an
allocation-sensitive representation, and custom persistence before a measured
need for direct raw startup.

### No, in the irreducible concern

Precomputed sparse adjacency and transfer into caller-owned numerical arrays are
real serving concerns. The current `CSRSource` expresses them directly. DuckDB
can produce the same final arrays, but it must first bind/execute a query and
drain engine-owned chunks.

The simplification is to make that cost difference earn the custom system.

> **Keep the serving index only when reuse pays back its construction and
> maintenance.**

---

## The experiment that earns the next line of code

Do not benchmark an isolated `freeze` against an entire database query. Build
each snapshot once, then compare the repeated operation you actually care about:
selected sparse values copied into the same preallocated arrays in the same
order. Record construction separately so reuse can amortize it honestly.

### Contenders

1. **Bulk CSR:** build/freeze once; repeat `csr-copy-ranges!` into caller arrays.
2. **DuckDB stable:** append once; repeat a prepared, ordered
   `DuckDBChunkedResult` and drain scalar vectors into the same arrays.
3. **Generic visitor control:** repeat `csr-scan-*` plus `csr-copy-block!` to
   isolate the old callback cost.
4. **Hybrid, later:** DuckDB builds a derived CSR index, only if each of the first
   two wins a different production requirement.

The bulk/visitor split prevents a loss at the generic Clojure callback from being
misattributed to CSR, while keeping the comparison focused on the same output
arrays rather than the macro, facade, overlay, artifact, or custom builder.

### Workloads

Use real distributions plus a controlled matrix:

- densities below, near, and above the expected sparse/dense crossover;
- fixed widths such as 1, 4, 24, and the production width;
- already-sorted versus shuffled input;
- no duplicates, clustered duplicates, and adversarial duplicates;
- compact integer keys and real logical keys;
- selection sizes of one row, tens of rows, thousands of rows, and the full
  snapshot;
- row scans, batched points, column scans, prefix filters, and one real aggregate;
- 1, 10, 100, and the production number of visits per frozen snapshot;
- heap and cold/warm reopen if persistence matters; and
- repeated reuse of the same destination arrays.

The current largest benchmark contains 786,432 entries, block width 4. It is a
useful smoke scale, not evidence for production limits.

### Metrics

Report all of these; a single throughput number will hide the trade:

| Phase | Required observations |
|---|---|
| construction, reported separately | rows/s, entries/s, wall time, peak RSS |
| each repeated materialization | wall/CPU, JVM allocated bytes, GC, DuckDB native memory |
| copies | logical count and bytes at decode, ingest, persist, query, result |
| serving | median/p99 by selected-entry count; bytes allocated per visit and per selected entry |
| persistence | file size, write time, cold open, warm open, first query |
| amortization | `T_build + Q × T_materialize` over realistic visits `Q` |

The existing `ThreadMXBean` construction benchmark measures repeated freeze of
an already-populated builder. Existing comparators are persistent maps and
Vectorz, not DuckDB, Arrow, a direct sorted packer, or `CSRSource` hot scans.
The benchmark smoke checks numerical agreement but has no performance gate.

### Fresh repository measurements

These checks were run on 3 September 2026 on ARM64 macOS, OpenJDK 21.0.12,
Clojure CLI 1.12.4.1618, and Babashka 1.12.218. They did not add dependencies or
alter tracked runtime code.

| Check | Result |
|---|---|
| `clojure -M:test` | 45 tests, 142 assertions, 0 failures/errors; 1.86 s wall |
| `clojure -M:bench smoke scalar` | semantic cross-check passed at 24,576 nonzeros; 3.73 s wall |
| `clojure -M:construct scalar` | failed because its alias omits Vectorz while imported benchmark data requires it |
| `clojure -M:memory scalar` | failed with `AttachNotSupportedException`, including with `StartAttachListener` |

Using the existing combined `:bench:construct` aliases made it possible to
measure isolated freeze without editing the project:

| Shape | Nonzeros | Mean freeze | Heap allocation/call | Allocation/nonzero |
|---|---:|---:|---:|---:|
| scalar/small | 24,576 | 1.419 ms | 3.30 MiB | 140.8 B |
| scalar/medium | 262,144 | 16.793 ms | 34.86 MiB | 139.5 B |

This fixed-dimension-4 benchmark layout uses CSR + CSC and no retained COO. Its
final primitive floor is approximately 44 bytes/nonzero—32 payload bytes and
three 4-byte index arrays—before pointers, array headers, and dictionaries.
Freeze therefore allocated about 3.15 times that floor in this run. The input
grew 10.67×, time 11.83×, and allocation 10.56×: useful evidence of near-linear
freeze scaling across two points. It says nothing about allocation during the
repeated caller-buffer path, which is the clarified requirement.

The scalar/small quick microbenchmark reported:

| Operation | `sparse-layout` | Existing comparator |
|---|---:|---:|
| direct point | 37.1 ns | nested map 87.1 ns; Vectorz map 83.8 ns |
| facade point | 128 ns | nested map 87.1 ns |
| row reduction | 896 ns | nested map 135 ns; Vectorz map 100 ns |
| indexed column reduction | 2.93 µs | dual nested index 982 ns |
| build from flat entries | 6.01 ms | row map 4.32 ms; Vectorz 4.97 ms; dual index 8.87 ms |

The full quick run took 494 seconds, reported very wide 3σ bands, and capped JIT
calibration at 10 seconds where estimates ranged from 623 to 1,320 seconds.
Treat the point result as promising and every ratio as directional. The facade
results also reinforce a design warning: a compact representation does not make
an allocating convenience layer fast.

Those historical timings did not exercise the distinctive `CSRSource`
caller-buffer path or DuckDB. The following matched run does.

### Production-width Parquet comparison

The new executable benchmark is
[`duckdb_compare.clj`](../bench/sparse_layout/duckdb_compare.clj). It pins the
DuckDB JDBC artifact `1.5.5.0` (engine `v1.5.5`) and changes the payload from the
old four-double smoke shape to the production width: **295 doubles for every
present `(row-key, col-key)` pair**.

It retains the existing compound-map key and sparsity scales:

| Scale | Rows | Logical columns | Present columns/row | Edges | Logical payload |
|---|---:|---:|---:|---:|---:|
| small | 2,048 | 256 | 12 | 24,576 | 55.31 MiB |
| medium | 16,384 | 1,024 | 16 | 262,144 | 590.00 MiB |
| large | 32,768 | 2,048 | 24 | 786,432 | 1.73 GiB |

The bench setup creates three ZSTD Parquet files with identical logical values:

1. **column explosion:** one edge row with 295 `DOUBLE` columns;
2. **row explosion:** 295 `(lane, value)` rows for each edge; and
3. **list/array:** one 295-value list per edge, cast to `DOUBLE[295]` on read.

The third name needs care. Arrow has a `FixedSizeList` memory layout, but Arrow's
own Parquet mapping writes it as Parquet `LIST`; Parquet has no fixed-size-list
logical type ([Arrow Parquet mapping](https://arrow.apache.org/docs/cpp/parquet.html#logical-types)).
The local probe agreed: DuckDB wrote a `DOUBLE[295]` table but read the Parquet
column back as `DOUBLE[]`. The benchmark therefore casts the list to
`DOUBLE[295]`, which validates the length, then projects its elements. DuckDB
1.5.5's Java chunked API exposes only basic types, not `LIST`/`STRUCT`, so a
nested column cannot be drained directly through that API
([DuckDB Java result handling](https://duckdb.org/docs/current/clients/java/result_handling)).

Setup streams the compact list file through DuckDB, reconstructs the existing
compound keys, appends to the mutable COO builder, and freezes a CSR-only source.
Every timed contender then writes row IDs, column IDs, and 295-double blocks into
the same two preallocated `int[]` arrays and one preallocated `double[]`. Exact
array equality is checked before timing. DuckDB is single-threaded; query
warmups make these cache-warm results.

#### Setup and footprint

| Scale | Parquet→COO→CSR | JVM allocation | 295-column file | 295-row file | list file |
|---|---:|---:|---:|---:|---:|
| small | 300.2 ms | 193.46 MiB | 10.56 MiB | 9.80 MiB | 6.66 MiB |
| medium | 2.189 s | 2.66 GiB | 158.36 MiB | 96.33 MiB | 62.30 MiB |
| large | 5.954 s | 5.97 GiB | 287.91 MiB | 303.28 MiB | 221.42 MiB |

Construction allocation is intentionally allowed, but it is not free: the
large derivative allocates about 3.45× its 1.73 GiB logical payload while
decoding, growing COO storage, and gathering the frozen payload. These are total
thread allocations, not peak live heap. File generation is benchmark setup and
is not included in the Parquet→CSR time.

#### Serving result

For each selection, this table compares the new bulk copy with the *fastest*
direct Parquet shape. Times are medians; p95 and allocation remain available in
the command output.

| Scale / selection | Edges | Bulk CSR | Best direct Parquet | Winning Parquet shape | DuckDB / CSR |
|---|---:|---:|---:|---|---:|
| small / one row | 12 | 0.004 ms | 1.954 ms | 295 rows | 489× |
| small / 64 rows | 768 | 0.088 ms | 6.985 ms | 295 rows | 79× |
| small / full | 24,576 | 2.707 ms | 71.568 ms | 295 columns | 26× |
| medium / one row | 16 | 0.005 ms | 8.103 ms | 295 rows | 1,621× |
| medium / 64 rows | 1,024 | 0.114 ms | 14.896 ms | 295 rows | 131× |
| medium / full | 262,144 | 26.964 ms | 746.080 ms | 295 columns | 28× |
| large / one row | 24 | 0.006 ms | 23.502 ms | 295 rows | 3,917× |
| large / 64 rows | 1,536 | 0.163 ms | 33.237 ms | 295 rows | 204× |
| large / full | 786,432 | 54.256 ms | 2,677.841 ms | 295 columns | 49× |

The crossover did not appear. Row explosion wins DuckDB's selective case
because it reads four primitive result columns, but it pays 295 physical result
rows per edge. Column explosion amortizes that row overhead on full scans. The
compact list produces the smallest files yet is never the fastest Java path,
because it must be cast and projected into 295 basic result vectors.

The full-scan medians expose the important local trade-off as well:

| Scale | Bulk CSR | Generic visitor | 295 columns | 295 rows | list→`DOUBLE[295]` |
|---|---:|---:|---:|---:|---:|
| small | 2.707 ms | 2.222 ms | 71.568 ms | 157.413 ms | 149.954 ms |
| medium | 26.964 ms | 24.798 ms | 746.080 ms | 1,710.719 ms | 1,523.286 ms |
| large | 54.256 ms | 69.831 ms | 2,677.841 ms | 5,145.269 ms | 4,750.417 ms |

Bulk copy is 2–5× faster for one-row selections and about 2–3× faster for 64-row
ranges than the generic visitor. One giant full-range copy is 9–22% slower on
small and medium in these runs, then 22% faster at large. The old visitor remains
useful when arbitrary per-edge work matters; the bulk path makes allocation a
per-call cost when direct materialization matters.

#### The allocation result now matches the arena contract

Destination arrays are excluded, but source-side work is not:

| Full scan | Bulk CSR | Generic visitor | 295 columns | 295 rows | list→array |
|---|---:|---:|---:|---:|---:|
| small | 248 B | 4.73 MiB | 2.63 MiB | 7.69 MiB | 2.63 MiB |
| medium | 248 B | 52.08 MiB | 28.12 MiB | 82.00 MiB | 28.12 MiB |
| large | 248 B | 156.44 MiB | 84.35 MiB | 246.01 MiB | 84.35 MiB |

Across all selection sizes and scales, `csr-copy-ranges!` allocates roughly
248–320 bytes per invocation rather than per edge. That constant is protocol and
range-iteration overhead; no selected payload or coordinate object is created.
The generic visitor's cost remains visible and documented rather than being
silently called “zero allocation.” DuckDB's JVM number is incomplete because
`ThreadMXBean` cannot see native engine allocation.

#### Amortization

Using the compact-file Parquet→CSR setup time and the fastest direct Parquet
shape for each selection, the time-only break-even is approximately:

| Scale | One-row visits | 64-row visits | Full visits |
|---|---:|---:|---:|
| small | 154 | 44 | 5 |
| medium | 270 | 148 | 4 |
| large | 253 | 180 | 3 |

This excludes file generation, destination allocation, native memory, cold-cache
effects, concurrency, and downstream compute. It is nevertheless decisive
enough to reject “DuckDB always replaces the visitor.” If a snapshot receives
only one or two selective reads, avoid the CSR derivative. If it receives the
visits above—or only a handful of full drains—the derivative earns its build.

### Overlay growth: cache rows, split clean runs, compact by reuse

The companion [`overlay_compare.clj`](../bench/sparse_layout/overlay_compare.clj)
holds the production width at 295 doubles and grows a source-bound DOK overlay
through `0`, `1`, `10`, `100`, `1,000`, `10,000`, and `100,000` value overrides.
The base has 262,144 edges, 16 edges per row, and compound map keys. Two locality
extremes matter more than the raw delta count:

- **clustered:** fill each affected row before touching the next; and
- **scattered:** touch every row once before adding another change to a row.

The benchmark checks exact output before timing four paths:

1. the existing overlay visitor;
2. bulk-copy clean row runs and merge only dirty rows;
3. compact the overlay to a new CSR source, then bulk-copy it; and
4. the unchanged base bulk copy as a non-semantic lower bound.

The first run exposed repeated sorting as the dominant defect. At 100,000
overrides, the old implementation took 1,000 ms / 1.64 GiB per clustered full
scan and 670 ms / 1.07 GiB per scattered scan. Source-bound DOK rows now cache
CSR column order and invalidate only when that row changes. This reduced the
same visitor scans to 62.9 ms / 66.88 MiB and 74.4 ms / 66.37 MiB respectively.

#### Full-snapshot medians after row-order caching

| Locality | Overrides | Dirty base rows | Overlay visitor | Split bulk/merge | Compacted bulk | Compaction |
|---|---:|---:|---:|---:|---:|---:|
| clustered | 1,000 | 63 / 16,384 | 35.0 ms | 18.8 ms | 18.0 ms | 151.8 ms |
| clustered | 10,000 | 625 / 16,384 | 38.7 ms | 20.9 ms | 17.7 ms | 182.1 ms |
| clustered | 100,000 | 6,250 / 16,384 | 62.9 ms | 48.0 ms | 17.4 ms | 218.8 ms |
| scattered | 1,000 | 1,000 / 16,384 | 36.7 ms | 21.0 ms | 18.0 ms | 149.4 ms |
| scattered | 10,000 | 10,000 / 16,384 | 42.8 ms | 38.0 ms | 17.8 ms | 160.0 ms |
| scattered | 100,000 | 16,384 / 16,384 | 74.4 ms | 76.2 ms | 17.8 ms | 226.4 ms |

The split path stays near the 17–18 ms base bulk floor while most rows remain
clean. Once every row is dirty, it has nothing left to bulk-copy and should be
skipped. Its full-scan allocation ranges from hundreds of bytes with an empty
overlay to 33.81 MiB at 100,000 clustered changes and 66.03 MiB when all rows
are dirty. Compacted bulk reads stay at about 400 B/call, but compaction allocates
2.66–2.74 GiB and must be amortized.

#### Time-only compaction crossover

Including split-plan construction, compaction beats the best live-overlay path
after approximately:

| Overrides | Clustered | Scattered |
|---:|---:|---:|
| 1,000 | 201 full drains | 51 full drains |
| 10,000 | 58 full drains | 8 full drains |
| 100,000 | 7 full drains | 5 full drains |

These thresholds do not transfer to one-row or 64-row reads: their live cached
visits remain microsecond-scale, while full compaction costs 149–226 ms. The
minimal policy is therefore selection-aware:

1. use the cached visitor for narrow reads;
2. for broad reads, bulk-copy clean runs and merge dirty rows while row coverage
   remains incomplete; and
3. compact only when the expected remaining full drains cross the table above.

The current benchmark deliberately changes values at existing coordinates.
Inserts and deletes add an ID-policy question: delta-only rows and columns use
`-1`, while rebuilding dictionaries can renumber IDs. Benchmark them only after
deciding whether IDs are snapshot-local or externally stable; otherwise two
fast paths would be compared under different semantics.

#### Limits of this run

- Deterministic, uniform data; no nulls, duplicates, skew, or real Parquet file.
- One thread and warm filesystem cache; no cold-open or concurrency result.
- Five to thirty measured samples, so reported p95 is diagnostic rather than a
  production tail-latency claim.
- JVM current-thread allocation only; no DuckDB native RSS or peak live heap.
- No direct Arrow IPC/ADBC path. That is a separate source format, not the stated
  Parquet scenario.
- Heap and mmap bulk-copy correctness are covered; mmap throughput is not yet
  benchmarked at production scale.

### Decision after the run

Before running, write numbers in this table:

| Gate | Required value |
|---|---|
| visits per snapshot (`Q`) | ______ |
| typical / p99 selected entries per visit | ______ / ______ |
| maximum materialization p99 | ______ |
| maximum JVM allocation per visit / selected entry | ______ / ______ |
| maximum snapshot build time / peak RSS | ______ / ______ |
| maximum cold-open time | ______ |
| output arrays allocated outside timed region? | yes |
| stable row/column IDs externally observable? | yes / no |

The production gates still need values, but this baseline already selects the
architecture mechanically:

- **DuckDB meets every gate:** replace the custom engine.
- **Observed here:** DuckDB misses repeated serving latency while retaining its
  data-management role, so use the hybrid and retain the small CSR index.
- **Only bulk CSR meets the arena gates:** retain that boundary, not the rest of
  the system by association.
- **The current full implementation uniquely wins a production workload:** keep
  it, and turn that workload into a continuous evidence threshold.

### Next timebox

Do not add another backend matrix. Run one production input sample and set the
actual allocation/latency gate. Tune full-range copy granularity only if its
small/medium regression matters in the real selection mix. Test Arrow IPC/ADBC
only if it is a real source, rather than a hypothetical way to improve Parquet.

---

## If the custom kernel survives

Make its boundary smaller than the repository is today.

1. **Promote `CSRSource`, not `defsparse`, as the core contract.** Define
   destination sizing, logical output length, ordering, thread safety, and array
   reuse.
2. **Use bulk copy for arenas; keep callbacks for transforms.** Do not force one
   abstraction to serve two different jobs.
3. **Accept ordered input only if build cost matters.** A direct row-major path
   can avoid mutable COO plus global sorting, but construction is explicitly not
   the zero-allocation contract.
4. **Add `expected-nnz` only if growth appears in the snapshot budget.**
5. **Build CSC only for a measured column workload.** It adds pointer, row-ID,
   payload-ID, construction, and validation costs.
6. **Do not keep COO by default** unless an API consumes it.
7. **Choose one persistence owner.** Keep raw CSR mmap only if direct cold-start
   serving is the requirement; otherwise use DuckDB/Parquet upstream.
8. **Keep facades outside the hot contract.** Ergonomic maps and low allocation
   optimize for different consumers.
9. **Make “frozen” true or rename it.** Do not expose mutable backing arrays from
   an immutable abstraction without explicit unsafe naming.

This is a direction, not a request to implement all nine. Each surviving feature
still needs evidence beyond the adjacency kernel measured here.

---

## Second hammock: simple, easy, and owned complexity

DuckDB is not internally simple. Adopting it can still make *your system*
simpler because its complexity is behind a maintained boundary.

The current path began with simple values—coordinates and dense blocks—but now
complects:

- input traversal with ID policy;
- storage with duplicate policy;
- layout with API generation;
- fast reads with collection emulation;
- persistence with key serialization;
- immutable snapshots with mutable overlay semantics; and
- numerical serving with general querying.

The hammock question is not “could each feature be made fast?” It is:

> **Which concerns must change together?**

If changing duplicate policy requires changing the physical builder, generated
type, artifact assumptions, facade behavior, and overlay tests, those concerns
are not independent. DuckDB may be the simplest way to separate them, even if
its executable is larger.

Values, identity, and state also clarify the boundary:

- A frozen CSR snapshot is a **value**.
- First-seen row IDs are an **identity assignment policy**.
- A DOK delta is **state over time**.
- A DuckDB relation plus transactions is already a vocabulary for values and
  state.
- The destination arrays are a **place and lifetime** owned by the downstream
  system.

Do not encode state semantics into the place-specific kernel unless the hot
consumer actually requires them there.

---

## Questions to leave on the hammock

### Purpose

- Is the consumer doing sparse algebra, feature lookup, graph adjacency, SQL
  analytics, or serialization?
- Is the structure read once, scanned repeatedly, or updated continuously?
- Is “dense” a dense value block, a dense subset of rows, or a dense feature
  region that deserves a different representation?

### Destination

- How is the destination capacity known before a visit?
- Does the consumer require an entry callback, or is a chunk loop equivalent?
- Can it process roughly 2,048 result rows per chunk?
- Is output order part of the contract?

### Time

- How many reads amortize one freeze/export?
- Is cold open important enough to own a binary format?
- Are IDs stable across snapshots, or only inside one value?
- When does the entire arena become garbage at once?

### Policy

- Are `:last`, `:sum`, and arbitrary `:merge` all real production needs?
- Does first-seen ID order matter externally?
- Must empty rows exist?
- What happens at the density crossover?

### Evidence

- What is the allocation budget per visit and per selected entry?
- What p99 makes DuckDB unacceptable?
- At what selection size does the chunked query overtake CSR traversal?
- What dataset breaks the current 2 GiB mmap ceiling?
- Which result would cause you to delete the custom implementation?

The last question is the most important. A benchmark without a deletion
criterion is often permission to keep everything.

---

## Evidence and limits

### Repository evidence reviewed

- runtime source: 4,257 physical lines across `core`, `facade`, `csr_source`, its
  protocols, and artifact code;
- tests: 1,056 physical lines across three namespaces;
- DuckDB comparison benchmark: 627 physical lines;
- overlay-growth benchmark: 470 physical lines;
- README, Bridge profile/policy, and current working-tree notebook; and
- current test/benchmark structure and earlier recorded Bridge evidence.

The working tree already contained unrelated changes. This report treats the
untracked Lean project as an early executable sketch, not proof: it has no
theorems/lemmas and several placeholder or behaviorally different paths. No
claim in this memo relies on it.

### What remains unmeasured

The matched DuckDB dependency and benchmark are now present, but the synthetic
run is not a production acceptance test. Peak live heap, DuckDB native RSS,
cold-cache reads, concurrency, real-data compression/skew/nulls, Arrow IPC/ADBC,
and production selection mix remain unknown. The benchmark does measure
current-thread allocation in the hot path; it does not explain every allocated
object.

### Primary DuckDB sources

- [Release calendar](https://duckdb.org/release_calendar)
- [DuckDB 2.0 alpha announcement](https://duckdb.org/2026/09/02/try-duckdb-20-alpha)
- [DuckDB 2.0 highlights](https://duckdb.org/2026/08/17/duckdb-20-highlights)
- [C API v2 design PR](https://github.com/duckdb/duckdb/pull/24702)
- [Java overview](https://duckdb.org/docs/current/clients/java/overview)
- [Java data import / Appender](https://duckdb.org/docs/current/clients/java/data_import)
- [Java result handling](https://duckdb.org/docs/current/clients/java/result_handling)
- [Java functions](https://duckdb.org/docs/current/clients/java/functions)
- [C data chunks](https://duckdb.org/docs/current/clients/c/data_chunk)
- [C vectors](https://duckdb.org/docs/lts/clients/c/vector)
- [C API](https://duckdb.org/docs/current/clients/c/api)
- [Arrow integration](https://duckdb.org/2021/12/03/duck-arrow)
- [ADBC](https://duckdb.org/docs/current/clients/adbc)
- [Replacement scans](https://duckdb.org/docs/current/clients/c/replacement_scans)
- [Data type catalog](https://duckdb.org/docs/current/sql/data_types/overview)
- [`MAP` type](https://duckdb.org/docs/current/sql/data_types/map)
- [`VARIANT` type](https://duckdb.org/docs/current/sql/data_types/variant)
- [Array functions](https://duckdb.org/docs/current/sql/functions/array)
- [Arrow ↔ Parquet logical type mapping](https://arrow.apache.org/docs/cpp/parquet.html#logical-types)
- [Indexing guide](https://duckdb.org/docs/current/guides/performance/indexing)
- [Indexes](https://duckdb.org/docs/current/sql/indexes)
- [Workload tuning](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads)
- [Memory management](https://duckdb.org/2024/07/09/memory-management)
- [Resource limits](https://duckdb.org/docs/current/operations_manual/limits)
- [Out-of-memory guidance](https://duckdb.org/docs/current/guides/performance/oom)
- [Concurrency](https://duckdb.org/docs/current/connect/concurrency)

---

## One-page decision record

**Context.** The project builds sparse coordinates with dense numerical payloads,
then bulk-copies selected blocks into arrays preallocated by another system.
Construction may allocate. On the 295-double production width, Parquet-derived
CSR materialized the same outputs 26–49× faster for full scans and 489–3,917×
faster for one-row scans than the best of three DuckDB Parquet shapes. The
repository also owns sorting, duplicate resolution, CSR/CSC, query facades,
deltas, and persistence.

**Decision.** Keep DuckDB/Parquet upstream. Build a CSR derivative when snapshot
reuse crosses the measured amortization threshold. Retain and simplify the
serving kernel; do not preserve the whole surrounding system by association.

**Why.** DuckDB can remove most owned complexity and is stronger for mixed types,
persistence, transactions, filtering, and analytics. Because the final operation
already copies values, engine-owned result chunks are compatible with the arena
goal. The unresolved difference is query/chunk overhead versus precomputed CSR
adjacency. The repository's defensible differentiator is the narrow
`CSRSource` buffer-fill boundary, not the full generated storage ecosystem.

**Consequences.** Current first-seen IDs, duplicate order, and arbitrary key
semantics must be classified as product guarantees or implementation details.
Use `csr-copy-ranges!` for the arena contract and retain the generic visitor only
where per-entry transformation is required. DuckDB 2.0 alpha remains research
input, not a production target.

**Reversal.** Cheap. If DuckDB passes all gates, delete the custom serving path.
If it misses only repeated-serving gates, CSR remains a small derived index.

> **Final thought:** construction is not the argument, but reuse is. A 5.8-second
> derivative is waste for one read and cheap for hundreds; the same bytes become
> simple only after time is part of the value.
