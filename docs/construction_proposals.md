# Sparse Layout Design Proposals

## 1. Bottleneck Analysis: CSR/CSC via COO Construction

The current construction process for CSR/CSC matrices follows a "COO-to-CSR" pattern:
1. **Ingest**: Entries are collected into an `ArrayList` of tuples (COO).
2. **Sort**: The `ArrayList` is sorted by row and column indices.
3. **Coalesce**: Duplicates are merged, and indices are flattened into the final CSR format.

### Performance Bottlenecks
* **Object Allocation (GC Pressure)**: Every entry $(r, c, v)$ requires an object (or multiple, e.g., `Integer` for $r$ and $c$, and a tuple object). For large datasets, this causes massive pressure on the JVM Garbage Collector.
* **Boxing/Unboxing**: Using `Integer` objects for indices requires constant boxing/unboxing during sorting.
* **Sorting Overhead**: Sorting a list of objects with a custom `Comparator` is significantly slower than sorting primitive arrays.

---

## 2. Proposed Optimization Strategies

We propose three distinct strategies for constructing CSR/CSC layouts, depending on the input source and memory constraints.

### Strategy A: The "Packed-Long" Sort (Optimized In-Memory)
**Best for:** In-memory batch construction where performance is the primary concern.

Instead of creating objects for every entry, we pack the coordinates into a single `long` and use primitive arrays.

**Implementation Details:**
1. **Key Packing**: Represent each entry as a single `long` where the upper 32 bits are the `row` and the lower 32 bits are the `column`: `key = (row << 32) | col`.
2. **Primitive Arrays**: Maintain two parallel primitive arrays:
   - `long[] keys` (stores the packed `(r, c)`)
   - `double[] values` (stores the payload)
3. **Primitive Sort**: Use a primitive Dual-Pivot Quicksort (e.g., via a custom implementation or `Arrays.sort` on a custom primitive-aware wrapper) that sorts the `keys` and `values` arrays in tandem.
4. **Complexity**: $O(N \log N)$ time, $O(N)$ primitive space.

**Pros:**
* Eliminates nearly all object allocations (no `Integer`, no `Double`, no Tuple objects).
* Maximizes cache locality during the sort.
* Extremely fast on the JVM.

---

### Strategy B: The "Two-Pass" Streaming Construction (Memory Efficient)
**Best for:** Extremely large datasets that are stored in an indexable format (e.g., a large `vector` or an on-disk file) and where memory is limited.

This avoids the $O(N)$ space required for the intermediate COO format by using the input twice.

**Implementation Details:**
1. **Pass 1 (Counting)**: Iterate through the input data once to count the number of elements in each row.
2. **Prefix Sum**: Convert row counts into `row_ptr` (the cumulative sum of counts).
3. **Pass 2 (Filling)**: Re-iterate the input data. For each entry $(r, c, v)$, use the `row_ptr[r]` to determine the target index in the `col_indices` and `values` arrays.
4. **Complexity**: $O(N)$ time, $O(\text{num\_rows})$ intermediate space.

**Pros:**
* Near-zero intermediate memory footprint (only the `row_ptr` array).
* Minimal GC pressure.

**Cons:**
* Requires the ability to "rewind" or re-iterate the input source.

---

### Strategy C: The "Sorted-Map" Accumulator (Streaming-Only)
**Best for:** True streaming scenarios where the input cannot be re-read and memory is too small for the full dataset.

**Implementation Details:**
- Use a specialized primitive-friendly B-Tree or Skip-List that stores keys as packed longs.
- As entries arrive, insert them into the structure.
- If duplicates occur, perform in-place accumulation (e.g., sum the values).

**Pros:**
* Supports pure single-pass construction.

**Cons:**
- Significant overhead per entry due to tree balancing/node pointers.
- Much slower than Strategy A or B for most batch workloads.

---

## 3. Summary Comparison

| Metric | Current (COO) | Strategy A (Packed-Long) | Strategy B (Two-Pass) | Strategy C (Sorted-Map) |
| :--- | :--- | :--- | :--- | :--- |
| **Complexity** | $O(N \log N)$ | $O(N \log N)$ | $O(N)$ | $O(N \log N)$ |
| **Intermediate Space** | $O(N)$ (Objects) | $O(N)$ (Primitives) | $O(\text{num\_rows})$ | $O(N)$ (Tree nodes) |
| **GC Pressure** | Very High | Very Low | Very Low | High |
| **Primary Constraint** | Memory/GC | CPU (Sorting) | Input Iterability | CPU/Memory |
