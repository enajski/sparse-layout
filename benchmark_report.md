# Sparse-Layout Benchmark Report

## Overview
This report presents the performance analysis of the `sparse-layout` library. Benchmarks were conducted using `criterium` to compare different data access and construction methods across two key scenarios: **Scalar Keys** and **Compound Map Keys**.

## Methodology
Benchmarks were run in "quick" mode for efficiency. 
- **Dataset size:** 2048 rows $\times$ 256 columns.
- **Key shapes:** 
    - `:scalar` (simple primitive-like keys).
    - `:compound-map` (complex nested maps).
- **Scenarios:**
    - **Point Lookup:** Finding a value at a specific (row, col).
    - **Row/Column Scans:** Iterating through all elements in a row or column.
    - **Construction:** Building the data structures from raw entries.

---

## 1. Point Lookup Performance
*Goal: Accessing a single value given a row and column.*

| Method | Scalar Keys (ns) | Compound Map Keys (ns) | Efficiency (Scalar) |
| :--- | :--- | :--- | :--- |
| **Generated Sparse API** | **38** | **154** | **Fastest** |
| Nested Row Index | 81 | 234 | ~2.1x slower |
| Sparse Facade | 241 | 360 | ~6.3x slower |

### Key Findings
- The **Generated Sparse API** is significantly faster than all other methods. It is the preferred choice for performance-critical lookup operations.
- The **Sparse Facade** introduces substantial overhead, especially for scalar keys, likely due to map lookups and abstraction layers.

---

## 2. Scan Performance
*Goal: Iterating over all elements in a single row or column.*

### Row Scans
| Method | Scalar Keys (ns) | Compound Map Keys (ns) |
| :--- | :--- | :--- |
| **Nested Row Map** | **123** | **204** |
| Sparse Facade | 1,540 (1.54 µs) | 1,610 (1.61 µs) |

### Column Scans
| Method | Scalar Keys (ns) | Compound Map Keys (ns) |
| :--- | :--- | :--- |
| **Nested Dual Index** | **917** | **1,000** |
| Sparse Facade | 8,690 (8.69 µs) | 9,380 (9.38 µs) |
| Row-only Nested Index | 49,100 (49.1 µs) | 86,800 (86.8 µs) |

### Key Findings
- For both row and column scans, the **Nested Index** strategies outperform the **Sparse Facade** by approximately **10x**.
- For column scans, using a **Row-only Nested Index** is extremely inefficient (approx. **50x-80x slower** than a Dual Index) because it forces a full row scan for each column.

---

## 3. Construction Performance
*Goal: Initializing the data structures from a list of entries.*

| Method | Scalar Keys (ms) | Compound Map Keys (ms) |
| :--- | :--- | :--- |
| Nested Row Index | 5.32 | 6.25 |
| Nested Dual Index | 11.5 | 10.3 |
| **Sparse COO/CSR/CSC** | **104.0** | **117.0** |

### Key Findings
- Constructing the **CSR/CSC** structure via COO ingest is the most expensive operation, taking roughly **10x to 20x longer** than building nested index structures.

## Future Optimization: CSR/CSC Construction

The current bottleneck in CSR/CSC construction is the intermediate COO stage, which involves high memory pressure and significant JVM overhead due to object boxing (`Integer`, `Double`) and the use of `ArrayList<Object>`.

To optimize this, we are investigating three primary strategies:

### 1. The "Packed-Long" Sort (Optimized In-Memory)
This approach avoids object boxing by packing the coordinate pair $(row, col)$ into a single primitive `long` value: `(row << 32) | col`.
- **Mechanism:** Use a primitive `long[]` for keys and a primitive `double[]` (or `long[]`) for payloads. Implement/use a primitive dual-pivot quicksort that swaps both arrays in parallel.
- **Benefit:** Drastically reduces GC pressure and removes the $O(N)$ overhead of `Integer` and `Map.Entry` objects.

### 2. The "Two-Pass" Streaming Construction (Zero-Overhead)
If the input data is indexable (e.g., a `vector` or an array), we can bypass the intermediate COO storage entirely.
- **Mechanism:**
    - **Pass 1 (Counting):** Iterate the input to calculate row frequencies (row counts).
    - **Prefix Sum:** Convert row counts into `row_ptr` offsets.
    - **Pass 2 (Filling):** Re-iterate the input, using the `row_ptr` as a cursor to place values directly into `col_indices` and `values` arrays.
- **Benefit:** Requires $O(\text{num\_rows})$ extra space instead of $O(\text{NNZ})$, making it extremely memory-efficient for large datasets.

### 3. The "Sorted-Map" / B-Tree Approach (For Streaming)
For true single-pass streaming where the dataset size is unknown and cannot be re-read.
- **Mechanism:** Use a specialized primitive-friendly B-tree or Skip-List to maintain order as data arrives.
- **Benefit:** Supports infinite data streams.
- **Trade-off:** Higher per-element overhead compared to batch-oriented methods.


### Performance Tiering
1.  **Tier 1 (Optimal):** 
    - Use **Generated Sparse API** for maximum point-lookup speed.
    - Use **Nested Index** (Row/Dual) for optimal scan speeds.
2.  **Tier 2 (Convenience):** 
    - The **Sparse Facade** is useful for general-purpose code but incurs a ~10x performance penalty on scans and a ~6x penalty on lookups.

### Recommendations
- **Application Design:** If your application is bottlenecked by data access or iteration, avoid the `sparse facade` and implement the data structure using the **Generated Sparse API** or **Nested Index** patterns directly.
- **Column Access:** Always use a **Dual Index** if column scans are a frequent operation in your workload; avoid relying on row-only indices for column-wise operations.
- **Data Lifecycle:** Be mindful of the construction cost when using CSR/CSC structures if the data is updated frequently.
