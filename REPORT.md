## Evaluated two implementations for TPC-H Query 5:

1️. Row-based storage using map<string,string> per row(Code for this is not pushed)
2️. Structure-of-Arrays (SoA) columnar storage with integer-based processing

All tests were executed on the same machine and dataset(dbgen -s 2)


| Configuration                   | Data Loading Time | Query Execution Time | Total Time |
| ------------------------------- | ----------------- | -------------------- | ---------- |
| **Row-based (Maps), 1 Thread**  | ~323 sec          | ~82 sec              | ~405 sec   |
| **Row-based (Maps), 2 Threads** | ~548 sec          | ~86 sec              | ~635 sec   |
| **SoA, 1 Thread**               | ~38 sec           | ~2.5 sec             | ~41 sec    |
| **SoA, 2 Threads**              | ~27 sec           | ~1.7 sec             | ~29 sec    |
| **SoA, 4 Threads**              | ~35 sec           | ~1.0 sec             | ~36 sec    |


Results produced by SoA implementation

INDONESIA 115979499.65
CHINA     109568736.22
INDIA     106258458.17
JAPAN     104738341.03
VIETNAM    98052109.13

## Key Observations

Total runtime improved ~15× overall
Query execution accelerated ~40–80×
Data loading accelerated ~10×


Query execution scales well with additional threads because it is CPU-bound and parallelized cleanly across independent data partitions. In contrast, data loading becomes limited by disk throughput and memory bandwidth; beyond 2 threads, thread coordination overhead outweigh parallel benefits, leading to slower load performance at 4 threads.

## Why SoA Is Faster

1. Row-based (maps)

   1. Heavy string parsing

   2. Multiple heap allocations per row

   3. Hash lookups for every column access

   4. Poor cache locality

2. SoA

    1. Contiguous column vectors

    2. Native typed columns (int, double, string)

    3. Cheap integer joins instead of string compares

    4. CPU-friendly, cache-efficient, parallel streaming execution




## Final Outcome

By redesigning storage from row-oriented maps to columnar SoA, and converting joins to operate on numeric keys:

    The system now behaves like a lightweight analytical engine
    Query execution is near-instant (~1–2 seconds vs ~80 seconds)
    Loading performance improved dramatically
    Parallel execution is efficient and scalable
    This demonstrates that data layout and execution model matter more than raw compute for analytical workloads like TPC-H.




## Parallelization Strategy For loading Data (Parallel File Chunk Reading)

    Input .tbl files are divided into byte-range chunks.

    Each thread independently parses its chunk and appends to a thread-local Structure-of-Arrays (SoA) instance.

    After all threads complete, thread-local SoA blocks are merged into the global table.

    This avoids shared writes, locks, and false sharing.

    Benefit: High throughput ingestion, efficient memory writes, no contention.



## Query Execution and Parallelization Strategy
The implementation follows a pipeline-based parallel execution model for TPC-H Query 5. Instead of executing a monolithic multi-table join, the query is decomposed into logically independent stages. Each stage builds only the data relationships needed for the next, and each stage is executed in parallel using thread-local structures to avoid synchronization overhead.
The query computes revenue per nation for orders:
    placed in a specific region
    within a given date range
    where customer nation matches supplier nation

Execution proceeds as a structured pipeline:
Region
 → Nations in region
 → Suppliers from those nations
 → Customers and their nations
 → Orders in date range + nation propagation
 → LineItems revenue contribution
 → Per-nation aggregation

### Parallel Execution Design

Each stage is parallelized using partitioned workloads and thread-local maps, followed by a single merge step. This avoids contention and enables efficient CPU utilization.

1. Region & Nation Resolution (Single Thread)

    Region and nation tables are tiny, so they are processed serially:

        resolve regionkey from region name

        build nationkey → nation_name for nations in that region

        This forms the scope for the rest of the pipeline.

2. Supplier Filtering (Parallel)

    Suppliers are partitioned across threads.
    Each thread filters only suppliers whose nation belongs to the selected region and builds:

        suppkey → nationkey

        Thread-local maps are merged at the end.

        Ensures only suppliers in the region participate downstream.

3. Customer Mapping (Parallel)

    Customers are also partitioned across threads.
    Each builds:
        custkey → nationkey

        Merged into a global customer nationality map.

        No filtering required here; correctness is enforced later via matching with filtered suppliers.

4. Order Filtering + Nation Propagation (Parallel)

    Threads process disjoint order partitions:

    apply date filter

    attach nationality derived from customer

    Result:

        orderkey → nationkey

        Only relevant orders progress further.

5.  LineItem Revenue Computation (Parallel)

    Lineitems are processed in parallel and joined via in-memory lookups:

    For each lineitem:
    1️. Retrieve order nationality
    2️. Retrieve supplier nationality
    3️. Enforce:
        order nation == supplier nation
    4️. Compute revenue:
        extendedprice × (1 − discount)
    5️. Accumulate into thread-local:
        nation_name → revenue

    At the end, thread results are merged into the final aggregation.

Test:
Tables folder was created in the same root directory.
and query5_result.txt also exist
tpch_qsuery5 --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 1 --table_path \tpch-query5\tables --result_path tpch-query5    