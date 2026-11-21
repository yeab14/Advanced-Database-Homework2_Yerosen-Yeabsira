# Homework 3, Task 2.2 — bottleneck analysis

Machine: 8 logical CPUs, CPU governor `powersave` (cores idle at about 1.1 GHz), PostgreSQL 18.6 on the same host, OpenJDK 25, pgJDBC 42.2.18. Client and server talk over `127.0.0.1`.

Four tools were used:

1. **Per-kind latency accounting** in `WorkloadStream` (printed as `[DONE] latency per kind`). It measures wall time per transaction kind, including retries.
2. **Java Flight Recorder**: `java -XX:StartFlightRecording=filename=run.jfr,settings=profile …`, then `jfr print --events jdk.ExecutionSample` and `jdk.SocketRead`.
3. **`EXPLAIN (ANALYZE, BUFFERS)`** on the four statements, plus `pg_stat_user_tables` for dead tuples and HOT updates.
4. **Changing one setting at a time** (the ablation in `results/throughput/throughput.md`).

`pg_stat_statements` and `log_min_duration_statement` need superuser rights, and the `hw2` role does not have them. With a superuser they would show the same split per statement.

## Findings

### B1 — The full-table scan dominates (server CPU)

Original client, fixed stream of 10,000 transactions (logs in `results/throughput/throughput-logs/`):

| Kind | Share of transactions | Mean latency, serial | Mean latency, 8 workers | Share of worker busy time |
|---|---:|---:|---:|---:|
| read (3 point SELECTs + COMMIT) | 60% | 0.25 ms | 0.6 ms | about 20% |
| insert | 10% | 0.69 ms | 1.3 ms | about 7–10% |
| **scan `SELECT sum(data_value)`** | **10%** | **3.2 ms** | **10 ms** | **about 47–52%** |
| write (3 UPDATEs + COMMIT) | 20% | 0.84 ms | 2.3 ms | about 22% |

`EXPLAIN (ANALYZE, BUFFERS)`: the point lookup is an index scan on `blocking_data_pkey` with 3 shared-buffer hits, well under 1 ms. The scan is a `Seq Scan` over about 52,000 rows / 343 pages, all in shared buffers. It is pure CPU, not I/O: 3 ms with the clock up, about 10 ms when the cores idle at 1.1 GHz.

With 8 or more workers every kind gets slower. Client threads and PostgreSQL backends share the same 8 CPUs, so the server CPU saturates. That is why throughput stops growing between 8 and 16 workers (3,858 → 4,034 txn/s), and why scaling is sublinear. A 16-worker check with Little's law: 16 / 3.87 ms mean latency ≈ 4,130 txn/s, against 4,034 measured.

Tried and rejected:

- **Parallel sequential scan** (`min_parallel_table_scan_size=0`, 4 workers): slower, 17–19 ms instead of 10 ms. Starting the parallel workers costs more than a 2.7 MB scan saves.
- **Caching the sum on the client, or a materialised summary**: not allowed. The sheet says the query must stay the same.

The remaining fix is concurrency. Scans are read-only MVCC snapshots, so they never block or get blocked. More workers let several scans run on different cores at once, which is why throughput keeps rising up to 8 workers on 8 CPUs.

### B2 — TLS encryption on a localhost connection (client and server CPU)

The JFR execution samples of the client are led by `com.sun.crypto.provider.GHASH.processBlocks` and `CounterMode.implCrypt`, which are AES-GCM. pgJDBC 42.2.x defaults to `sslmode=prefer`, so every message to the server on the same machine is encrypted and decrypted on both sides. Fix: `sslmode=disable` for the localhost connection. This was the single largest client-side gain (see the ablation).

### B3 — Round-trip latency: the client mostly waits

JFR shows the worker threads spending almost all of their time in `jdk.SocketRead`, waiting for the server. Only about 200 CPU samples were taken in 8 s with 4 workers. Every statement is one round trip. Under the original client a read transaction is 4 round trips (3 SELECTs + COMMIT), and an insert or scan is 2 (statement + COMMIT).

Fix: single-statement transactions (insert, scan) run in autocommit mode, so the statement and its COMMIT become one round trip. They are still one transaction each, with the same SQL. The 3-statement read and write transactions keep an explicit COMMIT, because they must stay one transaction.

### B4 — Console output

The original client printed one line per transaction through `System.out`, which flushes on every `println` (one `write` system call per line) and is synchronized across all workers. The sheet requires the sums of read and scan transactions to be printed, so output cannot simply be switched off.

Fix: a 64 KiB `BufferedOutputStream` without autoflush, flushed every 5 s and at the end. Every required sum is still printed. Optional insert and write lines are behind `-Dhw3.verbose=true`.

### B5 — Commit durability: WAL flush per write commit

Each insert and write commit waits for the WAL to be flushed to disk. `SET synchronous_commit TO off` removes that wait and is measured as an extra ablation step. It is **not** part of the optimized profile: after a crash the last few hundred milliseconds of commits could be lost. That violates durability, so we only report it.

### B6 — Deadlock timeout under contention (only in the stress test)

With the real 50,000 rows, deadlocks did not occur (0 retries in every run). When we forced contention (`-Dhw3.tuples=6`, 8 workers, 2,000 transactions), every transaction still committed, with at most 3 aborts per transaction because of escalation. Throughput, however, fell to about 8 txn/s. Each deadlock is detected only after `deadlock_timeout = 1 s`. Lowering that setting needs superuser rights.

## Unexpected results

- **The same serial run measured 417, 480 or 1,336 txn/s** in our first benchmark, which had no warm-up. With the `powersave` governor, a single connection that ping-pongs with the server leaves the cores idle most of the time, so they can stay at 1.1 GHz. Every round trip and every scan then costs about 3× more. In that state, concurrent runs looked superlinear (up to 8× with 8 workers), only because they pushed the clock up. After one discarded warm-up run the serial baseline was stable (1,314–1,376 txn/s), and the real speed-up turned out to be sublinear. Results without a warm-up should not be trusted on this laptop.
- **Sublinear scaling and an early plateau.** With 8 workers, the original client reaches 2.4× serial, not 8×. Client and server compete for the same 8 cores, and half of the busy time is the CPU-bound scan. 16 workers add almost nothing.
- **Disabling logging alone gained little** (+1.5%). Version 1 of the Task 3 slides assumed logging was the bottleneck; the profiler showed TLS was the larger client-side cost (+15%).
- **The largest single gain is not a client change:** `synchronous_commit=off` gives +37% at 4 workers. We still leave it out of the optimized profile, because it gives up durability.
