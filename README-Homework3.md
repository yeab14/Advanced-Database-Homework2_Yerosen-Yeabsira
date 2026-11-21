# Homework 3 — Homework 2 revisited

**Course:** Advanced Database · **Points:** 2 · **Deadline:** 24.11.2026 01:00 (Blackboard)

**Group:** Yeabsira Dereje Alemayhu, Yerosen Hunduma Demise, Jannatul Ferdousi Sohana

Homework 2 (`Blocking`, `RestartBlocking`, `NoLiveLocks`) is unchanged. Homework 3 lives in `src/WorkloadStream.java` and `src/SerialWorkloadStream.java`.

## Where each requirement is met

| Sheet item | Where |
|---|---|
| 1.1 mixed transactions (60/10/10/20) | `WorkloadStream.Txn`, `kindOf`, `commitRead/Insert/Scan/Write` |
| 1.1c remove inserted tuples on restart | `createDB()` drops and reloads `blocking_data` and prints `Restart cleanup: removed N …` |
| 1.2 continuous stream + equilibrium | Closed loop of W workers. See [Equilibrium](#equilibrium) |
| 1.3 throughput + thread sweep | `scripts/benchmark_hw3_task2.sh` → `results/throughput/throughput.md` |
| 1.4 30 s live demo | [Live demo](#live-demo-30-s) |
| 2.1 serial execution + comparison | `SerialWorkloadStream` (one worker, one transaction at a time), same stream |
| 2.2 bottleneck analysis | `docs/bottleneck-analysis.md` (JFR, EXPLAIN, per-kind latency, ablation) |
| 2.3 bottlenecks solved | `hw3.profile=optimized` (default): TLS off on localhost, buffered output, autocommit for single-statement transactions |
| 2.4 improved vs serial, slides | `results/throughput/throughput.md`, `docs/Advanced_Database_HW3_Group_Presentation.pptx` |
| every transaction eventually commits | Retry + random backoff + exclusive escalation after 3 aborts. See [No starvation](#no-starvation) |

## Running

From the project folder, with PostgreSQL up and `.env` filled in (`HW2_DB_HOST`, `HW2_DB_PORT`, `HW2_DB_NAME`, `HW2_DB_USER`, `HW2_DB_PASSWORD`):

```bash
./hw WorkloadStream                                        # 4 workers, 30 s window, optimized client
./hw SerialWorkloadStream -Dhw3.transactions=10000         # serial baseline on the fixed stream
./hw WorkloadStream -Dhw3.threads=8 -Dhw3.transactions=10000
./scripts/benchmark_hw3_task2.sh                           # full benchmark, about 15 min
```

Windows: `.\hw WorkloadStream` with the same `-D` options.

| Property | Default | Meaning |
|---|---|---|
| `hw3.threads` | 4 | Workers, the concurrency knob for the thread sweep. `SerialWorkloadStream` forces 1. |
| `hw3.seconds` | 30 | Time-window mode: how long new transactions are admitted. |
| `hw3.transactions` | 0 | If > 0: run exactly the first N transactions of the stream and measure the makespan. |
| `hw3.streamSeed` | 20261124 | Transaction *i* is generated from (seed, *i*), so every run sees the same stream. |
| `hw3.tuples` | 50000 | Base rows, same as Homework 2. Not increased. |
| `hw3.queries` | 3 | Point reads per read transaction and updates per write transaction. |
| `hw3.seed` | 123456 | Seed for the initial `data_value`s. |
| `hw3.profile` | optimized | `original` = the Task 1 client before the Task 3 optimizations. |
| `hw3.sslmode`, `hw3.bufferedOutput`, `hw3.autocommitSingle` | from the profile | Switch one optimization individually, for the ablation. |
| `hw3.asyncCommit` | false | `synchronous_commit=off`. Measured only, not used: it gives up durability. |
| `hw3.verbose` | `true` for original, `false` for optimized | Also print insert and write lines. Read and scan sums are **always** printed. |
| `hw3.escalateAfter` | 3 | Aborts before a write transaction runs exclusively. |

## What each transaction does

Transaction *i* draws `p` uniformly from `[0, 1)` with `SplittableRandom(streamSeed + i·φ)`. Draws are independent across transactions. Because they depend only on *i*, the serial run and every concurrent run execute exactly the same transactions (checked: identical per-kind counts and identical read ids in every run).

| `p` | Kind | SQL | Console |
|---|---|---|---|
| `p < 0.6` | Read-only multi-point | 3 × `SELECT data_value FROM blocking_data WHERE data_id = ?`, distinct ids, then COMMIT | `read worker … ids=… sum=…` (always) |
| `0.6 ≤ p < 0.7` | Insert | `INSERT INTO blocking_data (data_id, data_value, modified_by) VALUES (?, ?, ?)`, `data_id = 50000 + i` | `insert worker …` (verbose) |
| `0.7 ≤ p < 0.8` | Full-table scan | `SELECT sum(data_value) FROM blocking_data` | `scan worker … sum=…` (always) |
| `p ≥ 0.8` | Write | 3 × `UPDATE … SET data_value = ?, modified_by = ? WHERE data_id = ?`, each must touch exactly 1 row, then COMMIT | `write worker …` (verbose) |

Isolation is READ COMMITTED. Write ids keep the order in which they were drawn; they are never sorted (the sheet forbids canonical lock ordering).

## Equilibrium

The stream is a closed loop. There is no generator thread and no queue. W workers each run one transaction, wait for its commit, then take the next transaction *i* from a shared counter. A new transaction is admitted only because one just committed, so:

- in-flight transactions stay at W (the run prints `peak in-flight = W` and `in-flight after the tail = 0`), and
- arrivals equal commits at every moment, give or take the W in flight.

We found the equilibrium by construction rather than by tuning an arrival rate. An open system, where a generator produces transactions at rate λ, is stable only if λ is below the capacity; otherwise the queue grows without bound. A closed loop always runs at capacity. By Little's law its throughput is W / (mean latency), and we verified that relation against the measured per-kind latencies. The `[stream]` line every 5 s shows `in-flight=W/W`.

## No starvation

Reads and scans are MVCC snapshots and never wait for locks. Inserts use fresh keys. Only writes take conflicting row locks, so only writes can deadlock. A deadlock victim (`40P01` / `40001`):

1. rolls back and sleeps a random 1–40 ms, so the losers do not restart in lockstep;
2. retries the **same** statements with the same ids, values and order;
3. after 3 aborts, takes a fair `ReentrantReadWriteLock` in exclusive mode. Every other write transaction holds it in shared mode, so the escalated transaction runs while no other write is active, cannot be part of a deadlock cycle, and commits.

So every transaction commits after at most 3 aborts. Stress test with 8 workers on only 6 rows (`-Dhw3.tuples=6 -Dhw3.transactions=2000`; fewer rows, used only to provoke deadlocks): all 2,000 committed, 386 deadlock retries, 65 escalations, **max 3 aborts for any transaction**. With the real 50,000 rows we measured 0 deadlocks in all 80 benchmark runs. Any other SQL error stops the benchmark with a nonzero exit status.

## Results (this machine: 8 CPUs, PostgreSQL 18.6, OpenJDK 25)

Full table and ranges: [results/throughput/throughput.md](results/throughput/throughput.md). Fixed stream of 10,000 transactions, 5 repetitions, median, after one discarded warm-up run.

| | Workers | txn/s | vs serial (original) |
|---|---:|---:|---:|
| Serial, original client | 1 | 1,327 | 1.00× |
| Concurrent, original | 4 | 2,769 | 2.09× |
| Concurrent, original | 8 | 3,207 | 2.42× |
| Serial, optimized | 1 | 1,523 | 1.15× |
| Concurrent, optimized | 4 | 3,189 | 2.40× |
| Concurrent, optimized | 8 | 3,858 | 2.91× |
| Concurrent, optimized | 16 | 4,034 | 3.04× |

Scaling is sublinear: doubling workers gives +57%, +32% and +16% for the original client. Client and server share 8 cores, and half of the busy time is the CPU-bound full-table scan. Bottlenecks and the "unexpected results" (CPU frequency scaling made cold serial runs about 3× slower) are explained in [docs/bottleneck-analysis.md](docs/bottleneck-analysis.md).

The results from the earlier Task 2 run on another machine (4 CPUs, PostgreSQL 16) are kept as `results/throughput/throughput-task2-old-machine.*`. They used a 10 s window and no fixed stream, so they are not compared with the numbers above.

## Live demo (30 s)

```bash
./hw WorkloadStream -Dhw3.threads=4 -Dhw3.seconds=30
```

Show:

1. the `Restart cleanup: removed N inserted tuple(s)` line;
2. read and scan lines with their sums;
3. the `[stream] in-flight=4/4 committed=…` line every 5 s;
4. the final `[DONE]` block: throughput, mix ≈ 60/10/10/20, 0 retries, latency per kind (scan ≈ 50% of busy time), and `peak in-flight=4, in-flight after the tail=0`.

For contrast, run `./hw SerialWorkloadStream -Dhw3.seconds=30`.

## Files

| File | Role |
|---|---|
| `src/WorkloadStream.java` | Tasks 1 and 3: stream, kinds, starvation-free retry, optimizations, reporting |
| `src/SerialWorkloadStream.java` | Task 2: serial execution (one worker) on the same stream |
| `src/ConnectionFactory.java` | JDBC connection from env / `.env`, with optional extra driver properties |
| `scripts/benchmark_hw3_task2.sh` | Serial + 1/2/4/8/16 workers × original/optimized + ablation, with a warm-up |
| `scripts/summarize_hw3.py` | CSV → `results/throughput/throughput.md` |
| `docs/bottleneck-analysis.md` | Task 2.2 evidence and the unexpected results |

## Contributions

About 33% each: Yeabsira (Task 1 stream), Yerosen (serial baseline, thread sweep, demo), Jannatul (bottleneck analysis, optimization, slides).
