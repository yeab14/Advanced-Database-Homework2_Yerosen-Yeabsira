# Homework 3 — handoff for the group

**Course:** Advanced Database · **Points:** 2 · **Deadline:** 24.11.2026 01:00 (Blackboard)

**Group:** Yeabsira Dereje Alemayhu, Yerosen Hunduma Demise, Jannatul Ferdousi Sohana

Homework 2 (`Blocking`, `RestartBlocking`, `NoLiveLocks`) is unchanged. Homework 3 starts from `src/WorkloadStream.java`.

| Part | Owner | Status |
|---|---|---|
| Task 1 — mixed transactions and the continuous stream | Yeabsira | Done in `WorkloadStream` |
| Task 2 — serial baseline, throughput, thread counts, 30 s demo | Yerosen | Done in `SerialWorkloadStream` and the benchmark script below. |
| Task 3 — bottlenecks, a faster version, slides | Jannatul | Not started. Profile `WorkloadStream`, then change a copy or the same class and say what changed. |

The sheet splits the points as 1.0 for the benchmark extension and 1.0 for bottlenecks. The table above is how this group divided that work.

## How to run Yeabsira’s part

From the project folder, with PostgreSQL up and `.env` filled in (`HW2_DB_HOST`, `HW2_DB_PORT`, `HW2_DB_NAME`, `HW2_DB_USER`, `HW2_DB_PASSWORD`):

```text
./hw WorkloadStream
./hw WorkloadStream -Dhw3.threads=4 -Dhw3.seconds=30
```

Windows: `.\hw WorkloadStream`. The same aliases work: `hw3`, `stream`, `workload`.

`./hw all` still runs only the three Homework 2 demos.

| Property | Default | Meaning |
|---|---|---|
| `hw3.threads` | 4 | Workers. This is the concurrency knob for the thread sweep. |
| `hw3.seconds` | 30 | How long new transactions are accepted. In-flight ones finish after that. |
| `hw3.tuples` | 50000 | Base rows loaded at start. Do not raise this unless the load itself is the problem. |
| `hw3.queries` | 3 | Point reads per read transaction, and updates per write transaction. |
| `hw3.seed` | 123456 | Seed for the initial `data_value`s only. Each transaction still draws its own `p`. |
| `hw3.mode` | concurrent | `SerialWorkloadStream` forces `serial` mode and exactly one worker. |

A Windows-edited `.env` can contain `\r`. `ConnectionFactory` strips those characters so the JDBC URL stays valid.

## What each transaction does

Every transaction draws `p` uniformly from `[0, 1)` with `ThreadLocalRandom`. The draw is independent of the other workers.

| `p` | Kind | SQL | Console |
|---|---|---|---|
| `p < 0.6` | Read-only multi-point | One `SELECT data_value FROM blocking_data WHERE data_id = ?` per tuple. Three distinct `data_id`s in `[0, 50000)`. | `read worker … ids=… sum=…` |
| `0.6 ≤ p < 0.7` | Insert | `INSERT INTO blocking_data (data_id, data_value, modified_by) VALUES (?, ?, ?)` | `insert worker … data_id=…` |
| `0.7 ≤ p < 0.8` | Full-table scan | `SELECT sum(data_value) FROM blocking_data` | `scan worker … sum=…` |
| `p ≥ 0.8` | Write | Three `UPDATE … SET data_value = ?, modified_by = ? WHERE data_id = ?` | `write worker … ids=…` |

Reads and writes sample only the base ids `[0, hw3.tuples)`. Inserts use `data_id` starting at `hw3.tuples` (50000, 50001, …). Isolation is **READ COMMITTED**. Autocommit is off. Each worker keeps one connection and reuses its prepared statements.

Write ids stay in the order they were drawn. Do not sort them. The assignment forbids changing write order to force a canonical lock order.

A deadlock (`40P01`) or serialization failure (`40001`) rolls the transaction back and repeats the same statements, same ids, same values, until that transaction commits. Other SQL errors are printed in red and fail the benchmark with a nonzero exit status, so a failed worker cannot silently produce a successful measurement.

## Equilibrium

There is no generator thread and no queue of waiting transactions. `hw3.threads` workers are started. Each worker runs one transaction, commits it, then dices a new `p` and runs the next one. A new transaction is admitted only because the previous one on that worker committed.

So while the window is open:

- in-flight transactions stay at the worker count
- transactions started and transactions committed stay matched (plus at most one unfinished transaction per worker at the end)

That match is how the stream was built. It is not a rate that was tuned with sleeps. The `[stream]` line, printed about every 5 seconds, shows `in-flight` against the worker count. The last lines show peak in-flight and the commit mix.

## Restart cleanup

Every start of `WorkloadStream` counts rows with `data_id >= hw3.tuples`, drops `blocking_data`, and loads the 50000 base rows again. Inserts from the previous process are gone. The first line of a run says how many were removed, for example:

```text
Restart cleanup: removed 6459 inserted tuple(s) from the previous run (data_id >= 50000), then reloaded 50000 base rows.
```

The timer starts once all workers have opened their connections and prepared their statements. Loading the table and setting up workers are outside the measured time.

## What the end of a run means

A 30-second run with 4 threads on this machine (PostgreSQL 18, port 5432, client and server on the same host):

```text
[DONE] committed 64244 transactions in 30.014 s (requested window 30 s, plus the in-flight tail).
[DONE] throughput 2140.45 txn/s | read=38208 insert=6459 scan=6511 write=13066 | deadlock retries=0
[DONE] observed mix read=59.5% insert=10.1% scan=10.1% write=20.3% (target 60/10/10/20).
[DONE] equilibrium check: workers=4, peak in-flight=4, in-flight after the tail=0.
```

`30.014 s` is the requested 30 seconds plus the last in-flight transactions. Throughput is commits divided by that wall time. The mix will sit near 60 / 10 / 10 / 20. A few tenths of a percent either way is the dice, not a bug. `deadlock retries=0` is a valid run. Single-tuple writes on 50000 rows rarely deadlock. Retries still happen if Postgres aborts one, and those transactions are still counted once, when they commit.

The output above is from Yeabsira's Task 1 run. Task 2 results are listed below.

## Yerosen — serial baseline, threads, demo

`SerialWorkloadStream` runs Yeabsira's `Blocker` with one worker and one connection. Each transaction commits before the next starts. Transaction generation, SQL, and retries use the existing methods.

Run from the project folder with the same database settings as Task 1:

```bash
./hw SerialWorkloadStream -Dhw3.seconds=30
./hw WorkloadStream -Dhw3.threads=4 -Dhw3.seconds=30
```

Windows uses `.\hw SerialWorkloadStream` and `.\hw WorkloadStream` with the same Java options. `./hw all` still runs only Homework 2.

The measured time includes transaction generation, SQL execution, retries, console output, and finishing current transactions. A worker failure stops the benchmark with a nonzero exit status.

The final `RESULT` line records the worker count, elapsed time, commits, transaction mix, and retries. Throughput is commits divided by elapsed seconds. The final in-flight count must be zero. The serial run's peak must not exceed one.

To collect the serial baseline and compare worker counts:

```bash
./scripts/benchmark_hw3_task2.sh
```

The script runs three repetitions of serial execution and concurrent execution at 1, 2, 4, and 8 workers. Each run admits work for 10 seconds, using 50,000 base rows, three queries per point-read/write transaction, and initial-data seed 123456. Complete transaction output is saved in `results/throughput/throughput-logs/`; measurements are saved in [results/throughput/throughput.csv](results/throughput/throughput.csv).

Results are in [results/throughput/throughput.md](results/throughput/throughput.md). Update the table after rerunning the benchmark. Each run draws new transactions with `ThreadLocalRandom`; the seed controls only the initial data.

For linear scaling, doubling workers would approximately double throughput. The CSV records the available processor count separately from the worker count. Compare the one-worker concurrent run with the serial run as well.

For the live demo, use the 30-second concurrent command above. Show the **Restart cleanup** line, transaction output, the `[stream]` progress lines, and the final `[DONE]` summary. At 30 seconds, new work stops and current transactions finish. Compilation and setup add to the complete command’s wall time.

## Jannatul — bottlenecks and the talk

Profile the running stream, not the `CREATE TABLE` / bulk insert in `createDB`. The assignment says connection setup and the initial load are out of scope. Time spent building any batches of non-conflicting transactions counts as run time.

Places that showed up while writing Task 1, and that are allowed to change if you can explain them:

- Every read, insert, scan, and write prints a line. That console lock is part of the specified output for Task 1. A throughput run can still be dominated by printing. If you reduce logging, say so on the slides and keep one setting that still prints the sums the sheet asks for.
- Full-table scans read all 50000 rows (and any inserts committed so far) under READ COMMITTED.
- Each worker holds one JDBC connection for the whole window. Do not open a new connection per transaction unless you are measuring that on purpose.
- Write ids are not sorted. Leave that alone unless the slide explains it as a deliberate experiment. The sheet says not to reorder writes to provoke canonical locking.

Postgres logging and `EXPLAIN` on the four statements are in scope. The table definition and the four queries should stay the same. Client and server should stay on one machine.

Slides need the idea of the stream, the problems you actually hit, the serial-versus-concurrent numbers, and the contribution split. The division we are using is about 33% each: Yeabsira (this stream), Yerosen (serial baseline, thread sweep, demo), Jannatul (bottlenecks, improvements, slides).

## Files

| File | Role |
|---|---|
| `src/WorkloadStream.java` | Task 1. Class `WorkloadStream`, inner class `Blocker`. |
| `src/SerialWorkloadStream.java` | Yerosen's Task 2 serial entry point; reuses `WorkloadStream` with exactly one worker. |
| `scripts/benchmark_hw3_task2.sh` | Repeated serial/concurrent worker sweep with complete per-run logs. |
| `src/ConnectionFactory.java` | JDBC connection from the environment, then `.env`. Strips `\r`. |
| `scripts/run.sh`, `scripts/run.ps1`, `hw` | `./hw WorkloadStream` |
| `README.md` | Homework 2 notes for the instructor. |
| `README-Homework3.md` | This file. |
