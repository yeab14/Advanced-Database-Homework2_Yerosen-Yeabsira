# Homework 3 — handoff for the group

**Course:** Advanced Database · **Points:** 2 · **Deadline:** 24.11.2026 01:00 (Blackboard)

**Group:** Yeabsira Dereje Alemayhu, Yerosen Hunduma Demise, Jannatul Ferdousi Sohana

Homework 2 (`Blocking`, `RestartBlocking`, `NoLiveLocks`) is unchanged. Homework 3 starts from `src/WorkloadStream.java`.

| Part | Owner | Status |
|---|---|---|
| Task 1 — mixed transactions and the continuous stream | Yeabsira | Done in `WorkloadStream` |
| Task 2 — serial baseline, throughput, thread counts, 30 s demo | Yerosen | Not started. Run `WorkloadStream` and add a serial runner beside it. |
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

A deadlock (`40P01`) or serialization failure (`40001`) rolls the transaction back and repeats the same statements, same ids, same values, until that transaction commits. Other SQL errors stop that worker and are printed in red.

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

Loading the table is outside the timed window. The clock starts when the workers start.

## What the end of a run means

A 30-second run with 4 threads on this machine (PostgreSQL 18, port 5432, client and server on the same host):

```text
[DONE] committed 64244 transactions in 30.014 s (requested window 30 s, plus the in-flight tail).
[DONE] throughput 2140.45 txn/s | read=38208 insert=6459 scan=6511 write=13066 | deadlock retries=0
[DONE] observed mix read=59.5% insert=10.1% scan=10.1% write=20.3% (target 60/10/10/20).
[DONE] equilibrium check: workers=4, peak in-flight=4, in-flight after the tail=0.
```

`30.014 s` is the requested 30 seconds plus the last in-flight transactions. Throughput is commits divided by that wall time. The mix will sit near 60 / 10 / 10 / 20. A few tenths of a percent either way is the dice, not a bug. `deadlock retries=0` is a valid run. Single-tuple writes on 50000 rows rarely deadlock. Retries still happen if Postgres aborts one, and those transactions are still counted once, when they commit.

## Yerosen — serial baseline, threads, demo

Build the serial runner on the same four transaction kinds. One connection, one transaction after another, same `p` rules, same SQL. Include only transaction time, not the initial load, and compare commits per second with `WorkloadStream`.

Suggested runs, same machine, same `hw3.seconds`:

```text
./hw WorkloadStream -Dhw3.threads=1 -Dhw3.seconds=30
./hw WorkloadStream -Dhw3.threads=2 -Dhw3.seconds=30
./hw WorkloadStream -Dhw3.threads=4 -Dhw3.seconds=30
./hw WorkloadStream -Dhw3.threads=8 -Dhw3.seconds=30
```

Use `nproc` as the top of the sweep. The question is whether commits per second grow roughly in proportion to the thread count. The 4-thread line above is one data point, not the whole sweep.

The live demo is about 30 seconds of `./hw WorkloadStream`. The `[stream]` lines and the final `[DONE]` block are what to show. Keep the **Restart cleanup** line from the top of that run. It is the evidence that inserted tuples were removed.

`WorkloadStream` is the concurrent side. A serial class can call the same SQL. If that is awkward, copy the four `commit*` methods and run them on one thread. Do not change the statement text or the write order while you are still measuring the baseline.

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
| `src/ConnectionFactory.java` | JDBC connection from the environment, then `.env`. Strips `\r`. |
| `scripts/run.sh`, `scripts/run.ps1`, `hw` | `./hw WorkloadStream` |
| `README.md` | Homework 2 notes for the instructor. |
| `README-Homework3.md` | This file. |
