# Task 2 — serial and concurrent throughput

Environment: Java 21.0.12.1, PostgreSQL 16.15, JDBC 42.2.18. The JVM reported four available processors.

Each configuration was run three times for 10 seconds, starting with 50,000 rows. Transaction probabilities and SQL were the same for every run. Output was redirected to log files. Timing started after loading the data and opening the connections, and ended after the remaining transactions finished.

| Execution | Workers | Median transactions/s | Observed range | Speedup vs serial |
|---|---:|---:|---:|---:|
| Serial | 1 | 140.29 | 122.14–184.88 | 1.00× |
| Concurrent | 1 | 174.79 | 151.92–203.79 | 1.25× |
| Concurrent | 2 | 233.97 | 209.48–254.07 | 1.67× |
| Concurrent | 4 | 422.53 | 390.92–428.18 | 3.01× |
| Concurrent | 8 | 891.35 | 771.37–926.07 | 6.35× |

Eight workers reached a median of 891.35 transactions/s, or 6.35× the serial result. Ideal linear scaling would give an 8× speedup.

Serial and one-worker concurrent execution use the same code. Each run has different random transactions, and machine load can change between tests, so their measured rates differ.

Raw measurements: [throughput.csv](throughput.csv). Complete per-run output and Java version are kept locally in `throughput-logs/`.
