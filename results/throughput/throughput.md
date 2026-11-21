# Homework 3 — serial vs concurrent, original vs optimized

Same fixed stream of 10000 transactions in every run (streamSeed fixed), 5 repetitions each, median shown. Speed-up is relative to the trivial serial execution with the original client. Processors reported by the JVM: 8.

| Client | Execution | Workers | Makespan (s) | Throughput (txn/s) | Range (txn/s) | Speed-up vs serial | Deadlock retries |
|---|---|---:|---:|---:|---:|---:|---:|
| original | serial | 1 | 7.54 | 1327 | 1314–1376 | 1.00× | 0 |
| original | concurrent | 1 | 7.41 | 1349 | 1277–1405 | 1.02× | 0 |
| original | concurrent | 2 | 4.79 | 2089 | 1976–2303 | 1.57× | 0 |
| original | concurrent | 4 | 3.61 | 2769 | 2693–2831 | 2.09× | 0 |
| original | concurrent | 8 | 3.12 | 3207 | 3047–3402 | 2.42× | 0 |
| original | concurrent | 16 | 3.12 | 3205 | 2962–3354 | 2.42× | 0 |
| optimized | serial | 1 | 6.56 | 1523 | 1250–1540 | 1.15× | 0 |
| optimized | concurrent | 1 | 6.57 | 1522 | 1518–1535 | 1.15× | 0 |
| optimized | concurrent | 2 | 3.99 | 2504 | 2081–2579 | 1.89× | 0 |
| optimized | concurrent | 4 | 3.14 | 3189 | 2942–3231 | 2.40× | 0 |
| optimized | concurrent | 8 | 2.59 | 3858 | 3520–3986 | 2.91× | 0 |
| optimized | concurrent | 16 | 2.48 | 4034 | 3827–4225 | 3.04× | 0 |

## Ablation at 4 workers (original client + one change)

| Variant | Throughput (txn/s) | Range (txn/s) | Change vs original, 4 workers |
|---|---:|---:|---:|
| original | 2769 | 2693–2831 | — |
| autocommit | 2803 | 2751–2875 | +1.2% |
| buffered-out | 2811 | 2791–2833 | +1.5% |
| optimized+async-commit | 3800 | 3709–3822 | +37.3% |
| ssl-off | 3186 | 3174–3273 | +15.1% |
| optimized (all three) | 3189 | 2942–3231 | +15.2% |
