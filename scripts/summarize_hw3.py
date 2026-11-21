#!/usr/bin/env python3
"""Turn the benchmark CSV into the Markdown tables used in the slides."""
import csv
import statistics
import sys
from collections import defaultdict

rows = list(csv.DictReader(open(sys.argv[1])))
groups = defaultdict(list)
for r in rows:
    if int(r["in_flight"]) != 0 or int(r["committed"]) != int(r["transactions"]):
        sys.exit(f"incomplete run in CSV: {r}")
    groups[(r["label"], r["mode"], int(r["threads"]))].append(r)

def rng(key):
    v = [float(r["throughput"]) for r in groups[key]]
    return f"{min(v):.0f}–{max(v):.0f}"

def med(key, field="throughput"):
    return statistics.median(float(r[field]) for r in groups[key])

baseline = med(("original", "serial", 1))
n = rows[0]["transactions"]
reps = len(groups[("original", "serial", 1)])
print(f"# Homework 3 — serial vs concurrent, original vs optimized\n")
print(f"Same fixed stream of {n} transactions in every run (streamSeed fixed), {reps} repetitions each, "
      f"median shown. Speed-up is relative to the trivial serial execution with the original client. "
      f"Processors reported by the JVM: {rows[0]['processors']}.\n")
print("| Client | Execution | Workers | Makespan (s) | Throughput (txn/s) | Range (txn/s) | Speed-up vs serial | Deadlock retries |")
print("|---|---|---:|---:|---:|---:|---:|---:|")
for label in ("original", "optimized"):
    keys = sorted((k for k in groups if k[0] == label), key=lambda k: (k[1] != "serial", k[2]))
    for k in keys:
        tp = med(k)
        retries = sum(int(r["retries"]) for r in groups[k])
        print(f"| {label} | {k[1]} | {k[2]} | {med(k, 'elapsed_seconds'):.2f} | {tp:.0f} | "
              f"{rng(k)} | {tp / baseline:.2f}× | {retries} |")

abl = [k for k in groups if k[0].startswith("abl-")]
if abl:
    ref = med(("original", "concurrent", 4))
    print("\n## Ablation at 4 workers (original client + one change)\n")
    print("| Variant | Throughput (txn/s) | Range (txn/s) | Change vs original, 4 workers |")
    print("|---|---:|---:|---:|")
    print(f"| original | {ref:.0f} | {rng(('original', 'concurrent', 4))} | — |")
    for k in sorted(abl):
        tp = med(k)
        print(f"| {k[0][4:]} | {tp:.0f} | {rng(k)} | {100 * (tp / ref - 1):+.1f}% |")
    opt = med(("optimized", "concurrent", 4))
    print(f"| optimized (all three) | {opt:.0f} | {rng(('optimized', 'concurrent', 4))} | {100 * (opt / ref - 1):+.1f}% |")
