#!/usr/bin/env bash
# Homework 3 benchmark: the same fixed stream of transactions, executed
#   - serially (one worker, one transaction at a time) and
#   - concurrently with 1, 2, 4, 8, 16 workers,
# for the original (Task 1) client and the optimized (Task 3) client,
# plus a one-optimization-at-a-time ablation at 4 workers.
#
#   ./scripts/benchmark_hw3_task2.sh                 # defaults below
#   HW3_TRANSACTIONS=20000 HW3_REPEATS=5 ./scripts/benchmark_hw3_task2.sh
#
# Throughput = committed transactions / makespan of the stream. Loading the
# table and opening connections are outside the measured time.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

RESULTS="${1:-results/throughput/throughput.csv}"
TRANSACTIONS="${HW3_TRANSACTIONS:-10000}"
REPEATS="${HW3_REPEATS:-5}"
THREAD_COUNTS="${HW3_THREADS:-1 2 4 8 16}"
PROFILES="${HW3_PROFILES:-original optimized}"
ABLATION="${HW3_ABLATION:-1}"
for n in "$TRANSACTIONS" "$REPEATS" $THREAD_COUNTS; do
  if [[ ! "$n" =~ ^[1-9][0-9]*$ ]]; then
    echo "HW3_TRANSACTIONS, HW3_REPEATS and HW3_THREADS must be positive integers." >&2
    exit 1
  fi
done

LOG_DIR="${RESULTS%.csv}-logs"
mkdir -p "$(dirname "$RESULTS")" "$LOG_DIR"
"$ROOT/scripts/compile.sh"
{
  java -version 2>&1
  psql --version 2>/dev/null || true
  echo "processors=$(nproc 2>/dev/null || echo ?)"
  echo "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo ?)"
  printf 'transactions=%s\nrepeats=%s\nworkers=%s\nprofiles=%s\n' "$TRANSACTIONS" "$REPEATS" "$THREAD_COUNTS" "$PROFILES"
} > "$LOG_DIR/environment.txt"
printf '' > "$RESULTS"

# run_benchmark <label> <mode> <threads> <repeat> [extra -D options...]
run_benchmark() {
  local label="$1" mode="$2" threads="$3" repeat="$4"
  shift 4
  local class="WorkloadStream"
  local log="$LOG_DIR/${label}-${mode}-${threads}-${repeat}.log"
  [[ "$mode" == serial ]] && class="SerialWorkloadStream"
  echo "Running $label $mode, workers=$threads, repetition=$repeat, stream=$TRANSACTIONS transactions"
  if ! java -cp 'out:lib/postgresql-42.2.18.jar' \
    "-Dhw3.mode=$mode" "-Dhw3.threads=$threads" "-Dhw3.transactions=$TRANSACTIONS" \
    -Dhw3.tuples=50000 -Dhw3.queries=3 -Dhw3.seed=123456 "$@" \
    "$class" > "$log" 2>&1; then
    echo "Benchmark failed; full output: $log" >&2
    tail -n 50 "$log" >&2
    return 1
  fi
  if [[ ! -s "$RESULTS" ]]; then
    printf 'label,%s\n' "$(sed -n 's/^CSV_HEADER,//p' "$log")" > "$RESULTS"
  fi
  printf '%s,%s\n' "$label" "$(sed -n 's/^RESULT,//p' "$log")" >> "$RESULTS"
  awk '/^\[DONE\] throughput/ {print}' "$log"
}

# One discarded run first: JIT, page cache and CPU clocks start from the same warm state.
echo "Warm-up run (not recorded)"
java -cp 'out:lib/postgresql-42.2.18.jar' -Dhw3.threads=4 "-Dhw3.transactions=$TRANSACTIONS" WorkloadStream > "$LOG_DIR/warmup.log" 2>&1

for ((repeat = 1; repeat <= REPEATS; repeat++)); do
  for profile in $PROFILES; do
    run_benchmark "$profile" serial 1 "$repeat" "-Dhw3.profile=$profile"
    for threads in $THREAD_COUNTS; do
      run_benchmark "$profile" concurrent "$threads" "$repeat" "-Dhw3.profile=$profile"
    done
  done
  if [[ "$ABLATION" == 1 ]]; then
    # Start from the original client and switch on one optimization at a time.
    run_benchmark "abl-ssl-off" concurrent 4 "$repeat" -Dhw3.profile=original -Dhw3.sslmode=disable
    run_benchmark "abl-buffered-out" concurrent 4 "$repeat" -Dhw3.profile=original -Dhw3.bufferedOutput=true -Dhw3.verbose=false
    run_benchmark "abl-autocommit" concurrent 4 "$repeat" -Dhw3.profile=original -Dhw3.autocommitSingle=true
    # Not part of the optimized profile: trades durability of the last commits for latency.
    run_benchmark "abl-optimized+async-commit" concurrent 4 "$repeat" -Dhw3.profile=optimized -Dhw3.asyncCommit=true
  fi
done
echo "Results: $RESULTS"
python3 "$ROOT/scripts/summarize_hw3.py" "$RESULTS" > "${RESULTS%.csv}.md"
echo "Summary: ${RESULTS%.csv}.md"
