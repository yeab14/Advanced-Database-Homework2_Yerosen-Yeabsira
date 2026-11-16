#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

RESULTS="${1:-results/throughput/throughput.csv}"
SECONDS_PER_RUN="${HW3_SECONDS:-10}"
REPEATS="${HW3_REPEATS:-3}"
THREAD_COUNTS="${HW3_THREADS:-1 2 4 8}"
if [[ ! "$SECONDS_PER_RUN" =~ ^[1-9][0-9]*$ || ! "$REPEATS" =~ ^[1-9][0-9]*$ ]]; then
  echo "HW3_SECONDS and HW3_REPEATS must be positive integers." >&2
  exit 1
fi
for threads in $THREAD_COUNTS; do
  if [[ ! "$threads" =~ ^[1-9][0-9]*$ ]]; then
    echo "HW3_THREADS must contain positive integers separated by spaces." >&2
    exit 1
  fi
done

LOG_DIR="${RESULTS%.csv}-logs"
mkdir -p "$(dirname "$RESULTS")" "$LOG_DIR"
"$ROOT/scripts/compile.sh"
java -version > "$LOG_DIR/environment.txt" 2>&1
printf 'seconds=%s\nrepeats=%s\nconcurrent_workers=%s\n' "$SECONDS_PER_RUN" "$REPEATS" "$THREAD_COUNTS" >> "$LOG_DIR/environment.txt"
printf '' > "$RESULTS"

run_benchmark() {
  local mode="$1" threads="$2" repeat="$3" class="WorkloadStream"
  local log="$LOG_DIR/${mode}-${threads}-${repeat}.log"
  if [[ "$mode" == serial ]]; then
    class="SerialWorkloadStream"
  fi
  echo "Running $mode, workers=$threads, repetition=$repeat, window=${SECONDS_PER_RUN}s"
  if ! java -cp 'out:lib/postgresql-42.2.18.jar' \
    "-Dhw3.mode=$mode" "-Dhw3.threads=$threads" "-Dhw3.seconds=$SECONDS_PER_RUN" \
    -Dhw3.tuples=50000 -Dhw3.queries=3 -Dhw3.seed=123456 \
    "$class" > "$log" 2>&1; then
    echo "Benchmark failed; full output: $log" >&2
    tail -n 50 "$log" >&2
    return 1
  fi
  if [[ ! -s "$RESULTS" ]]; then
    sed -n 's/^CSV_HEADER,//p' "$log" > "$RESULTS"
  fi
  sed -n 's/^RESULT,//p' "$log" >> "$RESULTS"
  awk '/^\[DONE\] throughput/ {print}' "$log"
}

for ((repeat = 1; repeat <= REPEATS; repeat++)); do
  run_benchmark serial 1 "$repeat"
  for threads in $THREAD_COUNTS; do
    run_benchmark concurrent "$threads" "$repeat"
  done
done
echo "Results: $RESULTS"
