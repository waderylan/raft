#!/bin/bash
set -eo pipefail

REPO=~/lab1-ozzie
BUILD=$REPO/build
APP=$BUILD/app
TPUT=$APP/tput
RESULTS_DIR=$APP/benchmark_results
TPUT_TIMEOUT=150

CURRENT_BRANCH="lab-3-solution-ddl2"
CURRENT_COMMIT=$(git -C $REPO rev-parse $CURRENT_BRANCH)
BASELINE_COMMIT="6246cfaa1bdbaea53716b75a83d155d78cf6e481"
LEASE_FIX_COMMIT="01f356366db9edfe4a3d83b95ba32b370c048514"
PARALLEL_AE_COMMIT="ce5c5e3f25820e05858a6e068fd0fb834a1b05aa"

# Capture the current branch so we can return to it at the end
ORIGINAL_BRANCH=$(git -C $REPO symbolic-ref --short HEAD 2>/dev/null || echo "")
if [ -z "$ORIGINAL_BRANCH" ]; then
  echo "ERROR: not on a branch (detached HEAD). Please checkout a branch first." >&2
  exit 1
fi

mkdir -p $RESULTS_DIR

run_tput() {
  local outfile=$1
  local put_ratio=$2
  cd $APP
  pkill -f kv_node || true
  sleep 1
  timeout $TPUT_TIMEOUT $TPUT 64 $put_ratio | tee $outfile
  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: tput 64 $put_ratio timed out or failed (limit: ${TPUT_TIMEOUT}s)" >&2
    exit 1
  fi
}

run_benchmarks() {
  local label=$1
  local outdir=$RESULTS_DIR/$label
  mkdir -p $outdir

  echo "  Running 100% put..."
  run_tput $outdir/put100.txt 100
  sleep 5

  echo "  Running 50% put..."
  run_tput $outdir/put50.txt 50
  sleep 5

  echo "  Running 10% put..."
  run_tput $outdir/put10.txt 10
  sleep 5
}

build_and_run() {
  local label=$1
  echo ""
  echo "================================================"
  echo "Building and benchmarking: $label"
  echo "================================================"
  cd $BUILD
  make -j$(nproc) 2>&1 | tail -3
  run_benchmarks $label
}

checkout_build_run() {
  local commit=$1
  local label=$2
  echo ""
  echo "Checking out $label ($commit)..."
  git -C $REPO checkout $commit
  build_and_run $label
}

# --- Stash only if there are local changes ---
STASHED=0
if ! git -C $REPO diff --quiet || ! git -C $REPO diff --cached --quiet; then
  git -C $REPO stash
  STASHED=1
fi

# --- Benchmark all four commits ---
checkout_build_run $CURRENT_COMMIT "current"
checkout_build_run $PARALLEL_AE_COMMIT "parallel_ae"
checkout_build_run $LEASE_FIX_COMMIT "lease_fix"
checkout_build_run $BASELINE_COMMIT "baseline"

# --- Return to original branch ---
echo ""
echo "Returning to branch: $ORIGINAL_BRANCH..."
git -C $REPO checkout $ORIGINAL_BRANCH
if [ $STASHED -eq 1 ]; then
  git -C $REPO stash pop
fi
cd $BUILD
make -j$(nproc) 2>&1 | tail -3

# --- Print comparison ---
print_comparison() {
  local workload=$1
  local label_a=$2
  local label_b=$3
  local file_a=$RESULTS_DIR/$label_a/$workload.txt
  local file_b=$RESULTS_DIR/$label_b/$workload.txt

  echo ""
  echo "------------------------------------------------------------------------------------------------------------------------"
  echo "  $label_a  vs  $label_b  |  workload: $workload"
  echo "------------------------------------------------------------------------------------------------------------------------"
  printf "%-10s | %-10s %-10s %-10s %-10s %-10s | %-10s %-10s %-10s %-10s %-10s | %s\n" \
    "clients" \
    "latAvg_a" "latP50_a" "latP90_a" "latP99_a" "tput_a" \
    "latAvg_b" "latP50_b" "latP90_b" "latP99_b" "tput_b" \
    "tput_delta"
  echo "------------------------------------------------------------------------------------------------------------------------"

  while IFS= read -r line_b; do
    [[ "$line_b" =~ ^[[:space:]]*[#\-]*[[:space:]]*$ ]] && continue
    [[ "$line_b" =~ "clientCount" ]] && continue
    [[ -z "$line_b" ]] && continue

    clients=$(echo "$line_b" | awk '{print $1}')
    line_a=$(grep -E "^[[:space:]]+${clients}[[:space:]]+" "$file_a" || true)
    [[ -z "$line_a" ]] && continue

    avg_a=$(echo "$line_a" | awk '{print $2}')
    p50_a=$(echo "$line_a" | awk '{print $3}')
    p90_a=$(echo "$line_a" | awk '{print $4}')
    p99_a=$(echo "$line_a" | awk '{print $5}')
    tput_a=$(echo "$line_a" | awk '{print $6}')

    avg_b=$(echo "$line_b" | awk '{print $2}')
    p50_b=$(echo "$line_b" | awk '{print $3}')
    p90_b=$(echo "$line_b" | awk '{print $4}')
    p99_b=$(echo "$line_b" | awk '{print $5}')
    tput_b=$(echo "$line_b" | awk '{print $6}')

    if [[ -z "$tput_a" || "$tput_a" == "0" ]]; then
      delta="N/A"
    else
      delta=$(awk "BEGIN {printf \"%+.0f%%\", ($tput_b - $tput_a) / $tput_a * 100}")
    fi

    printf "%-10s | %-10s %-10s %-10s %-10s %-10s | %-10s %-10s %-10s %-10s %-10s | %s\n" \
      "$clients" \
      "$avg_a" "$p50_a" "$p90_a" "$p99_a" "$tput_a" \
      "$avg_b" "$p50_b" "$p90_b" "$p99_b" "$tput_b" \
      "$delta"
  done < "$file_b"
}

print_section() {
  local workload=$1
  echo ""
  echo "========================================================"
  echo "WORKLOAD: $workload"
  echo "========================================================"
  print_comparison "$workload" "baseline" "lease_fix"
  print_comparison "$workload" "baseline" "parallel_ae"
  print_comparison "$workload" "baseline" "current"
  print_comparison "$workload" "lease_fix" "parallel_ae"
  print_comparison "$workload" "lease_fix" "current"
  print_comparison "$workload" "parallel_ae" "current"
}

print_section "put100"
print_section "put50"
print_section "put10"

echo ""
echo "Raw results saved to $RESULTS_DIR"
EOF
chmod +x ~/lab1-ozzie/bench/compare.sh