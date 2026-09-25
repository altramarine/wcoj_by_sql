#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
cd "$repo_root"

readonly query_type="5cycle"
readonly dataset="topcats"
readonly timeout_seconds="900"
readonly plan_count="50"
readonly baseline_output="robustness-test/baseline-topcats-5cycle.tsv"
readonly random_output="robustness-test/results-topcats-5cycle-15min.tsv"
readonly log_file="robustness-test/topcats-5cycle.log"

exec > >(tee -a "$log_file") 2>&1

if [[ -e "$baseline_output" || -e "$random_output" ]]; then
  echo "Refusing to overwrite an existing Topcats 5-cycle result file." >&2
  echo "Remove or rename these files before starting a new run:" >&2
  echo "  $baseline_output" >&2
  echo "  $random_output" >&2
  exit 1
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/wcoj-uv-cache}"

mkdir -p \
  "robustness-test/binary/$query_type" \
  "robustness-test/wcoj/$query_type"

for seed in $(seq 1 "$plan_count"); do
  binary_plan="robustness-test/binary/$query_type/${seed}.sql"
  wcoj_plan="robustness-test/wcoj/$query_type/${seed}.sql"

  if [[ ! -s "$binary_plan" ]]; then
    uv run python robustness-test/random_plan_generator.py \
      --mode binary \
      --seed "$seed" \
      --query-file "queries/$query_type.sql" > "${binary_plan}.tmp"
    mv -- "${binary_plan}.tmp" "$binary_plan"
  fi

  if [[ ! -s "$wcoj_plan" ]]; then
    uv run python robustness-test/random_plan_generator.py \
      --seed "$seed" \
      --query-file "queries/$query_type.sql" > "${wcoj_plan}.tmp"
    mv -- "${wcoj_plan}.tmp" "$wcoj_plan"
  fi
done

echo "Generated $plan_count binary and $plan_count WCOJ random plans."

./run_robustness_test.sh \
  -b \
  -d "$dataset" \
  -q "$query_type" \
  -t "$timeout_seconds" \
  -o "$baseline_output"

./run_robustness_test.sh \
  -d "$dataset" \
  -q "$query_type" \
  -t "$timeout_seconds" \
  -o "$random_output"

echo "Topcats 5-cycle experiment complete."
echo "Baseline: $baseline_output"
echo "Random plans: $random_output"
