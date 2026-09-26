#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
cd "$repo_root"

readonly query_type="5cycle"
readonly dataset="berkstan"
readonly timeout_seconds="900"
readonly baseline_output="robustness-test/baseline-berkstan-5cycle.tsv"
readonly random_output="robustness-test/results-berkstan-5cycle-15min.tsv"
readonly log_file="robustness-test/berkstan-5cycle.log"

exec > >(tee -a "$log_file") 2>&1

if [[ -e "$baseline_output" || -e "$random_output" ]]; then
  echo "Refusing to overwrite an existing BerkStan 5-cycle result file." >&2
  echo "Remove or rename these files before starting a new run:" >&2
  echo "  $baseline_output" >&2
  echo "  $random_output" >&2
  exit 1
fi

if [[ $(find robustness-test/binary/5cycle -maxdepth 1 -type f -name '*.sql' | wc -l) -ne 50 ||
      $(find robustness-test/wcoj/5cycle -maxdepth 1 -type f -name '*.sql' | wc -l) -ne 50 ]]; then
  echo "Expected 50 binary and 50 WCOJ plans under robustness-test/*/5cycle." >&2
  exit 1
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/wcoj-uv-cache}"

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

echo "BerkStan 5-cycle experiment complete."
echo "Baseline: $baseline_output"
echo "Random plans: $random_output"
