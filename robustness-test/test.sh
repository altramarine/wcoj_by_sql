#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
cd "$repo_root"

# These result files have already been generated. Uncomment to regenerate them.
# for dataset in skitters topcats uspatent; do
#   ./run_robustness_test.sh \
#     -d "$dataset" \
#     -t 900 \
#     -o "robustness-test/results-${dataset}-15min.tsv"
# done

./run_robustness_test.sh \
  -b \
  -t 900 \
  -o robustness-test/baseline.tsv
