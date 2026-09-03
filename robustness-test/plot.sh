#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
cd "$repo_root"

mkdir -p robustness-test/filtered

for input in robustness-test/results-*-15min.tsv; do
  if [[ ! -e "$input" ]]; then
    echo "No results-*-15min.tsv files found in robustness-test" >&2
    exit 1
  fi

  filename=${input##*/}
  output="robustness-test/${filename%.tsv}-boxplot.png"
  filtered_output="robustness-test/filtered/${filename%.tsv}-boxplot.png"

  uv run python plot_robustness_boxplot.py \
    "$input" --baseline robustness-test/baseline.tsv -o "$output"
  uv run python plot_robustness_boxplot.py \
    "$input" --baseline robustness-test/baseline.tsv \
    --filtered -o "$filtered_output"
done
