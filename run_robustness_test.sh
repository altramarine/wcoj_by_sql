#!/usr/bin/env bash

set -u

usage() {
  echo "Usage: $0 [-d DATASET] [-t SECONDS] [-o OUTPUT] [-q QUERY_TYPES] [-r] [-b]"
  echo "  QUERY_TYPES: query directories separated by spaces or commas, e.g. '3 4' or '3,4'"
  echo "  -b: run queries/normal baselines for skitters, topcats, and uspatent"
}

dataset="skitters"
timeout_seconds="600"
output="robustness-test/results-${dataset}.tsv"
resume=false
query_types=""
baseline=false

while getopts ":d:t:o:q:hrb" option; do
  case "$option" in
    d) dataset="$OPTARG" ;;
    t) timeout_seconds="$OPTARG" ;;
    o) output="$OPTARG" ;;
    q) query_types="${OPTARG//,/ }" ;;
    h) usage; exit 0 ;;
    r) resume=true ;;
    b) baseline=true ;;
    \?) usage >&2; exit 2 ;;
    :) echo "Option -$OPTARG requires an argument" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -n "$query_types" ]]; then
  for query_type in $query_types; do
    if [[ ! "$query_type" =~ ^[A-Za-z0-9_-]+$ ]]; then
      echo "Invalid query type: $query_type" >&2
      exit 2
    fi
  done
else
  query_types=$(find robustness-test/binary -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort -V)
fi

case "$dataset" in
  skitters|topcats|gplus|uspatent) ;;
  *) echo "Unsupported dataset: $dataset" >&2; exit 2 ;;
esac

if [[ "$baseline" == true ]]; then
  mkdir -p "$(dirname "$output")"
  printf 'dataset\tquery\tstatus\texecution_time\n' > "$output"
  log_file="${output%.tsv}.log"

  for baseline_dataset in skitters topcats uspatent; do
    for query_type in $query_types; do
      sql_file="queries/normal/${query_type}.sql"
      [[ -f "$sql_file" ]] || continue

      start_time=$(date +%s.%N)
      timeout --signal=TERM --kill-after=10s "${timeout_seconds}s" \
        uv run python run_sql.py "$sql_file" -d "$baseline_dataset" \
          --timeout "$timeout_seconds" > "$log_file" 2>&1
      exit_code=$?
      end_time=$(date +%s.%N)

      status=$(sed -n 's/^STATUS: //p' "$log_file" | tail -n 1)
      if [[ -z "$status" ]]; then
        elapsed_seconds=$(awk -v start="$start_time" -v end="$end_time" 'BEGIN { print end - start }')
        if awk -v elapsed="$elapsed_seconds" -v limit="$timeout_seconds" 'BEGIN { exit !(elapsed >= limit) }'; then
          status="TIMEOUT"
        elif [[ "$exit_code" -eq 137 ]]; then
          status="MEMORY_OUT"
        else
          status="ERROR"
        fi
      fi

      execution_time=$(awk -v start="$start_time" -v end="$end_time" 'BEGIN { printf "%.3f", end - start }')
      printf '%s\t%s\t%s\t%s\n' \
        "$baseline_dataset" "${query_type}.sql" "$status" "$execution_time" >> "$output"
      printf '%s/%s: %s (%ss)\n' \
        "$baseline_dataset" "${query_type}.sql" "$status" "$execution_time"
    done
  done

  rm -f "$log_file"
  echo "Baseline results written to $output"
  exit 0
fi

mkdir -p "$(dirname "$output")"
if [[ "$resume" == true && -f "$output" ]]; then
  declare -A completed_queries
  while IFS=$'\t' read -r family query status execution_time; do
    [[ "$family" == "family" ]] && continue
    completed_queries["$family/$query"]="$status"
  done < "$output"
else
  printf 'family\tquery\tstatus\texecution_time\n' > "$output"
fi
declare -A status_counts=([NORMAL]=0 [TIMEOUT]=0 [MEMORY_OUT]=0)

for family in binary wcoj; do
  for query_type in $query_types; do
    while IFS= read -r sql_file; do
    log_file="${output%.tsv}.log"
    query_name=${sql_file#robustness-test/"$family"/}
    if [[ "$resume" == true && -n "${completed_queries[$family/$query_name]+x}" ]]; then
      continue
    fi
    start_time=$(date +%s.%N)
    timeout --signal=TERM --kill-after=10s "${timeout_seconds}s" \
      uv run python run_sql.py "$sql_file" -d "$dataset" --timeout "$timeout_seconds" > "$log_file" 2>&1
    exit_code=$?
    end_time=$(date +%s.%N)

    status=$(sed -n 's/^STATUS: //p' "$log_file" | tail -n 1)
    if [[ -z "$status" ]]; then
      elapsed_seconds=$(awk -v start="$start_time" -v end="$end_time" 'BEGIN { print end - start }')
      if awk -v elapsed="$elapsed_seconds" -v limit="$timeout_seconds" 'BEGIN { exit !(elapsed >= limit) }'; then
        status="TIMEOUT"
      elif [[ "$exit_code" -eq 137 ]]; then
        status="MEMORY_OUT"
      else
        status="ERROR"
      fi
    fi
    execution_time=$(awk -v start="$start_time" -v end="$end_time" 'BEGIN { printf "%.3f", end - start }')
    printf '%s\t%s\t%s\t%s\n' "$family" "$query_name" "$status" "$execution_time" >> "$output"
    if [[ -n "${status_counts[$status]+x}" ]]; then
      status_counts[$status]=$((status_counts[$status] + 1))
    fi
    printf '%s/%s: %s (%ss)\n' "$family" "$query_name" "$status" "$execution_time"
    done < <(find "robustness-test/$family/$query_type" -type f -name '*.sql' -print 2>/dev/null | sort -V)
  done
done

rm -f "${output%.tsv}.log"
printf 'NORMAL: %s\n' "${status_counts[NORMAL]}"
printf 'TIMEOUT: %s\n' "${status_counts[TIMEOUT]}"
printf 'MEMORY_OUT: %s\n' "${status_counts[MEMORY_OUT]}"
echo "Results written to $output"
