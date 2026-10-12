#!/usr/bin/env bash
set -euo pipefail

readonly REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
export DATASET_DIR="${DATASET_DIR:-/data/wcoj_by_sql/datasets}"

if (( $# > 1 )) || [[ "${1:-}" != "" && "${1:-}" != "--local-only" ]]; then
  echo "Usage: bash load_datasets.sh [--local-only]" >&2
  exit 2
fi

mkdir -p -- "$DATASET_DIR"
shopt -s nullglob
for source in "$REPO_DIR"/datasets/*.csv "$REPO_DIR"/datasets/*.txt.gz "$REPO_DIR"/datasets/*.tar.gz; do
  destination="$DATASET_DIR/$(basename -- "$source")"
  if [[ -e "$destination" ]]; then
    if ! cmp -s -- "$source" "$destination"; then
      echo "Destination differs from local file: $destination" >&2
      exit 1
    fi
    continue
  fi
  echo "Copying $source to $destination"
  temporary="$(mktemp "$DATASET_DIR/.load-XXXXXXXX")"
  trap 'rm -f -- "$temporary"' EXIT
  cp -- "$source" "$temporary"
  mv -- "$temporary" "$destination"
  trap - EXIT
done

if [[ "${1:-}" != "--local-only" ]]; then
  bash "$REPO_DIR/download.sh" \
    skitters topcats gplus uspatent epinions livejournal twitter berkstan google amazon
fi

echo "Datasets ready in $DATASET_DIR"
