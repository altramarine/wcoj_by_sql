#!/usr/bin/env bash
set -euo pipefail

# Datasets from Table 8 of:
# "Optimizing Subgraph Queries by Combining Binary and Worst-Case Optimal Joins"
# (PVLDB 12(11), 2019).
#
# Each output is a headerless, two-column CSV containing directed edges:
#
#     source_vertex_id,destination_vertex_id
#
# The source archives are retained so interrupted conversions can be retried
# without downloading the data again.

readonly DATASET_DIR="${DATASET_DIR:-datasets}"

usage() {
  cat <<'EOF'
Usage: ./download.sh [DATASET ...]

Download and convert graph datasets to headerless two-column CSV files.
With no DATASET arguments, the repository's original four datasets are
downloaded. Existing CSVs are kept.

Paper https://arxiv.org/pdf/2510.25684 datasets:
  skitters     AS-Skitter Internet topology graph
  topcats      Wiki-Topcats page network
  gplus        Google+ ego-network edges
  uspatent     US patent citation network

Paper https://www.vldb.org/pvldb/vol12/p1692-mhedhbi.pdf Table 8 datasets:
  epinions     Epinions social network
  livejournal  LiveJournal social network
  twitter      Twitter-2010 follower network (5.5 GB download; CSV is much larger)
  berkstan     Berkeley/Stanford web graph
  google       Google web graph
  amazon       Amazon product co-purchasing graph

Examples:
  ./download.sh skitters topcats gplus uspatent
  ./download.sh epinions google amazon
  DATASET_DIR=/data/graphs ./download.sh twitter
EOF
}

download_and_convert() {
  local name="$1"
  local url="$2"
  local expected_edges="$3"
  local archive="${DATASET_DIR}/${name}.txt.gz"
  local csv="${DATASET_DIR}/${name}.csv"
  local tmp="${csv}.tmp"

  if [[ -s "$csv" ]]; then
    echo "Already exists, skipping: ${csv}"
    return
  fi

  echo "Downloading ${name}..."
  wget --continue --output-document="$archive" "$url"

  echo "Converting ${name} to ${csv}..."
  if ! gzip --decompress --stdout -- "$archive" |
    awk -v expected="$expected_edges" '
      BEGIN { OFS = "," }
      /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
      {
        sub(/\r$/, "", $NF)
        if (NF != 2 || $1 !~ /^[0-9]+$/ || $2 !~ /^[0-9]+$/) {
          printf "Malformed edge at input line %d: %s\n", NR, $0 > "/dev/stderr"
          invalid = 1
          next
        }
        print $1, $2
        edges++
      }
      END {
        if (invalid || edges != expected) {
          printf "Expected %d edges, converted %d\n", expected, edges > "/dev/stderr"
          exit 1
        }
      }
    ' > "$tmp"; then
    rm -f -- "$tmp"
    echo "Conversion failed for ${name}; archive kept at ${archive}." >&2
    return 1
  fi

  mv -- "$tmp" "$csv"
  echo "Ready: ${csv} (${expected_edges} directed edges)"
}

download_gplus() {
  local archive="${DATASET_DIR}/gplus.tar.gz"
  local csv="${DATASET_DIR}/gplus.csv"
  local tmp="${csv}.tmp"

  if [[ -s "$csv" ]]; then
    echo "Already exists, skipping: ${csv}"
    return
  fi

  echo "Downloading gplus..."
  wget --continue --output-document="$archive" \
    "https://snap.stanford.edu/data/gplus.tar.gz"

  echo "Converting gplus to ${csv}..."
  if ! tar --extract --to-stdout --file="$archive" --wildcards 'gplus/*.edges' |
    awk '
      BEGIN { OFS = "," }
      /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
      {
        sub(/\r$/, "", $NF)
        if (NF != 2 || $1 !~ /^[0-9]+$/ || $2 !~ /^[0-9]+$/) {
          printf "Malformed edge at input line %d: %s\n", NR, $0 > "/dev/stderr"
          invalid = 1
          next
        }
        print $1, $2
      }
      END { if (invalid) exit 1 }
    ' > "$tmp"; then
    rm -f -- "$tmp"
    echo "Conversion failed for gplus; archive kept at ${archive}." >&2
    return 1
  fi

  mv -- "$tmp" "$csv"
  echo "Ready: ${csv}"
}

download_dataset() {
  case "$1" in
    skitters)
      download_and_convert \
        "as-skitter" \
        "https://snap.stanford.edu/data/as-skitter.txt.gz" \
        11095298
      ;;
    topcats)
      download_and_convert \
        "wiki-topcats" \
        "https://snap.stanford.edu/data/wiki-topcats.txt.gz" \
        28511807
      ;;
    gplus)
      download_gplus
      ;;
    uspatent)
      download_and_convert \
        "cit-Patents" \
        "https://snap.stanford.edu/data/cit-Patents.txt.gz" \
        16518947
      ;;
    epinions)
      download_and_convert \
        "soc-Epinions1" \
        "https://snap.stanford.edu/data/soc-Epinions1.txt.gz" \
        508837
      ;;
    livejournal)
      download_and_convert \
        "soc-LiveJournal1" \
        "https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz" \
        68993773
      ;;
    twitter)
      download_and_convert \
        "twitter-2010" \
        "https://snap.stanford.edu/data/twitter-2010.txt.gz" \
        1468364884
      ;;
    berkstan)
      download_and_convert \
        "web-BerkStan" \
        "https://snap.stanford.edu/data/web-BerkStan.txt.gz" \
        7600595
      ;;
    google)
      download_and_convert \
        "web-Google" \
        "https://snap.stanford.edu/data/web-Google.txt.gz" \
        5105039
      ;;
    amazon)
      download_and_convert \
        "amazon0601" \
        "https://snap.stanford.edu/data/amazon0601.txt.gz" \
        3387388
      ;;
    *)
      echo "Unknown dataset: $1" >&2
      usage >&2
      return 2
      ;;
  esac
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

mkdir -p -- "$DATASET_DIR"

if (( $# == 0 )); then
  set -- skitters topcats gplus uspatent
fi

for dataset in "$@"; do
  download_dataset "$dataset"
done

echo "All requested datasets are ready."
