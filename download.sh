#!/bin/bash
set -e

mkdir -p datasets

echo "Downloading as-skitter.txt.gz..."
wget -c -P datasets https://snap.stanford.edu/data/as-skitter.txt.gz
zcat ./datasets/as-skitter.txt.gz | grep -v '^#' | tr '\t' ',' | tr ' ' ',' > ./datasets/as-skitter.csv

echo "Downloading wiki-topcats.txt.gz..."
wget -c -P datasets https://snap.stanford.edu/data/wiki-topcats.txt.gz
zcat ./datasets/wiki-topcats.txt.gz | grep -v '^#' | tr '\t' ',' | tr ' ' ',' > ./datasets/wiki-topcats.csv


echo "Downloading gplus.tar.gz..."
wget -c -P datasets https://snap.stanford.edu/data/gplus.tar.gz
tar -xOf ./datasets/gplus.tar.gz --wildcards 'gplus/*.edges' | tr ' ' ',' > ./datasets/gplus.csv

echo "Downloading cit-Patents.txt.gz..."
wget -c -P datasets https://snap.stanford.edu/data/cit-Patents.txt.gz
zcat ./datasets/cit-Patents.txt.gz | grep -v '^#' | tr '\t' ',' | tr -d '\r' > ./datasets/cit-Patents.csv

echo "All downloads complete."


