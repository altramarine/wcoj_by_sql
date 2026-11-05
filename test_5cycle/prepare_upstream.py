"""Prepare the datasets named by the pinned upstream hand-plan configuration.

For scaled inputs, use upstream's original scale-edges binary (same Cargo.lock),
which shuffles sorted vertex IDs and takes the induced subgraph. This is not
edge sampling. Threshold fields are ignored by upstream's FlowLog path too.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time
import tomllib

import duckdb


REPOSITORY = Path('/data/Partitioned-Join-Flowlog')
CACHE_ROOT = Path('/data/wcoj_by_sql/flowlog_sql')
DATASETS = {'amazon': Path('/data/wcoj_by_sql/datasets/amazon0601.csv'),
            'berkstan': Path('/data/wcoj_by_sql/datasets/web-BerkStan.csv')}


def upstream_file(repository, path):
    return subprocess.check_output(['git', '-C', str(repository), 'show', f'HEAD:{path}'])


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def cache_name(dataset, scale, seed):
    return f'{dataset}_scale{scale:g}_seed{seed}'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, default=REPOSITORY)
    parser.add_argument('--cache-root', type=Path, default=CACHE_ROOT)
    parser.add_argument('--datasets', nargs='+', choices=tuple(DATASETS), default=list(DATASETS))
    parser.add_argument('--scaler', type=Path, help='binary compiled from the pinned upstream scale-edges source')
    args = parser.parse_args()
    config = tomllib.loads(upstream_file(args.repository, 'robustness_config_hand.toml').decode())
    revision = subprocess.check_output(['git', '-C', str(args.repository), 'rev-parse', 'HEAD'], text=True).strip()
    seed = config['defaults']['seed']
    entries = {(Path(entry.split(':')[0]).stem, float(entry.split(':')[1]))
               for query in config['queries'].values() for entry in query['datasets']
               if Path(entry.split(':')[0]).stem in args.datasets}
    args.cache_root.mkdir(parents=True, exist_ok=True)
    hashes = {dataset: digest(DATASETS[dataset]) for dataset in args.datasets}
    for dataset, scale in sorted(entries):
        started = time.perf_counter()
        stem = cache_name(dataset, scale, seed)
        database = args.cache_root / f'{stem}.duckdb'
        metadata_path = database.with_suffix('.json')
        expected = {'dataset': dataset, 'source': str(DATASETS[dataset]),
                    'source_sha256': hashes[dataset], 'scale': scale, 'seed': seed, 'flags': [],
                    'upstream_revision': revision,
                    'scaling': 'unmodified full graph' if scale == 1 else 'upstream sorted-node shuffle, induced subgraph'}
        if metadata_path.exists() and database.exists():
            cached = json.loads(metadata_path.read_text())
            if all(cached.get(key) == value for key, value in expected.items()):
                print('cache hit', database, flush=True)
                continue
        source = DATASETS[dataset]
        if scale != 1:
            if args.scaler is None:
                parser.error('A pinned upstream --scaler binary is required for scaled inputs')
            source = args.cache_root / f'{stem}.csv'
            subprocess.run([str(args.scaler.resolve()), str(DATASETS[dataset]), str(source),
                            '--scale', str(scale), '--seed', str(seed)], check=True)
            expected.update({'scaler_binary_sha256': digest(args.scaler),
                             'scaler_source_sha256': hashlib.sha256(upstream_file(args.repository,
                                 'flowlog_helper/scale-edges/src/main.rs')).hexdigest(),
                             'scaler_lock_sha256': hashlib.sha256(upstream_file(args.repository,
                                 'flowlog_helper/scale-edges/Cargo.lock')).hexdigest()})
        temporary = database.with_suffix('.preparing.duckdb')
        temporary.unlink(missing_ok=True)
        with duckdb.connect(str(temporary), config={'threads': config['defaults']['workers']}) as con:
            con.execute("CREATE TABLE R AS SELECT * FROM read_csv(?,header=false,columns={'col0':'BIGINT','col1':'BIGINT'})", [str(source)])
            rows, unique, nulls = con.execute('SELECT count(*),count(DISTINCT (col0,col1)),count(*) FILTER(WHERE col0 IS NULL OR col1 IS NULL) FROM R').fetchone()
            if nulls:
                raise ValueError('NonNULL input required')
        temporary.replace(database)
        expected.update({'input_csv': str(source), 'input_csv_sha256': digest(source),
                         'rows': rows, 'distinct_edges': unique, 'database': str(database),
                         'load_seconds': time.perf_counter() - started})
        metadata_path.write_text(json.dumps(expected, indent=2) + '\n')
        print(stem, 'rows=', rows, 'distinct=', unique, flush=True)


if __name__ == '__main__':
    main()
