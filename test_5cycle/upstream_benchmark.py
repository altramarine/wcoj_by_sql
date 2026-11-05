"""Run the pinned upstream hand-plan matrix using the ordered SQL backend.

Uses upstream query/algorithm/dataset labels, seed, workers, repeat and timeout.
Adds a default-optimizer DuckDB CQ control for each family. Historical FlowLog
results are labelled separately and never used as same-machine speed evidence.
Input loading is excluded; input dedup, degrees, routing, joins and COUNT are
included. Every run uses a fresh read-only database connection, sequentially.
"""
import argparse
import hashlib
import json
from pathlib import Path
import resource
import statistics
import subprocess
import threading
import time
import tomllib

import duckdb

from .benchmark_cte import cq_sql
from .cte import compile_cte_program
from .prepare_upstream import CACHE_ROOT, REPOSITORY, cache_name, upstream_file
from .translate import parse_program


def execute(database, sql, *, workers, timeout, memory):
    with duckdb.connect(str(database), read_only=True,
                        config={'threads': workers, 'memory_limit': memory, 'temp_directory': ''}) as con:
        timer = threading.Timer(timeout, con.interrupt)
        timer.daemon = True
        timer.start()
        started = time.perf_counter()
        result = {'status': 'ok', 'count': None}
        try:
            # Full SQL, including SET/RESET, is parsed inside the timed region.
            for statement in con.extract_statements(sql):
                if time.perf_counter() - started >= timeout:
                    result['status'] = 'timeout'
                    break
                rows = con.execute(statement).fetchall()
                if statement.type == duckdb.StatementType.SELECT:
                    result['count'] = rows[0][0]
        except duckdb.InterruptException:
            result['status'] = 'timeout'
        except duckdb.OutOfMemoryException as error:
            result.update(status='memory_out', error=str(error))
        except duckdb.Error as error:
            result.update(status='error', error=str(error))
        finally:
            result['seconds'] = time.perf_counter() - started
            timer.cancel()
            timer.join()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, default=REPOSITORY)
    parser.add_argument('--cache-root', type=Path, default=CACHE_ROOT)
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'upstream_results')
    parser.add_argument('--queries', nargs='+')
    parser.add_argument('--datasets', nargs='+', choices=('amazon', 'berkstan'), default=['amazon', 'berkstan'])
    parser.add_argument('--algorithms', nargs='+', help='upstream names, e.g. base h1 b1 h9r h9rr')
    parser.add_argument('--memory', default='220GB', help='SQL-specific limit, same as run_sql.py')
    parser.add_argument('--timeout', type=float, help='override upstream 600-second timeout')
    parser.add_argument('--repeat', type=int, help='override upstream one execution')
    parser.add_argument('--resume', action='store_true', help='skip completed query/algorithm/dataset/scale records')
    args = parser.parse_args()
    config_bytes = upstream_file(args.repository, 'robustness_config_hand.toml')
    config = tomllib.loads(config_bytes.decode())
    defaults = config['defaults']
    workers, seed = defaults['workers'], defaults['seed']
    timeout = args.timeout if args.timeout is not None else defaults['timeout_seconds']
    repeat = args.repeat if args.repeat is not None else defaults['repeat']
    if min(timeout, repeat) <= 0:
        parser.error('timeout and repeat must be positive')
    if args.queries and set(args.queries) - config['queries'].keys():
        parser.error('Unknown query family')
    revision = subprocess.check_output(['git', '-C', str(args.repository), 'rev-parse', 'HEAD'], text=True).strip()
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'upstream_config_hand.toml').write_bytes(config_bytes)
    historical = [json.loads(line) for line in upstream_file(args.repository, 'results/robustness_hand.jsonl').decode().splitlines() if line.strip()]
    latest = {}
    for row in historical:
        if row.get('status') == 'ok':
            latest[(row['query'], row['algorithm'], row['dataset_file'], row['scale'], row['seed'])] = row
    results_path = args.output / 'results.jsonl'
    if results_path.exists() and not args.resume:
        parser.error('Output exists; use --resume or a new --output directory')
    records = [json.loads(line) for line in results_path.read_text().splitlines() if line.strip()] if results_path.exists() else []

    def key(row):
        return row['query'], row['algorithm'], row['dataset_file'], row['scale']

    completed = {key(row) for row in records}
    metadata = {'upstream_revision': revision, 'config_sha256': hashlib.sha256(config_bytes).hexdigest(),
                'duckdb': duckdb.__version__, 'workers': workers, 'seed': seed,
                'repeat': repeat, 'timeout_seconds': timeout, 'memory_limit': args.memory,
                'spill': False, 'queries': args.queries, 'datasets': args.datasets,
                'algorithms': args.algorithms, 'historical_results': 'recorded FlowLog runs from upstream, different execution/hardware provenance; not a fresh runtime comparison',
                'timing': 'SQL parse, SET/RESET, input dedup, degree preprocessing, routing, joins, COUNT; excludes dataset load, connection startup and SQL generation; fresh read-only connections, no warmup',
                'peak_memory_policy': 'process ru_maxrss cumulative high-water mark only; no per-query memory claim'}
    metadata_path = args.output / 'run_metadata.json'
    if metadata_path.exists() and json.loads(metadata_path.read_text()) != metadata:
        parser.error('Resume settings differ from the saved run metadata')
    metadata_path.write_text(json.dumps(metadata, indent=2) + '\n')
    for query, settings in config['queries'].items():
        if args.queries and query not in args.queries:
            continue
        # The CQ header comes from an actual hand plan, not a baseline's comments.
        representative = next((args.repository / 'robustness/hand_selected_split' / query).glob('*.dl'))
        cq = cq_sql(parse_program(representative.read_text()))
        algorithms = [('duckdb_default', None, False)]
        for entry in settings['algorithms']:
            parts = entry.split(':')
            name, kind, source = parts[:3]
            if kind != 'flowlog':
                raise ValueError('Expected an upstream FlowLog rule')
            if args.algorithms and name not in args.algorithms:
                continue
            algorithms.append((name, source, 'reference' in parts[3:]))
        for entry in settings['datasets']:
            dataset_file, scale_text, *_ = entry.split(':')
            dataset, scale = Path(dataset_file).stem, float(scale_text)
            if dataset not in args.datasets:
                continue
            input_metadata = json.loads((args.cache_root / f'{cache_name(dataset, scale, seed)}.json').read_text())
            database = Path(input_metadata['database'])
            if input_metadata['rows'] != input_metadata['distinct_edges']:
                raise ValueError('The direct CQ control requires a set input; prepare a distinct control table for duplicate input')
            for algorithm, source, reference in algorithms:
                record_key = query, algorithm, dataset_file, scale
                if record_key in completed:
                    continue
                if source is None:
                    sql, source_hash = cq, None
                else:
                    program = parse_program((args.repository / source).read_text())
                    plan = compile_cte_program(program)
                    sql, source_hash = plan.sql, plan.manifest['source_sha256']
                sql_path = args.output / 'sql' / query / f'{algorithm}.sql'
                sql_path.parent.mkdir(parents=True, exist_ok=True)
                sql_path.write_text(sql)
                runs = []
                for _ in range(repeat):
                    run = execute(database, sql, workers=workers, timeout=timeout, memory=args.memory)
                    runs.append(run)
                    if run['status'] != 'ok':
                        break
                counts = {run['count'] for run in runs if run['status'] == 'ok'}
                if len(counts) > 1:
                    raise AssertionError('Repeated execution produced different counts')
                status = 'ok' if len(runs) == repeat and all(run['status'] == 'ok' for run in runs) else runs[-1]['status']
                old = latest.get((query, algorithm, dataset_file, scale, seed))
                reference_record = next((row for row in records if row['query'] == query
                    and row['dataset_file'] == dataset_file and row['scale'] == scale
                    and row['algorithm'] == 'duckdb_default' and row['status'] == 'ok'), None)
                value = next(iter(counts)) if status == 'ok' else None
                ref_value = reference_record['result_value'] if reference_record else value if algorithm == 'duckdb_default' else None
                record = {'query': query, 'algorithm': algorithm, 'kind': 'duckdb',
                          'is_reference': reference, 'dataset_file': dataset_file, 'scale': scale,
                          'seed': seed, 'workers': workers, 'repeat': repeat, 'status': status,
                          'result_value': value, 'reference_value': ref_value,
                          'correctness_match': value == ref_value if value is not None and ref_value is not None else None,
                          'elapsed_s_mean': statistics.mean(run['seconds'] for run in runs) if status == 'ok' else None,
                          'runs': runs, 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest(),
                          'source': source, 'source_sha256': source_hash, 'input': input_metadata,
                          'historical_flowlog': {field: old.get(field) for field in ['run_id', 'elapsed_s_mean', 'result_value', 'workers', 'flowlog_commit']} if old else None,
                          'historical_count_matches': old['result_value'] == value if old and value is not None else None,
                          'process_peak_rss_kb_so_far': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
                with results_path.open('a') as stream:
                    stream.write(json.dumps(record) + '\n')
                records.append(record)
                completed.add(record_key)
                print(f'{dataset}@{scale:g}/{query}/{algorithm}: {status} '
                      f'{record["elapsed_s_mean"]}s count={value} '
                      f'CQ={record["correctness_match"]} historical={record["historical_count_matches"]}', flush=True)
                if record['correctness_match'] is False:
                    raise AssertionError('SQL count differs from the independently optimized original CQ')
    print(f'{len(records)} records; results: {results_path}', flush=True)


if __name__ == '__main__':
    main()
