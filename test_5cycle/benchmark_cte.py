"""Bounded comparison of literal CTAS, all-materialized WITH and selective WITH.

Input loading and SQL generation are excluded; degree computation, routing,
intermediates, COUNT and optimizer SET/RESET are included. Each execution uses
a fresh connection to the same immutable input database. This is a smoke test,
not an exhaustive benchmark of the source graph or the 80 plans.
"""
import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path
import random
import statistics
import tempfile
import threading
import time

import duckdb

from .cte import compile_cte_program
from .reference import original_query
from .translate import compile_program, parse_program


def cq_sql(program):
    """Independent unsplit CQ, using DuckDB's default optimizer on the edge set."""
    atoms, _ = original_query(program)
    bindings, filters, sources = {}, [], []
    for index, atom in enumerate(atoms):
        alias = f'e{index}'
        sources.append(f'R AS {alias}')
        for position, variable in enumerate(atom.arguments):
            value = f'{alias}.col{position}'
            if variable in bindings:
                filters.append(f'{value} = {bindings[variable]}')
            else:
                bindings[variable] = value
    return ('SELECT count(*) FROM ' + ', '.join(sources)
            + (' WHERE ' + ' AND '.join(filters) if filters else '') + ';')


def execute(database, statements, *, threads, timeout, memory):
    with duckdb.connect(str(database), config={'threads': threads, 'memory_limit': memory,
                                             'temp_directory': ''}) as con:
        timer = threading.Timer(timeout, con.interrupt)
        timer.daemon = True
        timer.start()
        started = time.perf_counter()
        result = {'status': 'ok', 'count': None}
        try:
            for statement in statements:
                if time.perf_counter() - started >= timeout:
                    result['status'] = 'timeout'
                    break
                rows = con.execute(statement).fetchall()
                if statement.lstrip().startswith('SELECT'):
                    result['count'] = rows[0][0]
        except duckdb.InterruptException:
            result['status'] = 'timeout'
        except duckdb.OutOfMemoryException:
            result['status'] = 'memory_out'
        finally:
            result['seconds'] = time.perf_counter() - started
            timer.cancel()
            timer.join()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, default=Path('/data/wcoj_by_sql/datasets/amazon0601.csv'))
    parser.add_argument('--rows', type=int, default=10000, help='first N CSV edges in file order')
    parser.add_argument('--source-root', type=Path,
                        default=Path('/data/Partitioned-Join-Flowlog/robustness/hand_selected_split'))
    parser.add_argument('--plans', nargs='+', default=['5cycle/h1.dl', '5cycle_2/9r.dl',
                        '5cycle_2/9rr.dl', '5clique/h4.dl', '5starfish/h4.dl'])
    parser.add_argument('--threads', type=int, default=32)
    parser.add_argument('--memory', default='8GB')
    parser.add_argument('--timeout', type=float, default=10)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'cte_smoke_results.json')
    args = parser.parse_args()
    if min(args.rows, args.threads, args.timeout, args.repeats) <= 0 or args.warmups < 0:
        parser.error('rows, threads, timeout and repeats must be positive; warmups nonnegative')
    with args.dataset.open() as stream:
        edges = [tuple(map(int, row)) for row in itertools.islice(csv.reader(stream), args.rows)]
    edge_hash = hashlib.sha256(''.join(f'{a},{b}\n' for a, b in edges).encode()).hexdigest()
    report = {'duckdb': duckdb.__version__, 'dataset': str(args.dataset),
              'sample': 'first N CSV edges in file order, then set deduplication; no graph symmetrization',
              'requested_rows': args.rows, 'loaded_rows': len(edges), 'edge_sequence_sha256': edge_hash,
              'distinct_edges': len(set(edges)), 'threads': args.threads, 'memory': args.memory,
              'timeout_seconds': args.timeout, 'repeats': args.repeats, 'warmups': args.warmups,
              'seed': 42, 'timing': 'fresh connections; immutable cached input; all SQL statements, including preprocessing and COUNT; excludes input loading and SQL generation',
              'plans': []}
    rng = random.Random(42)
    with tempfile.TemporaryDirectory(prefix='flowlog_cte_benchmark_') as directory:
        database = Path(directory) / 'input.duckdb'
        with duckdb.connect(str(database)) as con:
            con.execute('CREATE TABLE raw(col0 BIGINT,col1 BIGINT)')
            if edges:
                con.executemany('INSERT INTO raw VALUES (?,?)', edges)
            con.execute('CREATE TABLE R AS SELECT DISTINCT col0,col1 FROM raw')
            con.execute('DROP TABLE raw')
        for relative in args.plans:
            program = parse_program((args.source_root / relative).read_text())
            plans = {'ctas': compile_program(program),
                     'cte_all': compile_cte_program(program, materialization='all'),
                     'cte_auto': compile_cte_program(program)}
            row = {'source': relative, 'source_sha256': plans['ctas'].manifest['source_sha256'],
                   'unsplit_default_duckdb': execute(database, [cq_sql(program)], threads=args.threads,
                                                   timeout=args.timeout, memory=args.memory),
                   'layouts': {layout: {'sql_sha256': hashlib.sha256(plan.sql.encode()).hexdigest(),
                                        'runs': [], 'warmup_runs': []}
                               for layout, plan in plans.items()}}
            for repeat in range(-args.warmups, args.repeats):
                layouts = list(plans)
                rng.shuffle(layouts)
                for layout in layouts:
                    result = execute(database, plans[layout].statements, threads=args.threads,
                                     timeout=args.timeout, memory=args.memory)
                    row['layouts'][layout]['warmup_runs' if repeat < 0 else 'runs'].append(result)
            successful = [run['count'] for layout in row['layouts'].values()
                          for run in layout['runs'] + layout['warmup_runs'] if run['status'] == 'ok']
            baseline = row['unsplit_default_duckdb']
            if baseline['status'] == 'ok':
                successful.append(baseline['count'])
            if len(set(successful)) > 1:
                raise AssertionError(f'Count mismatch: {relative}: {successful}')
            for layout in row['layouts'].values():
                completed = [run['seconds'] for run in layout['runs'] if run['status'] == 'ok']
                layout['median_seconds'] = statistics.median(completed) if len(completed) == args.repeats else None
            old, new = (row['layouts'][layout]['median_seconds'] for layout in ('ctas', 'cte_auto'))
            row['ctas_over_cte_auto'] = old / new if old is not None and new is not None else None
            row['counts_agree'] = bool(successful) and len(set(successful)) == 1
            report['plans'].append(row)
            print(relative, {layout: values['median_seconds'] for layout, values in row['layouts'].items()},
                  'count=', successful[0] if successful else None, flush=True)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
