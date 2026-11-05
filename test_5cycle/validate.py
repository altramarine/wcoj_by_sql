#!/usr/bin/env python3
"""Validate every emitted SQL relation against rules, and rules against the CQ."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import random
import time

import duckdb

from .reference import brute_force_query, evaluate
from .translate import compile_program, identifier, parse_program


def graph_fixtures(random_graphs=6, seed=42):
    graphs = [
        ('empty', []),
        ('singleton_loop', [(7, 7)]),
        ('directed_cycle_with_sink', [(0, 1), (1, 2), (2, 3), (3, 4), (4, 0), (0, 5)]),
        ('complete_with_loops', [(a, b) for a in range(4) for b in range(4)]),
        ('nonfunctional_projection_duplicates', [(0, 1), (0, 2), (1, 2), (3, 1), (3, 2),
                                                (2, 4), (1, 4), (4, 0), (4, 3), (0, 1)]),
        ('starfish_sink_center', [(a, (a + 1) % 5) for a in range(5)] + [(a, 5) for a in range(5)]),
    ]
    rng = random.Random(seed)
    for index in range(random_graphs):
        probability = (.25, .55, .8)[index % 3]
        vertices = 4 + index % 2
        edges = [(a, b) for a in range(vertices) for b in range(vertices)
                 if rng.random() < probability]
        graphs.append((f'random_{index}', edges))
    return graphs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path,
                        default=Path('/data/Partitioned-Join-Flowlog/robustness/hand_selected_split'))
    parser.add_argument('--sql-root', type=Path, default=Path(__file__).parent / 'plans')
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'validation.json')
    parser.add_argument('--random-graphs', type=int, default=6)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--layout', choices=('materialized', 'cte'), default='materialized')
    args = parser.parse_args()
    paths = sorted(args.source_root.rglob('*.dl'))
    if not paths:
        parser.error('No upstream .dl programs found')
    fixtures = graph_fixtures(args.random_graphs, args.seed)
    summary = {'duckdb': duckdb.__version__, 'seed': args.seed, 'programs': len(paths), 'layout': args.layout,
               'fixtures': {name: {'rows': len(edges), 'distinct_edges': len(set(edges))}
                            for name, edges in fixtures},
               'comparison': 'every reachable SQL relation vs independent set interpreter; full Result tuples vs brute-force original CQ',
               'plans': [], 'failures': []}
    total_stages = 0
    started = time.perf_counter()
    oracle_cache = {}
    for path in paths:
        relative = path.relative_to(args.source_root)
        program = parse_program(path.read_text())
        if args.layout == 'cte':
            from .cte import compile_cte_program
            plan = compile_cte_program(program)
        else:
            plan = compile_program(program)
        sql_path = args.sql_root / relative.with_suffix('.sql')
        if sql_path.read_text() != plan.sql:
            raise ValueError(f'Saved SQL differs from generated SQL: {sql_path}')
        row = {'source': str(relative), 'source_sha256': plan.manifest['source_sha256'],
               'sql_sha256': hashlib.sha256(plan.sql.encode()).hexdigest(), 'fixtures': []}
        for name, edges in fixtures:
            expected_relations = evaluate(program, edges)
            from .reference import original_query
            original_atoms, head_order = original_query(program)
            oracle_key = (original_atoms, head_order, name)
            if oracle_key not in oracle_cache:
                oracle_cache[oracle_key] = brute_force_query(program, edges)
            original = oracle_cache[oracle_key]
            with duckdb.connect(config={'threads': 1}) as con:
                con.execute('CREATE TABLE R(col0 BIGINT, col1 BIGINT)')
                if edges:
                    con.executemany('INSERT INTO R VALUES (?, ?)', edges)
                answer = None
                # Execute the saved SQL, including the ordered WITH closure.
                for statement in con.extract_statements(sql_path.read_text()):
                    result = con.execute(statement).fetchall()
                    if statement.type == duckdb.StatementType.SELECT:
                        answer = result[0][0]
                if args.layout == 'cte':
                    if con.execute('SHOW TABLES').fetchall() != [('R',)]:
                        raise AssertionError('CTE plan leaked a temporary table')
                    con.execute(plan.statements[0])
                for relation, table in plan.tables.items():
                    query = (plan.relation_query(relation) if args.layout == 'cte'
                             else f'SELECT * FROM {identifier(table)}')
                    actual = con.execute(query).fetchall()
                    expected = expected_relations[relation]
                    total_stages += 1
                    if len(actual) != len(set(actual)) or set(actual) != expected:
                        raise AssertionError(f'SQL/rule mismatch: {relative}/{name}/{relation}')
                actual_result = expected_relations[program.output]
                if answer != len(actual_result):
                    raise AssertionError(f'Scalar count mismatch: {relative}/{name}')
            matches = actual_result == original
            fixture = {'name': name, 'sql_matches_rules': True, 'count': len(actual_result),
                       'original_cq_count': len(original), 'rules_match_original_cq': matches}
            row['fixtures'].append(fixture)
            if not matches:
                summary['failures'].append({'source': str(relative), 'fixture': name,
                    'missing_count': len(original - actual_result), 'extra_count': len(actual_result - original),
                    'missing_examples': sorted(original - actual_result)[:3],
                    'extra_examples': sorted(actual_result - original)[:3]})
        summary['plans'].append(row)
        print(f'{relative}: {len(fixtures)} graphs, SQL/rules agree; '
              f'CQ mismatches={sum(not item["rules_match_original_cq"] for item in row["fixtures"])}', flush=True)
    summary['total_relation_checks'] = total_stages
    summary['total_plan_graph_checks'] = len(paths) * len(fixtures)
    summary['elapsed_seconds'] = time.perf_counter() - started
    summary['mismatching_sources'] = sorted({row['source'] for row in summary['failures']})
    summary['family_counts'] = dict(Counter(path.parent.name for path in paths))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2) + '\n')
    print(f'{summary["total_plan_graph_checks"]} plan/graph checks; {total_stages} relation checks; '
          f'{len(summary["mismatching_sources"])} original-CQ mismatching sources; report: {args.output}')
    if summary['failures']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
