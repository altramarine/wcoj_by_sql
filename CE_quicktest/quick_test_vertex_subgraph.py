"""Run the triangle query on 1% induced vertex subgraphs of Topcats."""
import argparse
import math
from pathlib import Path
from time import perf_counter

import duckdb

from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fraction', type=float, default=.01)
    parser.add_argument('--trials', type=int, default=10)
    parser.add_argument('--seed', type=int, default=20260916)
    parser.add_argument('--log', type=Path, default=ROOT/'log/topcats-1-vertex-subgraph-1pct.log')
    args = parser.parse_args()
    if not 0 < args.fraction <= 1 or args.trials < 1:
        raise ValueError('fraction must be in (0,1] and trials must be positive')
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with args.log.open('w') as log, duckdb.connect(config={'threads':4,'memory_limit':'4GB'}) as con:
        def report(line):
            print(line, flush=True)
            print(line, file=log, flush=True)
        started = perf_counter()
        load_graph(con, ROOT/DATASETS['topcats'])
        report(f'load_seconds\t{perf_counter()-started:.6f}')
        total_vertices = con.execute('''
            SELECT COUNT(*) FROM (
                SELECT col0 AS v FROM R UNION SELECT col1 AS v FROM R
            )
        ''').fetchone()[0]
        sample_size = math.ceil(args.fraction*total_vertices)
        report(f'fraction\t{args.fraction}\ntotal_vertices\t{total_vertices}\nsampled_vertices\t{sample_size}')
        report('trial\tseed\tvertices\tedges\ttriangles\tsplit_subgraph\tsplit_full_degree_on_sampled_edges\ttriangles_scaled_p^-3\tsplit_subgraph_scaled_p^-3\tsplit_full_degree_scaled_p^-2\tseconds')
        for trial in range(args.trials):
            seed = args.seed+trial
            started = perf_counter()
            con.execute('DROP TABLE IF EXISTS sample_vertices')
            con.execute(f'''
                CREATE TEMP TABLE sample_vertices AS
                SELECT v FROM (
                    SELECT col0 AS v FROM R UNION SELECT col1 AS v FROM R
                )
                USING SAMPLE reservoir({sample_size} ROWS) REPEATABLE({seed})
            ''')
            con.execute('DROP TABLE IF EXISTS sample_edges')
            con.execute('''
                CREATE TEMP TABLE sample_edges AS
                SELECT DISTINCT r.col0, r.col1
                FROM R r
                SEMI JOIN sample_vertices va ON r.col0=va.v
                SEMI JOIN sample_vertices vb ON r.col1=vb.v
            ''')
            full_split = con.execute('''
                WITH d AS (SELECT col0 AS v, COUNT(*)::DOUBLE AS d FROM R GROUP BY col0)
                SELECT COALESCE(SUM(LEAST(COALESCE(da.d,0),COALESCE(db.d,0))),0)
                FROM R e
                JOIN sample_vertices va ON e.col0=va.v
                JOIN sample_vertices vb ON e.col1=vb.v
                LEFT JOIN d da ON e.col0=da.v
                LEFT JOIN d db ON e.col1=db.v
            ''').fetchone()[0]
            vertices, edges = con.execute('SELECT COUNT(*), (SELECT COUNT(*) FROM sample_edges) FROM sample_vertices').fetchone()
            triangles = con.execute('''
                SELECT COUNT(*) FROM sample_edges r1
                JOIN sample_edges r2 ON r1.col1=r2.col0
                JOIN sample_edges r3 ON r1.col0=r3.col0 AND r2.col1=r3.col1
            ''').fetchone()[0]
            split = con.execute('''
                WITH d AS (SELECT col0 AS v, COUNT(*)::DOUBLE AS d FROM sample_edges GROUP BY col0)
                SELECT COALESCE(SUM(LEAST(COALESCE(da.d,0),COALESCE(db.d,0))),0)
                FROM sample_edges e
                LEFT JOIN d da ON e.col0=da.v
                LEFT JOIN d db ON e.col1=db.v
            ''').fetchone()[0]
            p=args.fraction
            elapsed=perf_counter()-started
            report('\t'.join(map(str,[trial,seed,vertices,edges,triangles,split,full_split,
                triangles/(p**3),float(split)/(p**3),float(full_split)/(p**2),f'{elapsed:.6f}'])))
        report('note\tThis is an induced subgraph experiment. Scaling by p^-3 is exact in expectation for triangle count under independent vertex sampling, but split_candidates do not have an exact p^-3 scaling law because sampled degrees shrink.')


if __name__ == '__main__':
    main()
