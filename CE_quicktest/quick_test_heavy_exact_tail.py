"""DuckDB diagnostic: exact heavy-key contribution, separate tail histograms.

This deliberately materializes O(N) edge facts for the requested brute-force
experiment. It is not an implementation of cheap online statistics collection.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import duckdb

from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--heavy', type=int, default=5340)
    parser.add_argument('--log', type=Path, default=ROOT/'log/topcats-1-heavy-exact-tail.log')
    args = parser.parse_args()
    if args.heavy <= 0:
        raise ValueError('--heavy must be positive')
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with args.log.open('w') as log, duckdb.connect(config={'threads':4,'memory_limit':'4GB'}) as con:
        def report(k, v):
            line = f'{k}\t{v}'
            print(line, flush=True)
            print(line, file=log, flush=True)
        def timed_sql(label, sql):
            start = perf_counter()
            result = con.execute(sql)
            report(label+'_seconds', perf_counter()-start)
            return result
        report('dataset', 'topcats')
        report('query', 'queries/1.sql')
        report('duckdb', duckdb.__version__)
        report('heavy_keys', args.heavy)
        report('semantics', 'distinct edges; top-H outdegree keys, ties by key; both aliases reuse same degree profile')
        start = perf_counter()
        load_graph(con, ROOT/DATASETS['topcats'])
        report('load_seconds', perf_counter()-start)
        timed_sql('deduplicate', 'CREATE TEMP TABLE edges AS SELECT DISTINCT col0 a,col1 b FROM R')
        timed_sql('degree_and_heavy_preparation', f'''
            CREATE TEMP TABLE degrees AS
            WITH counts AS (SELECT a, COUNT(*) d FROM edges GROUP BY a)
            SELECT a,d,ROW_NUMBER() OVER (ORDER BY d DESC,a) <= {args.heavy} heavy
            FROM counts
        ''')
        timed_sql('materialize_all_edge_facts', '''
            CREATE TEMP TABLE facts AS
            SELECT COALESCE(da.d,0) x,COALESCE(db.d,0) y,
                   COALESCE(da.heavy,FALSE) ha,COALESCE(db.heavy,FALSE) hb
            FROM edges e LEFT JOIN degrees da ON e.a=da.a
                         LEFT JOIN degrees db ON e.b=db.a
        ''')
        start = perf_counter()
        heavy_rows = con.execute('''
            SELECT ha,hb,COUNT(*) AS n,SUM(LEAST(x,y)) AS total_cost
            FROM facts WHERE ha OR hb GROUP BY ha,hb ORDER BY ha,hb
        ''').fetchall()
        report('heavy_exact_aggregate_seconds', perf_counter()-start)
        heavy_cost = heavy_count = 0
        for ha,hb,n,cost in heavy_rows:
            label = ('H' if ha else 'L')+('H' if hb else 'L')
            report(label+'_edges', n)
            report(label+'_exact_cost', cost)
            heavy_count += n
            heavy_cost += int(cost)
        start = perf_counter()
        histogram_rows = con.execute('''
            SELECT GROUPING(x) gx,x,y,COUNT(*) cnt
            FROM facts WHERE NOT ha AND NOT hb
            GROUP BY GROUPING SETS ((x),(y))
        ''').fetchall()
        report('tail_histogram_collection_seconds', perf_counter()-start)
        hx,hy = {},{}
        for gx,x,y,count in histogram_rows:
            (hy if gx else hx)[int(y if gx else x)] = int(count)
        tail_count = sum(hx.values())
        assert tail_count == sum(hy.values())
        start = perf_counter()
        sx,sy = tail_count,tail_count
        numerator = 0
        tau = max(max(hx,default=0),max(hy,default=0))
        for t in range(1,tau+1):
            sx -= hx.get(t-1,0)
            sy -= hy.get(t-1,0)
            numerator += sx*sy
        tail_estimate = numerator/tail_count if tail_count else 0.0
        report('cached_tail_estimate_seconds', perf_counter()-start)
        start = perf_counter()
        exact_count,exact_tail = con.execute('''
            SELECT COUNT(*),COALESCE(SUM(LEAST(x,y)),0)
            FROM facts WHERE NOT ha AND NOT hb
        ''').fetchone()
        report('tail_exact_validation_seconds', perf_counter()-start)
        assert exact_count == tail_count
        exact_tail = int(exact_tail)
        exact_total = heavy_cost+exact_tail
        independent_total = heavy_cost+tail_estimate
        assert exact_total == 779094625, 'Topcats split cost differs from established reference'
        for k,v in {
            'N':heavy_count+tail_count,'heavy_edge_count':heavy_count,
            'heavy_edge_fraction':heavy_count/(heavy_count+tail_count),
            'heavy_exact_cost':heavy_cost,'tail_edge_count':tail_count,
            'tail_max_degree':tau,'tail_histogram_entries':len(hx)+len(hy),
            'tail_exact_cost':exact_tail,'tail_independence_estimate':tail_estimate,
            'tail_relative_error':tail_estimate/exact_tail-1,
            'total_exact_cost':exact_total,'total_estimate':independent_total,
            'total_relative_error':independent_total/exact_total-1,
        }.items():
            report(k,v)
        cache_path = args.log.with_suffix('.statistics.json')
        cache_path.write_text(json.dumps({'heavy_keys':args.heavy,'N':heavy_count+tail_count,
            'heavy_edge_count':heavy_count,'heavy_exact_cost':heavy_cost,
            'tail_count':tail_count,'tail_x_counts':hx,'tail_y_counts':hy},sort_keys=True))
        report('cached_statistics_file', cache_path)
        report('cached_statistics_json_bytes', cache_path.stat().st_size)
        report('limitation', 'Full O(N) edge facts and degree joins used in preparation; final estimate reads only cached summary. Independence estimate is not a guaranteed upper bound.')


if __name__ == '__main__':
    main()
