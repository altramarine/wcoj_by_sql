"""Compare uniform edge sampling with the random-sign triangle estimator.

Cached sample stores R1 endpoint keys only. Candidate degree lookups happen
at query time. Full edge statistics below are experimental validation only.
"""
import math
from pathlib import Path
from time import perf_counter

import duckdb
import numpy as np

from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def main():
    started = perf_counter()
    with duckdb.connect(config={'threads': 4, 'memory_limit': '4GB'}) as con:
        load_graph(con, ROOT/DATASETS['topcats'])
        data = con.execute('SELECT DISTINCT col0, col1 FROM R ORDER BY col0, col1').fetchnumpy()
    a, b = data['col0'], data['col1']
    n = len(a)
    degree = np.bincount(a, minlength=int(max(a.max(), b.max()))+1)
    preparation = perf_counter()-started
    truths = {}
    for name, transform in [('K', lambda x,y: np.sqrt(x)*np.sqrt(y)),
                            ('S', np.minimum)]:
        values = transform(degree[a].astype(float), degree[b].astype(float))
        total = float(values.sum())
        square_sum = float(values @ values)
        truths[name] = total, square_sum, n*square_sum/total**2-1
        del values
    path = ROOT/'log/topcats-1-edge-sample.log'
    with path.open('w') as log:
        def report(k, v):
            line = f'{k}\t{v}'
            print(line, flush=True)
            print(line, file=log, flush=True)
        report('n', n)
        report('preparation_seconds', preparation)
        report('sampling', 'uniform, with replacement; seed 20260915')
        report('variance_formula', 'Var(estimate)/total^2 = (n*sum(h^2)/total^2 - 1)/m')
        for name, (total, square_sum, cv2) in truths.items():
            report(f'{name}_exact', total)
            report(f'{name}_square_sum', square_sum)
            report(f'{name}_CV_squared', cv2)
        for m in [math.ceil(math.sqrt(n)), math.ceil(n/math.log(n))]:
            rng = np.random.default_rng(20260915)
            start = perf_counter()
            indices = rng.integers(n, size=m)
            # This pair of arrays is the only R1 sample cache needed.
            sample_a, sample_b = a[indices].copy(), b[indices].copy()
            sample_seconds = perf_counter()-start
            start = perf_counter()
            x, y = degree[sample_a].astype(float), degree[sample_b].astype(float)
            estimates = {'K': float(n*np.mean(np.sqrt(x)*np.sqrt(y))),
                         'S': float(n*np.mean(np.minimum(x,y)))}
            query_seconds = perf_counter()-start
            report('m', m)
            report('sample_from_resident_edges_seconds', sample_seconds)
            report('degree_lookup_and_estimation_seconds', query_seconds)
            report('sample_endpoint_bytes', sample_a.nbytes+sample_b.nbytes)
            for name, estimate in estimates.items():
                total, _, cv2 = truths[name]
                report(f'{name}_estimate', estimate)
                report(f'{name}_relative_error', estimate/total-1)
                report(f'{name}_relative_standard_error', math.sqrt(cv2/m))
        report('warning', 'Estimates, not deterministic upper bounds. Timings assume resident edge arrays and degree arrays; streaming cache construction needs a pass over R1.')


if __name__ == '__main__':
    main()
