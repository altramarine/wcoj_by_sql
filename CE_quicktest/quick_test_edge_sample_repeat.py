"""Repeated uniform-with-replacement edge sampling for Topcats triangle."""
import math
from pathlib import Path
from time import perf_counter

import duckdb
import numpy as np

from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def main():
    truth = {}
    for line in (ROOT/'log/topcats-1-edge-sample.log').read_text().splitlines():
        k, _, v = line.partition('\t')
        if k in ('K_exact', 'S_exact', 'K_CV_squared', 'S_CV_squared'):
            truth[k] = float(v)
    with duckdb.connect(config={'threads': 4, 'memory_limit': '4GB'}) as con:
        load_graph(con, ROOT/DATASETS['topcats'])
        data = con.execute('SELECT DISTINCT col0, col1 FROM R ORDER BY col0, col1').fetchnumpy()
    a, b = data['col0'], data['col1']
    n = len(a)
    degree = np.bincount(a, minlength=int(max(a.max(), b.max()))+1)
    sizes = [100, 500, 1000, math.ceil(math.sqrt(n)), 10000, 50000, math.ceil(n/math.log(n))]
    trials = 200
    logpath = ROOT/'log/topcats-1-edge-sample-repeat.log'
    samplepath = ROOT/'log/topcats-1-edge-sample-repeat.trials.tsv'
    with logpath.open('w') as log, samplepath.open('w') as samples:
        def report(line):
            print(line, flush=True)
            print(line, file=log, flush=True)
        report(f'n={n}; trials_per_size={trials}; seed=SeedSequence([20260916,m,trial]); uniform_with_replacement')
        report('m\ttarget\tmean_relative_error\trelative_RMSE\ttheory_relative_RMSE\tmedian_absolute_relative_error\tp95_absolute_relative_error\tmax_absolute_relative_error\tfraction_within_1pct\tfraction_within_5pct\tmean_sample_and_query_seconds')
        print('m\ttrial\tK_relative_error\tS_relative_error', file=samples)
        for m in sizes:
            errors = {'K': [], 'S': []}
            started = perf_counter()
            for trial in range(trials):
                rng = np.random.default_rng(np.random.SeedSequence([20260916, m, trial]))
                indices = rng.integers(n, size=m)
                x = degree[a[indices]].astype(float)
                y = degree[b[indices]].astype(float)
                estimates = {'K': n*np.mean(np.sqrt(x)*np.sqrt(y)),
                             'S': n*np.mean(np.minimum(x,y))}
                for key, estimate in estimates.items():
                    errors[key].append(estimate/truth[key+'_exact']-1)
                print(f'{m}\t{trial}\t{errors["K"][-1]:.17g}\t{errors["S"][-1]:.17g}', file=samples)
            elapsed = (perf_counter()-started)/trials
            for key, vals in errors.items():
                e = np.array(vals)
                ae = np.abs(e)
                row = [m, key, np.mean(e), np.sqrt(np.mean(e*e)),
                       math.sqrt(truth[key+'_CV_squared']/m), np.median(ae),
                       np.quantile(ae,.95), np.max(ae), np.mean(ae<=.01),
                       np.mean(ae<=.05), elapsed]
                report('\t'.join(str(v) for v in row))
        report('NOTE: timings assume resident edges and degree arrays, include sampling and both target estimates; empirical percentiles are not guarantees.')


if __name__ == '__main__':
    main()
