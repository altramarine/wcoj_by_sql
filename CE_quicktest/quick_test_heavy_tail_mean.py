"""Topcats: exact top-H degrees plus a per-key mean for the remaining degrees.

Full graph/degree arrays are used ONLY to build experimental statistics and
validate bias. Query simulations use edge samples and the compressed summary.
"""
from pathlib import Path
import math
import duckdb
import numpy as np
from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def lookup(keys, ids, degrees, mean):
    pos = np.searchsorted(ids, keys)
    safe = np.minimum(pos, len(ids)-1)
    return np.where((pos < len(ids)) & (ids[safe] == keys), degrees[safe], mean)


def main():
    with duckdb.connect(config={'threads': 4, 'memory_limit': '4GB'}) as con:
        load_graph(con, ROOT/DATASETS['topcats'])
        data = con.execute('SELECT DISTINCT col0, col1 FROM R ORDER BY col0, col1').fetchnumpy()
    a, b = data['col0'], data['col1']
    n = len(a)
    d = np.bincount(a, minlength=int(max(a.max(), b.max()))+1)
    active = np.flatnonzero(d)
    rank = active[np.lexsort((active, -d[active]))]
    exact_cost = np.minimum(d[a], d[b])
    exact = float(exact_cost.sum())
    m, trials = math.ceil(math.sqrt(n)), 200
    # Only endpoint arrays, no candidate degrees, enter the simulated planner.
    indices = np.concatenate([np.random.default_rng(np.random.SeedSequence([20260916,m,t])).integers(n,size=m) for t in range(trials)])
    sample_a, sample_b = a[indices], b[indices]
    baseline = n*np.minimum(d[sample_a], d[sample_b]).reshape(trials,m).mean(axis=1)
    path = ROOT/'log/topcats-1-heavy-tail-mean.log'
    with path.open('w') as log:
        def report(line):
            print(line, flush=True)
            print(line, file=log, flush=True)
        report(f'n={n}; exact_S={exact}; positive_degree_keys={len(active)}; m={m}; trials={trials}')
        report('tail_mean_definition=sum(omitted positive degrees)/count(omitted positive keys); unseen keys also receive this mean')
        report(f'exact_degree_sample_p95_abs_error={np.quantile(abs(baseline/exact-1),.95)}')
        report('H\ttail_mean\tmax_omitted_degree\tfull_substitution_relative_bias\tsample_mean_relative_error\tsample_p95_absolute_relative_error\theavy_heavy_true_cost_fraction\tdegree_summary_payload_bytes')
        for H in [100, 1000, m, 10000, 50000, 100000]:
            ids = np.sort(rank[:H])
            vals = d[ids].astype(float)
            mu = (float(d.sum())-float(vals.sum()))/(len(active)-H)
            # Full substitution below isolates approximation bias, no sampling.
            approx = np.full(len(d), mu)
            approx[ids] = vals
            substituted = float(np.minimum(approx[a],approx[b]).sum())
            heavy = np.zeros(len(d), dtype=bool)
            heavy[ids] = True
            hh = float(exact_cost[heavy[a]&heavy[b]].sum())/exact
            x = lookup(sample_a, ids, vals, mu)
            y = lookup(sample_b, ids, vals, mu)
            estimates = n*np.minimum(x,y).reshape(trials,m).mean(axis=1)
            errors = estimates/exact-1
            row = [H, mu, int(d[rank[H]]), substituted/exact-1,
                   float(errors.mean()), float(np.quantile(abs(errors),.95)),
                   hh, ids.nbytes+vals.nbytes+8]
            report('\t'.join(str(v) for v in row))
        report('NOTE: fixed self-join reuses one degree summary for both aliases; no claim of bounded-memory summary construction or deterministic upper bounds.')


if __name__ == '__main__':
    main()
