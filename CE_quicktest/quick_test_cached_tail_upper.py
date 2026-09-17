"""Deterministic tail min bound from cached exact occurrence histograms only."""
import json
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parent.parent


def sorted_coupling(hx, hy):
    """Independent check: greedily pair sorted degree occurrences."""
    x, y = sorted(hx.items()), sorted(hy.items())
    i = j = total = 0
    if not x or not y:
        return 0
    rx, ry = x[0][1], y[0][1]
    while i < len(x) and j < len(y):
        count = min(rx, ry)
        total += count*min(x[i][0],y[j][0])
        rx -= count
        ry -= count
        if not rx:
            i += 1
            if i < len(x):
                rx = x[i][1]
        if not ry:
            j += 1
            if j < len(y):
                ry = y[j][1]
    return total


def main():
    started = perf_counter()
    cache = json.loads((ROOT/'log/topcats-1-heavy-exact-tail.statistics.json').read_text())
    hx = {int(k):v for k,v in cache['tail_x_counts'].items()}
    hy = {int(k):v for k,v in cache['tail_y_counts'].items()}
    n = cache['tail_count']
    assert sum(hx.values()) == sum(hy.values()) == n
    read_seconds = perf_counter()-started
    started = perf_counter()
    sx = sy = n
    upper = 0
    independent_numerator = 0
    for t in range(1,max(max(hx,default=0),max(hy,default=0))+1):
        sx -= hx.get(t-1,0)
        sy -= hy.get(t-1,0)
        upper += min(sx,sy)
        independent_numerator += sx*sy
    compute_seconds = perf_counter()-started
    assert upper == sorted_coupling(hx,hy)
    records = dict(line.split('\t',1) for line in (ROOT/'log/topcats-1-heavy-exact-tail.log').read_text().splitlines())
    exact_tail = int(records['tail_exact_cost'])
    exact_total = int(records['total_exact_cost'])
    heavy = cache['heavy_exact_cost']
    assert upper >= exact_tail
    out = {
        'input':'cached exact tail occurrence histograms; no raw table or full degrees accessed',
        'heavy_exact_cost':heavy,'tail_exact_cost':exact_tail,
        'tail_independence_estimate':independent_numerator/n,
        'tail_upper':upper,'tail_upper_over_exact':upper/exact_tail,
        'total_exact_cost':exact_total,
        'total_independence_estimate':heavy+independent_numerator/n,
        'total_upper':heavy+upper,
        'total_upper_relative_slack':(heavy+upper)/exact_total-1,
        'tail_sum_x':sum(d*c for d,c in hx.items()),
        'tail_sum_y':sum(d*c for d,c in hy.items()),
        'original_LpBound_output_upper':2112368511,
        'reduction_vs_original_LpBound':1-(heavy+upper)/2112368511,
        'previous_unsplit_marginal_upper':1871015295,
        'reduction_vs_unsplit_marginal_upper':1-(heavy+upper)/1871015295,
        'read_cache_seconds':read_seconds,'compute_bound_seconds':compute_seconds,
        'verification':'integer survival-sum equals sorted-occurrence coupling; upper >= exact tail',
        'limitation':'cached inputs were built using full degrees and O(N) edge facts in previous experiment; this does not solve statistics collection cost',
    }
    with (ROOT/'log/topcats-1-cached-tail-upper.log').open('w') as log:
        for k,v in out.items():
            print(f'{k}\t{v}',flush=True)
            print(f'{k}\t{v}',file=log)


if __name__=='__main__':
    main()
