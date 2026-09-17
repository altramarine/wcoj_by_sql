"""Diagnose tail weights and pairing bias; full scans are validation/preparation.

Histograms below are weighted by actual R1 endpoint occurrences. They are
cross-relation statistics, NOT available from independent degree histograms.
"""
from pathlib import Path
import duckdb
import numpy as np
from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def independent_min(x, y):
    if not len(x) or not len(y):
        return 0.0
    size = int(max(x.max(), y.max()))+1
    px = np.bincount(x, minlength=size)/len(x)
    py = np.bincount(y, minlength=size)/len(y)
    # E[min(X,Y)] = sum_{t>=1} P(X>=t) P(Y>=t), under independence.
    sx = np.cumsum(px[::-1])[::-1]
    sy = np.cumsum(py[::-1])[::-1]
    return float(sx[1:] @ sy[1:])


def main():
    with duckdb.connect(config={'threads':4, 'memory_limit':'4GB'}) as con:
        load_graph(con, ROOT/DATASETS['topcats'])
        data = con.execute('SELECT DISTINCT col0,col1 FROM R').fetchnumpy()
    a,b = data['col0'],data['col1']
    d = np.bincount(a,minlength=int(max(a.max(),b.max()))+1)
    active = np.flatnonzero(d)
    rank = active[np.lexsort((active,-d[active]))]
    x,y = d[a],d[b]
    costs = np.minimum(x,y)
    exact = float(costs.sum())
    with (ROOT/'log/topcats-1-tail-distribution.log').open('w') as log:
        def report(s):
            print(s,flush=True)
            print(s,file=log,flush=True)
        report(f'exact_S={exact}; full_population_comparisons_without_sampling_noise')
        report('H\tmax_tail\tweighted_mean_x\tweighted_mean_y\tweighted_means_bias\tglobal_tail_distributions_bias\tconditional_tail_distributions_bias\tLL_actual_mean_min\tLL_independent_mean_min\tLL_mean_x\tLL_mean_y\tLL_edge_count')
        for H in [1000,5340,10000,50000]:
            heavy = np.zeros(len(d),dtype=bool)
            heavy[rank[:H]]=True
            hx,hy = heavy[a],heavy[b]
            hh,hl,lh,ll = hx&hy,hx&~hy,~hx&hy,~hx&~hy
            tx,ty = x[~hx],y[~hy]
            mx,my = float(tx.mean()),float(ty.mean())
            fixed = float(costs[hh].sum())
            approx_means = fixed + hl.sum()*my + lh.sum()*mx + ll.sum()*min(mx,my)
            approx_distributions = fixed + hl.sum()*my + lh.sum()*mx + ll.sum()*independent_min(tx,ty)
            llx,lly=x[ll],y[ll]
            ind=independent_min(llx,lly)
            conditional = float(costs[~ll].sum()) + ll.sum()*ind
            row=[H,int(d[rank[H]]),mx,my,approx_means/exact-1,
                 approx_distributions/exact-1,conditional/exact-1,
                 float(costs[ll].mean()),ind,float(llx.mean()),float(lly.mean()),int(ll.sum())]
            report('\t'.join(str(v) for v in row))
        report('NOTE: conditional version uses exact non-LL subtotals to isolate LL pairing error; none of these are cheap online collection procedures.')


if __name__=='__main__':
    main()
