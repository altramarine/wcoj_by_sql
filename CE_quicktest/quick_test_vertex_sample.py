"""Fixed-size independent key samples for Topcats split candidate estimation.

Full degrees and a source adjacency index are built for experimental validation.
This tests accuracy, not sublinear statistics construction in DuckDB.
"""
import argparse
import itertools
import math
from pathlib import Path
from time import perf_counter

import duckdb
import numpy as np

from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def variance(total, rows_sq, cols_sq, edges_sq, va, vb, ka, kb):
    pa,pb = ka/va,kb/vb
    ca = va*(ka-1)/(ka*(va-1)) if va>1 else 1.0
    cb = vb*(kb-1)/(kb*(vb-1)) if vb>1 else 1.0
    aa,ab = 1/pa-ca,1/pb-cb
    return math.fsum([(ca*cb-1)*total*total, aa*cb*rows_sq,
                      ca*ab*cols_sq, aa*ab*edges_sq])


def check_variance():
    w = np.array([[1.,0.,4.],[2.,3.,0.],[0.,5.,2.]])
    for ka,kb in itertools.product(range(1,4),repeat=2):
        estimates = [w[np.ix_(a,b)].sum()*9/(ka*kb)
                     for a in itertools.combinations(range(3),ka)
                     for b in itertools.combinations(range(3),kb)]
        actual = np.var(estimates)
        expected = variance(w.sum(),np.square(w.sum(axis=1)).sum(),
            np.square(w.sum(axis=0)).sum(),np.square(w).sum(),3,3,ka,kb)
        np.testing.assert_allclose(expected,actual,atol=1e-10)
        np.testing.assert_allclose(np.mean(estimates),w.sum(),atol=1e-10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trials',type=int,default=200)
    parser.add_argument('--fraction',type=float)
    parser.add_argument('--log',type=Path,default=ROOT/'log/topcats-1-vertex-sample.log')
    args = parser.parse_args()
    if args.trials<2:
        raise ValueError('At least two trials required')
    if args.fraction is not None and not 0 < args.fraction <= 1:
        raise ValueError('--fraction must be in (0,1]')
    check_variance()
    out = args.log
    out.parent.mkdir(parents=True,exist_ok=True)
    samples_path = out.with_suffix('.trials.tsv')
    with out.open('w') as log, samples_path.open('w') as samples:
        def report(line):
            print(line,flush=True)
            print(line,file=log,flush=True)
        report('method\tindependent uniform fixed-size key sets for A and B; without replacement; exact sampled degrees')
        report('variance_check\tpassed exhaustive subsets of a 3x3 matrix for all 9 sample-size combinations')
        start = perf_counter()
        with duckdb.connect(config={'threads':4,'memory_limit':'4GB'}) as con:
            load_graph(con,ROOT/DATASETS['topcats'])
            edges = con.execute('SELECT DISTINCT col0,col1 FROM R ORDER BY col0,col1').fetchnumpy()
        a,b = edges['col0'],edges['col1']
        n = len(a)
        slots = int(max(a.max(),b.max()))+1
        d = np.bincount(a,minlength=slots)
        din = np.bincount(b,minlength=slots)
        keys_a,keys_b = np.flatnonzero(d),np.flatnonzero(din)
        va,vb = len(keys_a),len(keys_b)
        offsets = np.concatenate(([0],np.cumsum(d[keys_a])))
        assert offsets[-1]==n
        # Exact degree lookup + row ranges are O(N) experimental infrastructure.
        report(f'preparation_with_full_degree_and_adjacency_seconds\t{perf_counter()-start}')
        start = perf_counter()
        w = np.minimum(d[a],d[b]).astype(float)
        total = float(w.sum())
        rows = np.bincount(a,weights=w,minlength=slots)
        cols = np.bincount(b,weights=w,minlength=slots)
        rs,cs,es = float(rows@rows),float(cols@cols),float(w@w)
        assert total==779094625
        del w,rows,cols
        report(f'exact_reference_and_variance_statistics_seconds\t{perf_counter()-start}')
        report(f'N\t{n}\nVa\t{va}\nVb\t{vb}\nexact_S\t{total}\ntrials_per_size\t{args.trials}')
        report(f'row_contribution_square_sum\t{rs}\ncolumn_contribution_square_sum\t{cs}\nedge_contribution_square_sum\t{es}')
        sizes = sorted(set([math.ceil(math.sqrt(n)), math.ceil(math.sqrt(5340*va*vb/n)),50000]))
        if args.fraction is not None:
            assert va == vb, 'Equal-size sampling requires equal key domain sizes'
            sizes = [math.ceil(args.fraction*va)]
            report(f'requested_key_fraction\t{args.fraction}\nactual_key_fraction\t{sizes[0]/va}')
        print('k\ttrial\tretained_edges\tsampled_cost\testimate_S\trelative_error',file=samples)
        report('k_each_side\texpected_retained_edges\tmean_retained_edges\tmin_retained_edges\tmax_retained_edges\tmean_estimate_S\tmean_relative_error\trelative_RMSE\ttheoretical_relative_RMSE\tmedian_absolute_relative_error\tp95_absolute_relative_error\tmax_absolute_relative_error\tmean_query_seconds_with_resident_index\tsampled_key_payload_bytes')
        for k in sizes:
            assert k<=min(va,vb)
            estimates,counts = [],[]
            selected_b = np.zeros(slots,dtype=bool)
            start = perf_counter()
            for trial in range(args.trials):
                seeds = np.random.SeedSequence([20260916,k,trial]).spawn(2)
                ia = np.sort(np.random.default_rng(seeds[0]).choice(va,size=k,replace=False))
                chosen_b = keys_b[np.random.default_rng(seeds[1]).choice(vb,size=k,replace=False)]
                selected_b[chosen_b] = True
                starts = offsets[ia]
                lengths = offsets[ia+1]-starts
                cumulative = np.cumsum(lengths)
                prior = np.concatenate(([0],cumulative[:-1]))
                indices = np.repeat(starts-prior,lengths)+np.arange(cumulative[-1])
                indices = indices[selected_b[b[indices]]]
                count = len(indices)
                cost = int(np.minimum(d[a[indices]],d[b[indices]]).sum())
                estimate = cost*(va/k)*(vb/k)
                estimates.append(estimate)
                counts.append(count)
                selected_b[chosen_b] = False
                print(f'{k}\t{trial}\t{count}\t{cost}\t{estimate:.17g}\t{estimate/total-1:.17g}',file=samples)
            elapsed = (perf_counter()-start)/args.trials
            estimates = np.array(estimates)
            errors = estimates/total-1
            abs_errors = abs(errors)
            theory = math.sqrt(max(0,variance(total,rs,cs,es,va,vb,k,k)))/total
            values = [k,n*(k/va)*(k/vb),np.mean(counts),min(counts),max(counts),
                      estimates.mean(),errors.mean(),np.sqrt(np.mean(errors*errors)),theory,
                      np.median(abs_errors),np.quantile(abs_errors,.95),abs_errors.max(),elapsed,2*k*8]
            report('\t'.join(str(v) for v in values))
        report('limitation\tTimings assume full degree arrays and a prebuilt full adjacency index. This is an accuracy experiment, not a cheap DuckDB online collection algorithm. Estimates are not deterministic upper bounds.')
        report(f'trials_file\t{samples_path}')


if __name__=='__main__':
    main()
