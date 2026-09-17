"""Bound negative-mean edge sums using exact weighted marginal distributions.

Sorted matching maximizes the sum over all couplings; opposite matching minimizes
it. No actual edge pairing is read during bound evaluation. Equal-degree
compression is exact, not approximate binning. --exact is separate validation.
"""

import argparse
import json
import math
from contextlib import nullcontext
from pathlib import Path
from time import perf_counter

import duckdb

from lp_statistics import load_graph
from quick_test_cauchy import ROOT, prepare_profile
from quick_test_negative_mean import KS, collect
from quick_test_lp import report
from run_sql import DATASETS


def match(left, right, reverse=False):
    left = sorted((d, n) for d, n in left if n)
    right = sorted(((d, n) for d, n in right if n), reverse=reverse)
    if sum(n for d,n in left) != sum(n for d,n in right):
        raise ValueError("Marginal occurrence counts must match")
    if not left:
        return []
    i = j = 0
    a, b = left[0][1], right[0][1]
    pairs = []
    while i < len(left) and j < len(right):
        n = min(a,b)
        pairs.append((left[i][0],right[j][0],n))
        a -= n
        b -= n
        if not a:
            i += 1
            if i < len(left): a = left[i][1]
        if not b:
            j += 1
            if j < len(right): b = right[j][1]
    return pairs


def negative_mean(x, y, k):
    lo, hi = min(x,y), max(x,y)
    if not lo or math.isinf(k):
        return float(lo)
    return lo * math.exp((math.log(2)-math.log1p((lo/hi)**k))/k)


def evaluate(pairs, k):
    return math.fsum(n*negative_mean(x,y,k) for x,y,n in pairs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--dataset", choices=sorted(DATASETS), default="topcats")
    source.add_argument("--profile", type=Path, help="reuse saved marginals without reading graph or degree tables")
    parser.add_argument("--save-profile", type=Path, help="save exact marginals for subsequent bound evaluations")
    parser.add_argument("--exact", action="store_true")
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    if args.profile and args.exact:
        parser.error("--exact requires the graph; omit --profile for validation")
    if args.log: args.log.parent.mkdir(parents=True,exist_ok=True)
    with (args.log.open("w") if args.log else nullcontext()) as log, \
            (nullcontext() if args.profile else duckdb.connect(config={"memory_limit":"4GB","threads":4})) as con:
        if args.profile:
            started=perf_counter()
            saved=json.loads(args.profile.read_text())
            rows=saved["rows"]
            dataset=saved["dataset"]
            report(f"profile_load_seconds\t{perf_counter()-started:.6f}",log)
        else:
            dataset=args.dataset
            started = perf_counter()
            load_graph(con,ROOT/DATASETS[dataset])
            prepare_profile(con)
            report(f"load_and_degree_seconds\t{perf_counter()-started:.6f}",log)
            started = perf_counter()
            rows = con.execute("""
                SELECT dout, SUM(dout), SUM(din)
                FROM cauchy_profile GROUP BY dout ORDER BY dout
            """).fetchall()
            report(f"marginal_collection_seconds\t{perf_counter()-started:.6f}",log)
        report(f"dataset\t{dataset}",log)
        left=[(int(d),int(a)) for d,a,b in rows if a]
        right=[(int(d),int(b)) for d,a,b in rows if b]
        if args.save_profile:
            args.save_profile.parent.mkdir(parents=True,exist_ok=True)
            args.save_profile.write_text(json.dumps({"dataset":dataset,"rows":rows}))
        report(f"distinct_source_degrees\t{len(left)}",log)
        report(f"distinct_destination_degrees\t{len(right)}",log)
        report(f"edge_count\t{sum(n for d,n in left)}",log)
        started = perf_counter()
        upper_pairs=match(left,right)
        lower_pairs=match(left,right,reverse=True)
        estimates=[(k,evaluate(lower_pairs,k),evaluate(upper_pairs,k)) for k in [*KS,math.inf]]
        report(f"coupling_and_all_bounds_seconds\t{perf_counter()-started:.6f}",log)
        report(f"upper_coupling_segments\t{len(upper_pairs)}",log)
        report(f"best_fixed_candidates\t{min(sum(d*n for d,n in left),sum(d*n for d,n in right))}",log)
        actuals={}
        if args.exact:
            started=perf_counter()
            _,exact,_,means=collect(con)
            actuals=dict(zip(KS,means))
            actuals[math.inf]=exact
            report(f"exact_validation_seconds\t{perf_counter()-started:.6f}",log)
        report("k (inf=min)\tcoupling_lower\tactual_B_k\tcoupling_upper\tupper_over_actual",log)
        for k,lower,upper in estimates:
            if k in actuals:
                actual=actuals[k]
                tol=1e-8*max(1,upper)
                if not lower-tol<=actual<=upper+tol:
                    raise AssertionError("Actual edge sum outside coupling bounds")
                actual_text=f"{actual:.12g}"
                ratio=f"{upper/actual:.9g}" if actual else "-"
            else: actual_text=ratio="-"
            report(f"{k:g}\t{lower:.12g}\t{actual_text}\t{upper:.12g}\t{ratio}",log)
        report("note\tCoupling relaxes the actual edge structure. Requires cached weighted distributions, not only scalar norms.",log)


if __name__ == "__main__":
    main()
