"""Test negative power means for min, and reject a naive negative-p LP extension.

The pairwise mean is a valid bound on min. Substituting negative p into the
positive-p entropy inequality is not valid; a counterexample is logged first.
"""

import argparse
import math
from contextlib import nullcontext
from pathlib import Path
from time import perf_counter

import duckdb

from lp_statistics import load_graph
from quick_test_cauchy import ROOT, prepare_profile
from quick_test_lp import report
from run_sql import DATASETS


KS = [0.25, 0.5, 1, 2, 4, 8, 16, 32]


def counterexamples():
    # Relation degrees (1,4). Put U entirely on the degree-4 key and V uniform
    # on its four neighbors: H(U)=0, H(V|U)=log(4).
    results = []
    for k in [1, 2, 4]:
        lp_order = -k
        lhs = math.log(4)
        rhs = math.log(1 + 4**lp_order) / lp_order
        normalized_rhs = math.log((1 + 4**lp_order)/2) / lp_order
        assert lhs > rhs and lhs > normalized_rhs
        results.append((lp_order, lhs, rhs, normalized_rhs))
    return results


def collect(con, ks=KS):
    if not ks or any(not math.isfinite(k) or k <= 0 for k in ks):
        raise ValueError("k must be positive and finite")
    columns = ["COUNT(*)", "SUM(lo)", "SUM(SQRT(lo*hi))"]
    # Stable equivalent of ((x^-k+y^-k)/2)^(-1/k). Zero degree uses its limit 0.
    columns += [f"SUM(CASE WHEN lo=0 THEN 0 ELSE lo * "
                f"EXP((LN(2.0)-LN(1+POW(lo/hi,{k:.17g})))/{k:.17g}) END)"
                for k in ks]
    row = con.execute("SELECT " + ", ".join(columns) + """
        FROM (
            SELECT LEAST(a.dout,b.dout)::DOUBLE AS lo,
                   GREATEST(a.dout,b.dout)::DOUBLE AS hi
            FROM cauchy_edges e
            JOIN cauchy_profile a ON e.col0=a.key
            JOIN cauchy_profile b ON e.col1=b.key
        ) pairs
    """).fetchone()
    return int(row[0]), float(row[1] or 0), float(row[2] or 0), [float(x or 0) for x in row[3:]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="topcats")
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    if args.log:
        args.log.parent.mkdir(parents=True, exist_ok=True)
    with (args.log.open("w") if args.log else nullcontext()) as log, \
            duckdb.connect(config={"memory_limit": "4GB", "threads": 4}) as con:
        report("negative_p_LP_extension\tINVALID: counterexample degrees=(1,4), H(U)=0, H(V|U)=ln(4)",log)
        report("LP_order\tactual_entropy_LHS\tnaive_log_norm_RHS\tnormalized_log_mean_RHS",log)
        for values in counterexamples():
            report("\t".join(f"{value:.12g}" for value in values),log)
        report(f"query\t{ROOT/'queries/1.sql'}",log)
        report(f"dataset\t{args.dataset}",log)
        started = perf_counter()
        load_graph(con, ROOT/DATASETS[args.dataset])
        prepare_profile(con)
        report(f"load_and_degree_seconds\t{perf_counter()-started:.6f}",log)
        started = perf_counter()
        n, exact, geometric, bounds = collect(con)
        report(f"pairwise_aggregation_seconds\t{perf_counter()-started:.6f}",log)
        report(f"edge_count\t{n}",log)
        report(f"exact_sum_min\t{exact:.12g}",log)
        report(f"geometric_sum\t{geometric:.12g}",log)
        report("mean_order\tupper_bound_on_sum_min\tbound_over_actual\tguaranteed_max_ratio",log)
        previous = geometric
        for k,bound in zip(KS,bounds):
            tolerance = 1e-8*max(1,previous,exact)
            if bound+tolerance<exact or bound>previous+tolerance or bound>2**(1/k)*exact+tolerance:
                raise AssertionError("Negative-mean bound or monotonicity failed")
            previous=bound
            ratio=f"{bound/exact:.9g}" if exact else "-"
            report(f"{-k:.9g}\t{bound:.12g}\t{ratio}\t{2**(1/k):.9g}",log)
        report("note\tThese are pairwise-mean bounds, not negative-p LpBound outputs. Exact aggregation is not cheaper than min.",log)


if __name__ == "__main__":
    main()
