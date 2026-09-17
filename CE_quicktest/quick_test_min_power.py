"""Compare geometric and additive upper bounds on sum(min(x,y)^p).

Use i=2*p*alpha, j=2*p*(1-alpha), so i+j=2*p. All edge statistics
are collected in one aggregate query after degree preparation.
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


PS = [0.2, 0.25, 1 / 3, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0]
ALPHAS = [0.25, 0.5, 0.75]


def collect(con, ps=PS, alphas=ALPHAS):
    if not ps or any(not math.isfinite(p) or p <= 0 for p in ps):
        raise ValueError("p must be positive and finite")
    if not alphas or any(not math.isfinite(a) or not 0 < a < 1 for a in alphas):
        raise ValueError("alpha must be strictly between zero and one")
    columns = ["COUNT(*)", "SUM(LEAST(x,y))"]
    for p in ps:
        columns += [f"SUM(POW(LEAST(x,y), {p:.17g}))",
                    f"SUM(POW(x,{p:.17g}) + POW(y,{p:.17g}))"]
        columns += [f"SUM(POW(x,{p*a:.17g})*POW(y,{p*(1-a):.17g}))"
                    for a in alphas]
    row = con.execute("SELECT " + ", ".join(columns) + """
        FROM (
            SELECT a.dout::DOUBLE AS x, b.dout::DOUBLE AS y
            FROM cauchy_edges e
            JOIN cauchy_profile a ON e.col0=a.key
            JOIN cauchy_profile b ON e.col1=b.key
        ) pairs
    """).fetchone()
    n, exact_s = int(row[0]), float(row[1] or 0)
    results = []
    offset = 2
    for p in ps:
        exact, additive = (float(value or 0) for value in row[offset:offset+2])
        for idx, a in enumerate(alphas):
            geometric = float(row[offset+2+idx] or 0)
            tolerance = 1e-8 * max(1, additive)
            if not exact <= geometric + tolerance or geometric > additive + tolerance:
                raise AssertionError("min^p <= geometric <= additive failed")
            if a == 0.5 and geometric > additive/2 + tolerance:
                raise AssertionError("Symmetric geometric <= arithmetic mean failed")
            results.append((p, a, exact, geometric, additive))
        offset += 2 + len(alphas)
    return n, exact_s, results


def work_bound(moment, p, n):
    if not moment or not n:
        return 0.0
    # For p<1 subadditivity gives S^p <= sum(m^p); the N factor is invalid.
    log_bound = math.log(moment)/p + (1-1/p)*math.log(n) if p >= 1 else math.log(moment)/p
    return math.exp(log_bound) if log_bound < 709 else math.inf


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="topcats")
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    if args.log:
        args.log.parent.mkdir(parents=True, exist_ok=True)
    with (args.log.open("w") if args.log else nullcontext()) as log, \
            duckdb.connect(config={"memory_limit": "4GB", "threads": 4}) as con:
        report(f"query\t{ROOT/'queries/1.sql'}", log)
        report(f"dataset\t{args.dataset}", log)
        started = perf_counter()
        load_graph(con, ROOT/DATASETS[args.dataset])
        prepare_profile(con)
        report(f"load_and_degree_seconds\t{perf_counter()-started:.6f}", log)
        started = perf_counter()
        n, exact_s, results = collect(con)
        report(f"edge_aggregation_seconds\t{perf_counter()-started:.6f}", log)
        report(f"edge_count\t{n}", log)
        report(f"exact_sum_min\t{exact_s:.12g}", log)
        report("p\ti\tj\texact_sum_min_power\tgeometric_sum\tadditive_sum\tgeometric_over_exact\tadditive_over_exact\tgeometric_bound_on_S\tadditive_bound_on_S", log)
        for p,a,exact,g,s in results:
            bg,bs=work_bound(g,p,n),work_bound(s,p,n)
            if min(bg,bs) + 1e-7*max(1,exact_s) < exact_s:
                raise AssertionError("Converted bound below actual sum(min)")
            ratios=f"{g/exact:.9g}\t{s/exact:.9g}" if exact else "-\t-"
            report(f"{p:.9g}\t{2*p*a:.9g}\t{2*p*(1-a):.9g}\t{exact:.12g}\t{g:.12g}\t{s:.12g}\t{ratios}\t{bg:.12g}\t{bs:.12g}",log)
        report("note\tRows compare bounds on sum(min^p); the last columns convert them to bounds on sum(min).",log)


if __name__ == "__main__":
    main()
