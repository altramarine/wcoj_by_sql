"""Test asymmetric Cauchy bounds for the first split of queries/1.sql.

Moment collection uses one aggregate pass over the shared-key degree profile.
--exact separately scans edges to validate sum(min); it is not planning cost.
"""

import argparse
import math
from contextlib import nullcontext
from pathlib import Path
from time import perf_counter

import duckdb

from lp_statistics import load_graph
from quick_test_lp import report
from run_sql import DATASETS


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PS = sorted({i / 20 for i in range(1, 20)} | {1 / 3, 2 / 3})


def prepare_profile(con):
    con.execute("CREATE TEMP TABLE cauchy_edges AS SELECT DISTINCT col0, col1 FROM R")
    con.execute("""
        CREATE TEMP TABLE cauchy_profile AS
        WITH outgoing AS (
            SELECT col0 AS key, COUNT(*) AS dout FROM cauchy_edges GROUP BY col0
        ), incoming AS (
            SELECT col1 AS key, COUNT(*) AS din FROM cauchy_edges GROUP BY col1
        )
        SELECT COALESCE(o.key, i.key) AS key,
               COALESCE(o.dout, 0) AS dout, COALESCE(i.din, 0) AS din
        FROM outgoing o FULL OUTER JOIN incoming i USING (key)
    """)


def candidate_bounds(con, ps=DEFAULT_PS):
    if not ps or any(not math.isfinite(p) or not 0 < p < 1 for p in ps):
        raise ValueError("p values must lie strictly between zero and one")
    columns = ["SUM(dout::DOUBLE * dout)", "SUM(din::DOUBLE * dout)"]
    for p in ps:
        columns.extend([
            f"SUM(POW(dout::DOUBLE, {1 + 2 * p:.17g}))",
            f"SUM(din::DOUBLE * POW(dout::DOUBLE, {2 - 2 * p:.17g}))",
        ])
    row = con.execute("SELECT " + ", ".join(columns) + " FROM cauchy_profile").fetchone()
    row = [0.0 if value is None else float(value) for value in row]
    return row[0], row[1], [
        (p, row[2 + 2 * i], row[3 + 2 * i],
         math.sqrt(row[2 + 2 * i]) * math.sqrt(row[3 + 2 * i]))
        for i, p in enumerate(ps)
    ]


def exact_candidates(con):
    return con.execute("""
        SELECT COALESCE(SUM(LEAST(a.dout, b.dout)), 0)
        FROM cauchy_edges e
        JOIN cauchy_profile a ON e.col0 = a.key
        JOIN cauchy_profile b ON e.col1 = b.key
    """).fetchone()[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="topcats")
    parser.add_argument("--exact", action="store_true")
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    if args.log:
        args.log.parent.mkdir(parents=True, exist_ok=True)
    with (args.log.open("w") if args.log else nullcontext()) as log, \
            duckdb.connect(config={"memory_limit": "4GB", "threads": 4}) as con:
        report(f"query\t{ROOT / 'queries/1.sql'}", log)
        report(f"dataset\t{args.dataset}", log)
        started = perf_counter()
        load_graph(con, ROOT / DATASETS[args.dataset])
        report(f"load_seconds\t{perf_counter() - started:.6f}", log)
        started = perf_counter()
        prepare_profile(con)
        report(f"degree_preparation_seconds\t{perf_counter() - started:.6f}", log)
        started = perf_counter()
        w1, w2, bounds = candidate_bounds(con)
        report(f"moment_collection_seconds\t{perf_counter() - started:.6f}", log)
        report(f"fixed_R1_R3_candidates\t{w1:.12g}", log)
        report(f"fixed_R1_R2_candidates\t{w2:.12g}", log)
        exact = None
        if args.exact:
            started = perf_counter()
            exact = exact_candidates(con)
            report(f"exact_split_candidates\t{exact}", log)
            report(f"exact_validation_seconds\t{perf_counter() - started:.6f}", log)
        best_fixed = min(w1, w2)
        report("p\tA=sum(dout^(1+2p))\tB=sum(din*dout^(2-2p))\tU_p\tU_over_best_fixed", log)
        for p, a, b, bound in bounds:
            if exact is not None and bound + 1e-7 * max(1, exact) < exact:
                raise AssertionError("Cauchy bound is below exact split work")
            ratio = f"{bound / best_fixed:.9g}" if best_fixed else "-"
            report(f"{p:.9g}\t{a:.12g}\t{b:.12g}\t{bound:.12g}\t{ratio}", log)
        best = min(bounds, key=lambda item: item[3])
        report(f"best_tested_p\t{best[0]:.9g}", log)
        report(f"best_tested_cauchy_bound\t{best[3]:.12g}", log)
        report(f"combined_with_fixed_bound\t{min(best_fixed, best[3]):.12g}", log)


if __name__ == "__main__":
    main()
