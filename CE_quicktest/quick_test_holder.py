"""Compare Cauchy and conjugate-exponent Holder bounds for queries/1.sql."""

import argparse
import math
from contextlib import nullcontext
from pathlib import Path
from time import perf_counter

import duckdb
import numpy as np
from scipy.optimize import minimize
from scipy.special import logsumexp

from lp_statistics import load_graph
from quick_test_cauchy import DEFAULT_PS, ROOT, exact_candidates, prepare_profile
from quick_test_lp import report
from run_sql import DATASETS


class Moments:
    """Exact weighted degree distribution, compressed only by equal degrees."""

    def __init__(self, rows):
        # rows: positive outdegree, number of source keys, total indegree.
        values = np.asarray(rows, dtype=float)
        self.log_degree = np.log(values[:, 0])
        self.log_a_weights = np.log(values[:, 1]) + self.log_degree
        self.log_b_weights = np.full(len(values), -np.inf)
        positive = values[:, 2] > 0
        self.log_b_weights[positive] = np.log(values[positive, 2])

    def moment(self, exponent, weights):
        terms = weights + exponent * self.log_degree
        value = logsumexp(terms)
        derivative = np.dot(np.exp(terms - value), self.log_degree)
        return value, derivative

    def objective(self, params):
        # t=1/r, 1-t=1/s; r and s are conjugate Holder exponents.
        p, t = params
        a, b = p / t, (1 - p) / (1 - t)
        la, da = self.moment(a, self.log_a_weights)
        lb, db = self.moment(b, self.log_b_weights)
        value = t * la + (1 - t) * lb
        gradient = np.array([da - db, la - a * da - lb + b * db])
        return value, gradient


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
        prepare_profile(con)
        report(f"load_and_degree_seconds\t{perf_counter() - started:.6f}", log)
        started = perf_counter()
        rows = con.execute("""
            SELECT dout, COUNT(*), SUM(din)
            FROM cauchy_profile WHERE dout > 0 GROUP BY dout ORDER BY dout
        """).fetchall()
        report(f"exact_profile_collection_seconds\t{perf_counter() - started:.6f}", log)
        if not rows or not any(row[2] > 0 for row in rows):
            report("split_candidates\t0 (no positive-degree destination)", log)
            return
        moments = Moments(rows)
        w1 = math.exp(moments.moment(1, moments.log_a_weights)[0])
        w2 = math.exp(moments.moment(1, moments.log_b_weights)[0])
        report(f"fixed_R1_R3_candidates\t{w1:.12g}", log)
        report(f"fixed_R1_R2_candidates\t{w2:.12g}", log)
        exact = None
        if args.exact:
            started = perf_counter()
            exact = exact_candidates(con)
            report(f"exact_split_candidates\t{exact}", log)
            report(f"exact_validation_seconds\t{perf_counter() - started:.6f}", log)
        started = perf_counter()
        records = []
        report("p\tt=1/r\tr\ts\tupper_bound", log)
        for p in DEFAULT_PS:
            for t in [i / 20 for i in range(1, 20)]:
                value, _ = moments.objective((p, t))
                if exact and value + 1e-7 < math.log(exact):
                    raise AssertionError("Holder bound is below exact split work")
                records.append((value, p, t))
                report(f"{p:.9g}\t{t:.9g}\t{1/t:.9g}\t{1/(1-t):.9g}\t{math.exp(value):.12g}", log)
        best_grid = min(records)
        best_cauchy = min(row for row in records if row[2] == 0.5)
        fit = minimize(moments.objective, best_grid[1:], jac=True, method="L-BFGS-B",
                       bounds=[(1e-6, 1-1e-6), (1e-6, 1-1e-6)],
                       options={"ftol": 1e-14, "gtol": 1e-9, "maxiter": 1000})
        report(f"search_seconds\t{perf_counter() - started:.6f}", log)
        report(f"best_cauchy_grid\tp={best_cauchy[1]:.9g}\tbound={math.exp(best_cauchy[0]):.12g}", log)
        report(f"best_holder_grid\tp={best_grid[1]:.9g}\tt={best_grid[2]:.9g}\tbound={math.exp(best_grid[0]):.12g}", log)
        report(f"optimizer_success\t{fit.success}\t{fit.message}", log)
        value, _ = moments.objective(fit.x)
        if exact and value + 1e-7 < math.log(exact):
            raise AssertionError("Refined bound is below exact split work")
        p, t = fit.x
        report(f"refined_holder\tp={p:.12g}\tt={t:.12g}\tr={1/t:.12g}\ts={1/(1-t):.12g}\tbound={math.exp(value):.12g}", log)
        report(f"combined_with_fixed_bound\t{min(w1,w2,math.exp(best_grid[0]),math.exp(value)):.12g}", log)
        report("note\tRefinement searches a bounded interior; no certified global optimality claim.", log)


if __name__ == "__main__":
    main()
