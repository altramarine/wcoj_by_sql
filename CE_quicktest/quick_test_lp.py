"""Compare integer and reciprocal Lp bounds for queries/1.sql.

Statistics are collected once per physical column and reused for all three
triangle output bounds.
"""

import argparse
import math
from contextlib import nullcontext
from pathlib import Path
from time import perf_counter

import duckdb

from lp_statistics import compute_lp_statistics, format_lp_result, load_graph, solve_lp_bound
from run_sql import DATASETS


ROOT = Path(__file__).resolve().parent.parent
QUERY = ROOT / "queries/1.sql"
TEMPLATE = ROOT / "queries/lp_boundaries/1.sql.txt"


def report(text, log=None):
    print(text, flush=True)
    if log is not None:
        print(text, file=log, flush=True)


def quick_test(con, log=None):
    started = perf_counter()
    statistics = compute_lp_statistics(
        con, QUERY.read_text(), max_p=5, include_reciprocals=True
    )
    report(f"statistics_seconds\t{perf_counter() - started:.6f}", log)
    report("target\tnorms\tupper_bound\tlp_seconds", log)
    results = {}
    target = "triangle_output"
    log_bounds = []
    for label, selected in [
        ("0,1,2,3,4,5,inf", [s for s in statistics if s.p == 0 or s.p >= 1]),
        ("0,1,2,3,4,5,inf,1/2,1/3,1/4,1/5", statistics),
        ("1/2,1/3,1/4,1/5", [s for s in statistics if 0 < s.p < 1]),
    ]:
        started = perf_counter()
        result = solve_lp_bound(QUERY, selected, TEMPLATE)
        elapsed = perf_counter() - started
        if log is not None:
            print(f"\n[target={target}; norms={label}]", file=log)
            print(format_lp_result(result), file=log, flush=True)
        if result.status != "optimal" or result.objective_value is None:
            raise RuntimeError(f"{target}: {result.message}")
        log_bound = result.objective_value
        bound = math.exp(log_bound) if log_bound < 709 else math.inf
        log_bounds.append(log_bound)
        report(f"{target}\t{label}\t{bound:.12g}\t{elapsed:.6f}", log)
        results[target, label] = bound
    # The combined constraint set contains each of the two standalone sets.
    if log_bounds[1] > min(log_bounds[0], log_bounds[2]) + 1e-7:
        raise AssertionError("Adding constraints increased the bound")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="epinions")
    parser.add_argument("--log", type=Path,
                        help="write summary and each LP's variables/boundary inequalities")
    args = parser.parse_args()
    if args.log:
        args.log.parent.mkdir(parents=True, exist_ok=True)
    with (args.log.open("w") if args.log else nullcontext()) as log, \
            duckdb.connect(config={"memory_limit": "4GB", "threads": 4}) as con:
        report(f"query\t{QUERY}", log)
        report(f"dataset\t{args.dataset}", log)
        started = perf_counter()
        load_graph(con, ROOT / DATASETS[args.dataset])
        report(f"load_seconds\t{perf_counter() - started:.6f}", log)
        quick_test(con, log=log)


if __name__ == "__main__":
    main()
