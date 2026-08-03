"""Run four LP bounds for a degree-split triangle query.

The triangle is written as R1(x,y), R2(x,z), R3(y,z).  R1 is split using
deg_R2(*|x) and deg_R3(*|y), with one shared threshold for both sides.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path

import duckdb

from lp_statistics import compute_lp_statistics, load_graph, solve_lp_bound
from run_sql import DATASETS


TRIANGLE_QUERY = "q(x,y,z) :- R1(x,y), R2(x,z), R3(y,z)"


@dataclass(frozen=True)
class SplitResult:
    name: str
    row_count: int
    objective: float | None
    cardinality_bound: float


def _create_relation_set(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(
        """
        CREATE TEMP TABLE relation_set AS
        SELECT DISTINCT col0, col1
        FROM R
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM relation_set").fetchone()[0])


def _create_splits(
    con: duckdb.DuckDBPyConnection, threshold: float
) -> dict[str, str]:
    splits = {
        "LL": (False, False),
        "LH": (False, True),
        "HL": (True, False),
        "HH": (True, True),
    }
    table_names: dict[str, str] = {}
    for name, (a_high, b_high) in splits.items():
        table = f"r1_split_{name.lower()}"
        a_operator = ">" if a_high else "<="
        b_operator = ">" if b_high else "<="
        con.execute(
            f"""
            CREATE TEMP TABLE {table} AS
            WITH degrees AS (
                SELECT col0 AS key, COUNT(*)::DOUBLE AS degree
                FROM relation_set
                GROUP BY col0
            )
            SELECT r1.col0, r1.col1
            FROM relation_set AS r1
            LEFT JOIN degrees AS degree_a
              ON r1.col0 = degree_a.key
            LEFT JOIN degrees AS degree_b
              ON r1.col1 = degree_b.key
            WHERE COALESCE(degree_a.degree, 0) {a_operator} ?
              AND COALESCE(degree_b.degree, 0) {b_operator} ?
            """,
            [threshold, threshold],
        )
        table_names[name] = table
    return table_names


def run_split_lp(
    con: duckdb.DuckDBPyConnection,
    lp_file: str | Path,
    threshold: float,
    max_p: int = 5,
) -> list[SplitResult]:
    """Split R1 into four branches and solve one LP for each branch."""
    _create_relation_set(con)
    split_tables = _create_splits(con, threshold)
    results: list[SplitResult] = []

    for name, split_table in split_tables.items():
        row_count = int(
            con.execute(f"SELECT COUNT(*) FROM {split_table}").fetchone()[0]
        )
        if row_count == 0:
            results.append(SplitResult(name, 0, None, 0.0))
            continue

        statistics = compute_lp_statistics(
            con,
            TRIANGLE_QUERY,
            table_names={
                "R1": split_table,
                "R2": "relation_set",
                "R3": "relation_set",
            },
            max_p=max_p,
        )
        lp_result = solve_lp_bound(
            query_file="queries/1.sql",
            statistics=statistics,
            lp_file=lp_file,
            h_empty=0,
        )
        bound = (
            math.exp(lp_result.objective_value)
            if lp_result.objective_value is not None
            else 0.0
        )
        results.append(
            SplitResult(name, row_count, lp_result.objective_value, bound)
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        "-d",
        default="topcats",
        choices=sorted(DATASETS),
    )
    parser.add_argument(
        "--lp",
        type=Path,
        default=Path("queries/lp_boundaries/1.sql.txt"),
    )
    parser.add_argument(
        "--threshold",
        type=float,
        help="shared degree threshold (default: sqrt of distinct |R|)",
    )
    parser.add_argument("--max-p", type=int, default=5)
    args = parser.parse_args()

    con = duckdb.connect(config={"temp_directory": "", "max_memory": "32GB"})
    con.execute("SET THREADS=32")
    load_graph(con, DATASETS[args.dataset])
    relation_size = int(
        con.execute("SELECT COUNT(*) FROM (SELECT DISTINCT col0, col1 FROM R)")
        .fetchone()[0]
    )
    threshold = args.threshold if args.threshold is not None else math.sqrt(relation_size)

    results = run_split_lp(con, args.lp, threshold, max_p=args.max_p)
    print(f"threshold: {threshold:.12g}")
    for result in results:
        objective = "empty" if result.objective is None else f"{result.objective:.12g}"
        print(
            f"{result.name}: rows={result.row_count}, "
            f"objective={objective}, exp(objective)={result.cardinality_bound:.12g}"
        )
    print(f"total bound: {sum(result.cardinality_bound for result in results):.12g}")


if __name__ == "__main__":
    main()
