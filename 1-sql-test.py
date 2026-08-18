"""Print the complete, unsplit entropy LP for queries/1.sql."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from Lpsolver import parse_linear_program, solve_linear_program, substitute_parameters
from lp_statistics import (
    compute_lp_statistics,
    format_lp_result,
    inequalities_text,
    load_graph,
)
from run_sql import DATASETS


def build_complete_lp(
    con: duckdb.DuckDBPyConnection,
    query_file: str | Path,
    lp_file: str | Path,
    max_p: int = 5,
) -> str:
    """Return the rendered LP without splitting any relation."""
    query = Path(query_file).read_text().strip()
    statistics = compute_lp_statistics(con, query, max_p=max_p)
    complete_lp = substitute_parameters(
        Path(lp_file).read_text(),
        {
            "lp_statistics": inequalities_text(statistics),
            "h_empty": 0,
        },
    )
    # Validate exactly what will be printed and later passed to Lpsolver.
    parse_linear_program(complete_lp)
    return complete_lp


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        "-d",
        default="topcats",
        choices=sorted(DATASETS),
    )
    parser.add_argument(
        "--query",
        "-q",
        type=Path,
        default=Path("queries/1.sql"),
    )
    parser.add_argument(
        "--lp",
        type=Path,
        default=Path("queries/lp_boundaries/1.sql.txt"),
    )
    parser.add_argument("--max-p", type=int, default=5)
    args = parser.parse_args()

    con = duckdb.connect(config={"temp_directory": "", "max_memory": "32GB"})
    con.execute("SET THREADS=32")
    load_graph(con, DATASETS[args.dataset])
    try:
        complete_lp = build_complete_lp(con, args.query, args.lp, max_p=args.max_p)
        result = solve_linear_program(parse_linear_program(complete_lp))
        print(complete_lp)
        print()
        print(format_lp_result(result))
    finally:
        con.close()


if __name__ == "__main__":
    main()
