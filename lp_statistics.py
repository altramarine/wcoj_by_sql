"""Generate finite Lp and L-infinity statistics for every atom column in a CQ.

The public API accepts input such as
``q(x, y, z) :- R(x, y), R(y, z), R(z, x)`` and uses DuckDB to compute the
statistics of every column of every relation occurrence.
"""

from __future__ import annotations

import argparse
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import duckdb

from Lpsolver import LinearProgramResult, format_result, solve
from run_sql import DATASETS


@dataclass(frozen=True)
class LPStatistic:
    relation: str
    atom_index: int
    column_index: int
    atom_variables: tuple[str, ...]
    conditioning_variables: tuple[str, ...]
    extension_variables: tuple[str, ...]
    p: int | float
    norm: float
    log_norm: float
    inequality: str


@dataclass(frozen=True)
class QueryAtom:
    relation: str
    variables: tuple[str, ...]


def _quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_]\w*", identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def _entropy_name(variables: Sequence[str]) -> str:
    names = sorted(set(variables))
    if not names:
        return "{h_empty}"
    return "h_" + "".join(names)


def _number(value: float) -> str:
    return format(value, ".17g")


def _inequality(
    conditioning: Sequence[str],
    extension: Sequence[str],
    p: int | float,
    log_norm: float,
) -> str:
    h_u = _entropy_name(conditioning)
    if p == 0:
        return f"{h_u} <= {_number(log_norm)}"

    h_uv = _entropy_name([*conditioning, *extension])
    # (1/p)H(U) + H(V|U) = H(U,V) - (1 - 1/p)H(U).
    coefficient = 1.0 if math.isinf(p) else 1.0 - 1.0 / p
    if coefficient == 0:
        return f"{h_uv} <= {_number(log_norm)}"
    if coefficient == 1:
        return f"{h_uv} - {h_u} <= {_number(log_norm)}"
    return f"{h_uv} - {_number(coefficient)} {h_u} <= {_number(log_norm)}"


def parse_query(query: str) -> list[QueryAtom]:
    """Parse the body atoms of a conjunctive query."""
    if ":-" not in query:
        raise ValueError("query must contain ':-'")
    body = query.split(":-", 1)[1]
    atoms: list[QueryAtom] = []
    position = 0
    atom_pattern = re.compile(r"([A-Za-z_]\w*)\s*\(([^()]*)\)")
    while position < len(body):
        whitespace = re.match(r"\s*", body[position:])
        position += whitespace.end()
        if position == len(body):
            break
        match = atom_pattern.match(body, position)
        if not match:
            raise ValueError(f"invalid query body near {body[position:]!r}")
        variables = tuple(
            variable.strip() for variable in match.group(2).split(",")
        )
        if not variables or any(
            not re.fullmatch(r"[A-Za-z_]\w*", variable) for variable in variables
        ):
            raise ValueError(f"invalid variables in atom {match.group(0)!r}")
        atoms.append(QueryAtom(match.group(1), variables))
        position = match.end()
        whitespace = re.match(r"\s*", body[position:])
        position += whitespace.end()
        if position < len(body):
            if body[position] != ",":
                raise ValueError(f"expected ',' near {body[position:]!r}")
            position += 1
    if not atoms:
        raise ValueError("query body has no atoms")
    return atoms


def _degree_norms(
    con: duckdb.DuckDBPyConnection,
    table: str,
    arity: int,
    conditioning_column: int,
    p_values: Sequence[int | float],
) -> dict[int | float, tuple[float, float]]:
    """Return p -> (norm, log_norm) for one physical relation column."""
    table_sql = _quote_identifier(table)
    physical_columns = [f"col{index}" for index in range(arity)]
    distinct_columns = ", ".join(
        _quote_identifier(column) for column in physical_columns
    )
    conditioning_sql = _quote_identifier(physical_columns[conditioning_column])
    aggregate_columns = [
        "COUNT(*)::DOUBLE AS l0",
        "MAX(max_degree)::DOUBLE AS max_degree",
        *[
            f"SUM(POW(degree / max_degree, {p}))::DOUBLE AS scaled_{p}"
            for p in sorted(set(p_values))
            if p > 0 and not math.isinf(p)
        ],
    ]
    query = f"""
        WITH _lp_relation_set AS (
            SELECT DISTINCT {distinct_columns}
            FROM {table_sql}
        ),
        degrees AS (
            SELECT {conditioning_sql}, COUNT(*)::DOUBLE AS degree
            FROM _lp_relation_set
            GROUP BY {conditioning_sql}
        ),
        scaled AS (
            SELECT degree, MAX(degree) OVER () AS max_degree
            FROM degrees
        )
        SELECT {", ".join(aggregate_columns)}
        FROM scaled
    """
    row = con.execute(query).fetchone()
    if row is None or row[0] == 0:
        raise ValueError(f"relation {table!r} is empty")

    l0 = float(row[0])
    max_degree = float(row[1])
    positive_ps = [
        p for p in sorted(set(p_values)) if p > 0 and not math.isinf(p)
    ]
    scaled_sums = dict(zip(positive_ps, (float(value) for value in row[2:])))
    result = {0: (l0, math.log(l0))}
    for p in positive_ps:
        log_norm = math.log(max_degree) + math.log(scaled_sums[p]) / p
        result[p] = (math.exp(log_norm), log_norm)
    if math.inf in p_values:
        result[math.inf] = (max_degree, math.log(max_degree))
    return result


def compute_lp_statistics(
    con: duckdb.DuckDBPyConnection,
    query: str,
    table_names: Mapping[str, str] | None = None,
    max_p: int = 5,
    include_infinity: bool = True,
) -> list[LPStatistic]:
    """Compute Lp statistics for every column of every atom occurrence.

    Physical relation columns must be named ``col0``, ``col1``, etc.
    ``table_names`` optionally maps query relation names to DuckDB table names.
    Duplicate tuples are removed so the input is treated as a relation.
    """
    if not isinstance(max_p, int) or max_p < 0:
        raise ValueError("max_p must be a nonnegative integer")
    p_values: tuple[int | float, ...] = (
        *range(max_p + 1),
        *((math.inf,) if include_infinity else ()),
    )

    atoms = parse_query(query)
    table_names = table_names or {}
    statistics: list[LPStatistic] = []
    cache: dict[
        tuple[str, int, int, tuple[int | float, ...]],
        dict[int | float, tuple[float, float]],
    ] = {}

    for atom_index, atom in enumerate(atoms):
        physical_table = table_names.get(atom.relation, atom.relation)
        if len(atom.variables) < 2:
            raise ValueError(f"atom {atom.relation} must have at least two columns")
        for column_index, conditioning_variable in enumerate(atom.variables):
            cache_key = (
                physical_table,
                len(atom.variables),
                column_index,
                tuple(sorted(set(p_values))),
            )
            if cache_key not in cache:
                cache[cache_key] = _degree_norms(
                    con,
                    physical_table,
                    len(atom.variables),
                    column_index,
                    p_values,
                )
            extension_variables = tuple(
                variable
                for index, variable in enumerate(atom.variables)
                if index != column_index
            )
            for p in p_values:
                norm, log_norm = cache[cache_key][p]
                statistics.append(
                    LPStatistic(
                        relation=atom.relation,
                        atom_index=atom_index,
                        column_index=column_index,
                        atom_variables=atom.variables,
                        conditioning_variables=(conditioning_variable,),
                        extension_variables=extension_variables,
                        p=p,
                        norm=norm,
                        log_norm=log_norm,
                        inequality=_inequality(
                            (conditioning_variable,),
                            extension_variables,
                            p,
                            log_norm,
                        ),
                    )
                )

    return statistics


def load_graph(
    con: duckdb.DuckDBPyConnection, dataset_path: str | Path, table: str = "R"
) -> None:
    """Load a two-column CSV using the same layout as ``run_sql.py``."""
    table_sql = _quote_identifier(table)
    con.execute(
        f"""
        CREATE TABLE {table_sql} AS
        SELECT col0, col1
        FROM read_csv(
            ?,
            names = ['col0', 'col1'],
            header = false
        )
        """,
        [str(dataset_path)],
    )


def inequalities_text(statistics: Sequence[LPStatistic]) -> str:
    return "\n".join(statistic.inequality for statistic in statistics)


def solve_lp_bound(
    query_file: str | Path,
    statistics: Sequence[LPStatistic],
    lp_file: str | Path,
    h_empty: float = 0.0,
) -> LinearProgramResult:
    """Solve a query using the explicitly provided LP template."""
    query_path = Path(query_file)
    boundary_path = Path(lp_file)
    if not boundary_path.is_file():
        raise FileNotFoundError(
            f"LP boundary file not found for {query_path}: {boundary_path}"
        )
    return solve(
        {
            "input": boundary_path.read_text(),
            "parameters": {
                "lp_statistics": inequalities_text(statistics),
                "h_empty": h_empty,
            },
        }
    )


def format_lp_result(result: LinearProgramResult) -> str:
    output = format_result(result)
    if result.objective_value is not None:
        try:
            cardinality_bound = math.exp(result.objective_value)
        except OverflowError:
            cardinality_bound = math.inf
        output += f"\nexp(objective): {_number(cardinality_bound)}"
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        "-d",
        default="skitters",
        choices=sorted(DATASETS),
    )
    parser.add_argument("--output", "-o", type=Path)
    parser.add_argument(
        "--max-p",
        type=int,
        default=5,
        help="generate L0 through this Lp, plus L-infinity (default: 5)",
    )
    parser.add_argument(
        "--query",
        "-q",
        type=Path,
        default=Path("queries/1.sql"),
        help="conjunctive-query file (default: queries/1.sql)",
    )
    parser.add_argument(
        "--lp",
        type=Path,
        required=True,
        help="LP template file",
    )
    args = parser.parse_args()

    con = duckdb.connect(config={"temp_directory": "", "max_memory": "32GB"})
    con.execute("SET THREADS=32")
    load_graph(con, DATASETS[args.dataset])
    query = args.query.read_text().strip()
    statistics = compute_lp_statistics(
        con,
        query=query,
        max_p=args.max_p,
    )
    result = solve_lp_bound(
        args.query,
        statistics,
        lp_file=args.lp,
        h_empty=0,
    )
    output = format_lp_result(result)
    if args.output:
        args.output.write_text(output + "\n")
    else:
        print(output)


if __name__ == "__main__":
    main()
