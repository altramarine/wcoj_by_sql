"""Solve a linear program through the ``solve`` API.

The recommended input is a dictionary containing the LP text and an optional
parameter dictionary:

    result = solve({
        "input": '''
            max: h_ab
            h_a <= h_ab
            2 h_a + h_b <= h_ab
            h_ab <= {upper_bound}
        ''',
        "parameters": {
            "upper_bound": 10,
        },
    })

Each ``{name}`` in ``input`` is replaced with the matching value from
``parameters`` before parsing.  The API also accepts the LP text directly:

    result = solve("max: h_ab\\nh_ab <= 10")

Variables must use the ``h_*`` format.  Objectives may use ``max``,
``maximize``, ``min``, or ``minimize``.  Blank lines and comments beginning
with ``#`` are ignored.  ``solve`` returns a ``LinearProgramResult`` containing
the status, objective value, variable values, and boundary inequalities.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, NotRequired, TypedDict


_VARIABLE = r"h_(?:[A-Za-z0-9]+|\{[A-Za-z0-9, ]+\})"
_NUMBER = r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
_TERM_RE = re.compile(
    rf"(?P<sign>[+-]?)\s*(?:(?P<number>{_NUMBER})\s*\*?\s*)?"
    rf"(?P<variable>{_VARIABLE})"
)


@dataclass(frozen=True)
class LinearExpression:
    coefficients: dict[str, float]
    constant: float = 0.0


@dataclass(frozen=True)
class Inequality:
    left: LinearExpression
    operator: str
    right: LinearExpression
    original_input: str

    def print(self) -> str:
        return self.original_input


@dataclass(frozen=True)
class LinearProgram:
    objective: LinearExpression
    maximize: bool
    inequalities: list[Inequality]
    variables: list[str]
    original_input: str


@dataclass(frozen=True)
class LinearProgramResult:
    status: str
    objective_value: float | None
    variable_values: dict[str, float]
    boundary_inequalities: list[Inequality]
    message: str


class LinearProgramInput(TypedDict):
    input: str
    parameters: NotRequired[Mapping[str, object]]


def substitute_parameters(
    text: str, parameters: Mapping[str, object] | None = None
) -> str:
    """Replace ``{name}`` placeholders with values from *parameters*.

    Only placeholders whose names occur in the dictionary are replaced.  This
    keeps unrelated braces, such as those in entropy variable names, intact.
    """
    if not parameters:
        return text
    rendered = text
    for name, value in parameters.items():
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_]\w*", name):
            raise ValueError(f"invalid parameter name: {name!r}")
        rendered = rendered.replace("{" + name + "}", str(value))
    return rendered


def _parse_expression(text: str) -> LinearExpression:
    """Parse a sum of numeric constants and h_* variables."""
    coefficients: dict[str, float] = {}
    constant = 0.0
    position = 0

    while position < len(text):
        whitespace = re.match(r"\s+", text[position:])
        if whitespace:
            position += whitespace.end()
            continue

        term = _TERM_RE.match(text, position)
        if term:
            sign = -1.0 if term.group("sign") == "-" else 1.0
            coefficient = float(term.group("number") or 1.0) * sign
            variable = term.group("variable")
            coefficients[variable] = coefficients.get(variable, 0.0) + coefficient
            position = term.end()
            continue

        number = re.match(rf"(?P<sign>[+-]?)\s*(?P<number>{_NUMBER})", text[position:])
        if number:
            sign = -1.0 if number.group("sign") == "-" else 1.0
            constant += sign * float(number.group("number"))
            position += number.end()
            continue

        raise ValueError(f"invalid expression near {text[position:]!r}")

    return LinearExpression(
        coefficients={name: value for name, value in coefficients.items() if value},
        constant=constant,
    )


def parse_linear_program(text: str) -> LinearProgram:
    """Parse a complete linear program and retain its original input."""
    objective: LinearExpression | None = None
    maximize = True
    inequalities: list[Inequality] = []

    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        objective_match = re.fullmatch(
            r"(max(?:imize)?|min(?:imize)?)\s*:?\s*(.+)", line, re.IGNORECASE
        )
        if objective_match:
            if objective is not None:
                raise ValueError(f"line {line_number}: more than one objective")
            maximize = objective_match.group(1).lower().startswith("max")
            objective = _parse_expression(objective_match.group(2))
            continue

        parts = re.split(r"\s*(<=|>=|=)\s*", line)
        if len(parts) != 3:
            raise ValueError(
                f"line {line_number}: expected an inequality or an objective"
            )
        inequalities.append(
            Inequality(
                left=_parse_expression(parts[0]),
                operator=parts[1],
                right=_parse_expression(parts[2]),
                original_input=raw_line.strip(),
            )
        )

    if objective is None:
        raise ValueError("missing objective (for example, 'max: h_ab')")
    if not inequalities:
        raise ValueError("the linear program has no inequalities")

    names = set(objective.coefficients)
    for inequality in inequalities:
        names.update(inequality.left.coefficients)
        names.update(inequality.right.coefficients)
    return LinearProgram(
        objective=objective,
        maximize=maximize,
        inequalities=inequalities,
        variables=sorted(names),
        original_input=text,
    )


def _coefficient_vector(
    expression: LinearExpression, variables: list[str]
) -> list[float]:
    return [expression.coefficients.get(name, 0.0) for name in variables]


def solve_linear_program(
    problem: LinearProgram, tolerance: float = 1e-10
) -> LinearProgramResult:
    """Solve *problem* and identify inequalities active at the optimum."""
    try:
        from scipy.optimize import linprog
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError("install project dependencies with 'uv sync'") from exc

    upper_a: list[list[float]] = []
    upper_b: list[float] = []
    equal_a: list[list[float]] = []
    equal_b: list[float] = []

    for inequality in problem.inequalities:
        left = _coefficient_vector(inequality.left, problem.variables)
        right = _coefficient_vector(inequality.right, problem.variables)
        row = [a - b for a, b in zip(left, right)]
        bound = inequality.right.constant - inequality.left.constant
        if inequality.operator == ">=":
            row, bound = ([-value for value in row], -bound)
        if inequality.operator == "=":
            equal_a.append(row)
            equal_b.append(bound)
        else:
            upper_a.append(row)
            upper_b.append(bound)

    objective = _coefficient_vector(problem.objective, problem.variables)
    if problem.maximize:
        objective = [-value for value in objective]

    # Entropy quantities are nonnegative. Explicit negative bounds can still be
    # represented by using an equality or inequality in the input.
    result = linprog(
        objective,
        A_ub=upper_a or None,
        b_ub=upper_b or None,
        A_eq=equal_a or None,
        b_eq=equal_b or None,
        bounds=[(0, None)] * len(problem.variables),
        method="highs",
    )
    if not result.success:
        return LinearProgramResult(
            status={2: "infeasible", 3: "unbounded"}.get(result.status, "failed"),
            objective_value=None,
            variable_values={},
            boundary_inequalities=[],
            message=result.message,
        )

    values = dict(zip(problem.variables, (float(value) for value in result.x)))
    boundary = []
    for inequality in problem.inequalities:
        left = inequality.left.constant + sum(
            coefficient * values[name]
            for name, coefficient in inequality.left.coefficients.items()
        )
        right = inequality.right.constant + sum(
            coefficient * values[name]
            for name, coefficient in inequality.right.coefficients.items()
        )
        if abs(left - right) <= tolerance * max(1.0, abs(left), abs(right)):
            boundary.append(inequality)

    value = problem.objective.constant + sum(
        coefficient * values[name]
        for name, coefficient in problem.objective.coefficients.items()
    )
    return LinearProgramResult(
        status="optimal",
        objective_value=value,
        variable_values=values,
        boundary_inequalities=boundary,
        message=result.message,
    )


def solve(
    input_data: str | LinearProgramInput,
    parameters: Mapping[str, object] | None = None,
    tolerance: float = 1e-10,
) -> LinearProgramResult:
    """Parse and solve a linear program supplied as a string or input mapping.

    This is the simplest public API.  Use :func:`parse_linear_program` and
    :func:`solve_linear_program` separately when the parsed model is also
    needed.  A mapping has the form
    ``{"input": "...", "parameters": {"name": value}}``.
    """
    if isinstance(input_data, str):
        text = input_data
    else:
        if parameters is not None:
            raise ValueError(
                "parameters must be inside input_data when input_data is a dictionary"
            )
        text = input_data["input"]
        parameters = input_data.get("parameters", {})

    rendered_text = substitute_parameters(text, parameters)
    return solve_linear_program(
        parse_linear_program(rendered_text), tolerance=tolerance
    )


def format_result(result: LinearProgramResult) -> str:
    lines = [f"status: {result.status}"]
    if result.status != "optimal":
        lines.append(f"message: {result.message}")
        return "\n".join(lines)
    lines.append(f"objective: {result.objective_value:.12g}")
    lines.append("variables:")
    lines.extend(f"  {name} = {value:.12g}" for name, value in result.variable_values.items())
    lines.append("boundary inequalities:")
    lines.extend(
        f"  {inequality.print()}" for inequality in result.boundary_inequalities
    )
    if not result.boundary_inequalities:
        lines.append("  (none)")
    return "\n".join(lines)
