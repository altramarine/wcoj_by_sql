#!/usr/bin/env python3
"""Translate hand-selected .dl rule bodies to ordered DuckDB WITH closures.

Supported language: nonrecursive positive integer Datalog, comparisons, anonymous
variables, multiple rules, projection, and the two Arc degree-count aggregates.
Set semantics are explicit at every relation boundary. Unsupported syntax is
rejected; plan-tree comments are never used to generate execution operators.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any


def identifier(name: str) -> str:
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', name):
        raise ValueError(f'Invalid identifier: {name!r}')
    return '"' + name + '"'


def split_top_level(text: str, separator=',') -> list[str]:
    stack = []
    start = 0
    parts = []
    for index, character in enumerate(text):
        if character in '({':
            stack.append(character)
        elif character in ')}':
            if not stack or (stack.pop(), character) not in (('(', ')'), ('{', '}')):
                raise ValueError('Unbalanced delimiters')
        elif character == separator and not stack:
            parts.append(text[start:index].strip())
            start = index + 1
    if stack:
        raise ValueError('Unbalanced delimiters')
    parts.append(text[start:].strip())
    return parts


def term(text: str) -> str | int:
    if re.fullmatch(r'-?\d+', text):
        return int(text)
    identifier(text)
    return text


@dataclass(frozen=True)
class Atom:
    name: str
    arguments: tuple


@dataclass(frozen=True)
class Count:
    source: Atom


@dataclass(frozen=True)
class Comparison:
    left: str | int
    operator: str
    right: str | int


@dataclass(frozen=True)
class Rule:
    head: Atom
    body: tuple[Atom | Comparison, ...]


@dataclass(frozen=True)
class Program:
    columns: dict[str, tuple[str, ...]]
    rules: tuple[Rule, ...]
    inputs: tuple[str, ...]
    output: str
    source: str

    def definitions(self):
        result = defaultdict(list)
        for rule in self.rules:
            result[rule.head.name].append(rule)
        return dict(result)


@dataclass(frozen=True)
class SQLPlan:
    statements: tuple[str, ...]
    manifest: dict[str, Any]
    tables: dict[str, str]

    @property
    def sql(self):
        return ('-- Faithful nonrecursive Datalog rewrite with set semantics.\n'
                f'-- Input: {self.manifest["input_table"]}({", ".join(self.manifest["input_columns"])}).\n'
                '-- Degree expressions are from the rule bodies, not plan comments.\n'
                + '\n\n'.join(self.statements) + '\n')


def atom(text: str, *, head=False) -> Atom:
    match = re.fullmatch(r'([A-Za-z_]\w*)\s*\((.*)\)', text.strip(), re.S)
    if not match:
        raise ValueError(f'Unsupported atom: {text!r}')
    arguments = []
    for part in split_top_level(match[2]):
        count = re.fullmatch(r'count\s*:\s*\{(.*)\}', part, re.S)
        if count:
            if not head:
                raise ValueError('Aggregate supported only in a rule head')
            arguments.append(Count(atom(count[1])))
        else:
            arguments.append(term(part))
    return Atom(match[1], tuple(arguments))


def parse_program(source: str) -> Program:
    text = re.sub(r'/\*.*?\*/', '', source, flags=re.S)
    text = re.sub(r'//[^\n]*', '', text)
    columns = {}
    folded = set()
    declaration = re.compile(r'\.decl\s+(\w+)\s*\(([^()]*)\)', re.S)
    for match in declaration.finditer(text):
        identifier(match[1])
        if match[1].lower() in folded:
            raise ValueError(f'Duplicate/case-colliding declaration: {match[1]}')
        folded.add(match[1].lower())
        names = []
        for field in split_top_level(match[2]):
            field_match = re.fullmatch(r'(\w+)\s*:\s*(int32|int64|number)', field)
            if not field_match:
                raise ValueError(f'Unsupported declaration field: {field!r}')
            identifier(field_match[1])
            names.append(field_match[1])
        if len(set(name.lower() for name in names)) != len(names):
            raise ValueError('Duplicate declaration column')
        columns[match[1]] = tuple(names)
    text = declaration.sub('', text)
    input_pattern = re.compile(r'\.input\s+(\w+)(?:\s*\([^()]*\))?')
    inputs = tuple(match[1] for match in input_pattern.finditer(text))
    text = input_pattern.sub('', text)
    output_pattern = re.compile(r'\.printsize\s+(\w+)')
    outputs = [match[1] for match in output_pattern.finditer(text)]
    text = output_pattern.sub('', text)
    if len(outputs) != 1 or inputs != ('Arc',):
        raise ValueError('Expected one Arc input and one .printsize output')
    rules = []
    pieces = split_top_level(text, '.')
    if pieces[-1].strip():
        raise ValueError('Rules must end with a period; unsupported directive or syntax')
    for piece in pieces[:-1]:
        if not piece.strip():
            continue
        if piece.count(':-') != 1:
            raise ValueError(f'Unsupported rule: {piece!r}')
        head_text, body_text = piece.split(':-')
        body = []
        for literal in split_top_level(body_text):
            comparison = re.fullmatch(r'(\w+|-?\d+)\s*(<=|>=|!=|=|<|>)\s*(\w+|-?\d+)', literal)
            body.append(Comparison(term(comparison[1]), comparison[2], term(comparison[3]))
                        if comparison else atom(literal))
        rules.append(Rule(atom(head_text, head=True), tuple(body)))
    program = Program(columns, tuple(rules), inputs, outputs[0], source)
    definitions = program.definitions()
    for rule in rules:
        for relation in (rule.head, *(lit for lit in rule.body if isinstance(lit, Atom))):
            if relation.name not in columns or len(relation.arguments) != len(columns[relation.name]):
                raise ValueError(f'Undeclared relation or wrong arity: {relation.name}')
        if rule.head.name in inputs:
            raise ValueError('Rules cannot redefine an input')
    if program.output not in definitions:
        raise ValueError('Output has no defining rules')
    for name in definitions:
        dependency_order(program, name)  # Also reject cycles in unused definitions.
    return program


def dependency_order(program: Program, output=None) -> tuple[str, ...]:
    definitions = program.definitions()
    state = {}
    order = []

    def visit(name):
        if state.get(name) == 1:
            raise ValueError(f'Recursive program is unsupported: {name}')
        if state.get(name) == 2:
            return
        if name not in program.inputs and name not in definitions:
            raise ValueError(f'Relation has no producer: {name}')
        state[name] = 1
        for rule in definitions.get(name, []):
            for literal in rule.body:
                if isinstance(literal, Atom):
                    visit(literal.name)
            for argument in rule.head.arguments:
                if isinstance(argument, Count):
                    visit(argument.source.name)
        state[name] = 2
        order.append(name)

    visit(output or program.output)
    return tuple(order)


def degree_spec(rule: Rule) -> tuple[Atom, int]:
    """Validate precisely the degree-count idiom present in the upstream files."""
    args = rule.head.arguments
    if len(args) != 2 or not isinstance(args[0], str) or not isinstance(args[1], Count):
        raise ValueError('Only key + degree count aggregate heads are supported')
    inner = args[1].source
    if len(inner.arguments) != 2 or len(set(inner.arguments)) != 2:
        raise ValueError('Expected a binary degree source with two different variables')
    if any(not isinstance(value, str) or value == '_' for value in inner.arguments):
        raise ValueError('Expected variable-only degree source')
    if args[0] not in inner.arguments:
        raise ValueError('Degree group key is not in the count source')
    position = inner.arguments.index(args[0])
    seed = tuple(args[0] if index == position else '_' for index in range(2))
    if rule.body != (Atom(inner.name, seed),):
        raise ValueError('Aggregate seed must enumerate exactly the nonempty degree keys')
    return inner, position


def _expression(value, bindings):
    if isinstance(value, int):
        return str(value)
    if value == '_' or value not in bindings:
        raise ValueError(f'Unsafe or anonymous variable: {value}')
    return bindings[value]


def rule_sql(program: Program, rule: Rule, tables, *, distinct=True):
    if any(isinstance(arg, Count) for arg in rule.head.arguments):
        inner, position = degree_spec(rule)
        if inner.name not in program.columns or len(program.columns[inner.name]) != 2:
            raise ValueError('Undeclared/wrong-arity degree source')
        source_column = identifier(program.columns[inner.name][position])
        columns = program.columns[rule.head.name]
        return (f'SELECT {source_column} AS {identifier(columns[0])}, '
                f'count(*) AS {identifier(columns[1])} FROM {identifier(tables[inner.name])} '
                f'GROUP BY {source_column}')
    bindings = {}
    clauses = []
    from_sql = ''
    atoms = [literal for literal in rule.body if isinstance(literal, Atom)]
    if not atoms:
        raise ValueError('Rules must contain a positive relational atom')
    for index, literal in enumerate(atoms):
        alias = f'r{index}'
        conditions = []
        for column, argument in zip(program.columns[literal.name], literal.arguments):
            expression = f'{alias}.{identifier(column)}'
            if argument == '_':
                continue  # Each occurrence is a separate existential variable.
            if isinstance(argument, int):
                conditions.append(f'{expression} = {argument}')
            elif argument in bindings:
                conditions.append(f'{expression} = {bindings[argument]}')
            else:
                bindings[argument] = expression
        table = identifier(tables[literal.name])
        if not index:
            from_sql = f'{table} AS {alias}'
            clauses.extend(conditions)
        else:
            from_sql += (f' JOIN {table} AS {alias} ON ' + ' AND '.join(conditions)
                         if conditions else f' CROSS JOIN {table} AS {alias}')
    for literal in rule.body:
        if isinstance(literal, Comparison):
            clauses.append(f'{_expression(literal.left, bindings)} {literal.operator} '
                           f'{_expression(literal.right, bindings)}')
    selection = ', '.join(f'{_expression(value, bindings)} AS {identifier(column)}'
                          for column, value in zip(program.columns[rule.head.name], rule.head.arguments))
    return ('SELECT ' + ('DISTINCT ' if distinct else '') + selection + ' FROM ' + from_sql
            + (' WHERE ' + ' AND '.join(clauses) if clauses else ''))


def uniqueness_proof(program, rule, keys):
    """Prove a head tuple determines at most one complete body assignment.

    Every input/intermediate is a set, giving a full-tuple key. Degree tables
    additionally have their grouping key. Closing head variables through these
    keys accounts for deterministic degree values without assuming any entity
    key or property of the graph data. Anonymous occurrences are never keys
    supplied by the head.
    """
    known = {argument for argument in rule.head.arguments
             if isinstance(argument, str) and argument != '_'}
    atoms = [literal for literal in rule.body if isinstance(literal, Atom)]

    def covered(literal, key):
        return all(isinstance(literal.arguments[index], int)
                   or literal.arguments[index] in known for index in key)

    while True:
        previous = set(known)
        for literal in atoms:
            if any(covered(literal, key) for key in keys[literal.name]):
                known.update(argument for argument in literal.arguments
                             if isinstance(argument, str) and argument != '_')
        if previous == known:
            break
    return {'unique': all(any(covered(literal, key) for key in keys[literal.name]) for literal in atoms),
            'determined_variables': sorted(known),
            'argument_keys': {name: [list(key) for key in choices] for name, choices in keys.items()
                              if name in {literal.name for literal in atoms}}}


def compile_program(program: Program, *, input_table='R', input_columns=('col0', 'col1'),
                    prefix='fl', force_order=True, keep_all_distinct=False) -> SQLPlan:
    identifier(input_table)
    identifier(prefix)
    if len(input_columns) != len(program.columns['Arc']):
        raise ValueError('Input column mapping has the wrong arity')
    order = dependency_order(program)
    tables = {name: prefix + '_' + name for name in order}
    if input_table.lower() in {name.lower() for name in tables.values()}:
        raise ValueError('Input table collides with a generated table')
    setting = 'join_order,build_side_probe_side' if force_order else ''
    statements = [f"SET disabled_optimizers = '{setting}';"]
    input_selection = ', '.join(f'{identifier(source)} AS {identifier(target)}'
                                for source, target in zip(input_columns, program.columns['Arc']))
    statements.append(f'CREATE TEMP TABLE {identifier(tables["Arc"])} AS '
                      f'SELECT DISTINCT {input_selection} FROM {identifier(input_table)};')
    definitions = program.definitions()
    keys = {name: (tuple(range(len(program.columns[name]))),) for name in order}
    for name in order:
        if any(isinstance(argument, Count) for rule in definitions.get(name, []) for argument in rule.head.arguments):
            # degree_spec verifies exactly one grouping key in head position 0.
            for rule in definitions[name]:
                degree_spec(rule)
            keys[name] = ((0,), *keys[name])
    stages = []
    for name in order:
        if name in program.inputs:
            continue
        rules = definitions[name]
        if len(rules) > 1 and any(isinstance(arg, Count) for rule in rules for arg in rule.head.arguments):
            raise ValueError('Multiple aggregate definitions are unsupported')
        proofs = [uniqueness_proof(program, rule, keys) for rule in rules]
        needs_distinct = len(rules) == 1 and (keep_all_distinct or not proofs[0]['unique'])
        queries = [rule_sql(program, rule, tables, distinct=needs_distinct) for rule in rules]
        statements.append(f'CREATE TEMP TABLE {identifier(tables[name])} AS '
                          + '\nUNION\n'.join(queries) + ';')
        stages.append({'relation': name, 'table': tables[name], 'columns': list(program.columns[name]),
                       'rule_count': len(rules),
                       'dependencies': sorted({lit.name for rule in rules for lit in rule.body if isinstance(lit, Atom)}),
                       'set_boundary': 'GROUP BY' if any(isinstance(arg, Count) for arg in rules[0].head.arguments)
                                       else 'UNION' if len(rules) > 1 else 'SELECT DISTINCT' if needs_distinct
                                       else 'join is statically unique on head',
                       'uniqueness_proofs': proofs,
                       'rule_body_order': [[lit.name for lit in rule.body if isinstance(lit, Atom)] for rule in rules]})
    statements += [f'SELECT count(*) AS result_count FROM {identifier(tables[program.output])};',
                   "SET disabled_optimizers = '';"]
    manifest = {'semantics': 'nonrecursive Datalog sets; full output tuples materialized before COUNT',
                'input_table': input_table, 'input_columns': list(input_columns),
                'input_requirements': 'nonNULL integer values; duplicate rows are deduplicated before degree counts',
                'source_sha256': hashlib.sha256(program.source.encode()).hexdigest(),
                'output': program.output, 'output_columns': list(program.columns[program.output]),
                'force_rule_body_join_order': force_order,
                'distinct_elision': 'disabled' if keep_all_distinct else 'only with head-to-body key closure proof',
                'degree_policy': 'literal source aggregate/guard expressions; no substitution of intermediate extension degrees',
                'cross_relation_merge_policy': 'none; every named producer kept; multiple rules use set UNION',
                'reachable_order': list(order), 'unused_relations': sorted(set(program.columns) - set(order)),
                'stages': stages}
    return SQLPlan(tuple(statements), manifest, tables)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path, help='.dl file or directory containing plans')
    parser.add_argument('output', type=Path, help='.sql file or output directory')
    parser.add_argument('--input-table', default='R')
    parser.add_argument('--input-columns', nargs=2, default=['col0', 'col1'])
    parser.add_argument('--allow-reorder', action='store_true')
    parser.add_argument('--keep-all-distinct', action='store_true', help='conservative reference mode')
    parser.add_argument('--layout', choices=('cte', 'materialized'), default='cte')
    parser.add_argument('--cte-materialization', choices=('auto', 'inline', 'all'), default='auto')
    parser.add_argument('--assume-set-input', action='store_true',
                        help='CTE mode: caller certifies a duplicate-free input, avoiding an input copy/dedup')
    parser.add_argument('--revision', default=None)
    args = parser.parse_args()
    if args.layout == 'cte' and args.keep_all_distinct:
        parser.error('--keep-all-distinct is available with --layout materialized')
    if args.layout == 'materialized' and args.assume_set_input:
        parser.error('--assume-set-input is available with --layout cte')
    files = [args.source] if args.source.is_file() else sorted(args.source.rglob('*.dl'))
    if not files:
        parser.error('No .dl source files found')
    manifest = {'revision': args.revision, 'source_root': str(args.source.resolve()),
                'layout': args.layout, 'plans': []}
    for source in files:
        relative = Path(source.name) if args.source.is_file() else source.relative_to(args.source)
        program = parse_program(source.read_text())
        if args.layout == 'cte':
            from .cte import compile_cte_program
            plan = compile_cte_program(program, input_table=args.input_table, input_columns=args.input_columns,
                                       force_order=not args.allow_reorder,
                                       assume_set_input=args.assume_set_input,
                                       materialization=args.cte_materialization)
        else:
            plan = compile_program(program, input_table=args.input_table, input_columns=args.input_columns,
                                   force_order=not args.allow_reorder, keep_all_distinct=args.keep_all_distinct)
        target = args.output if args.source.is_file() else args.output / relative.with_suffix('.sql')
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(plan.sql)
        manifest['plans'].append({'source': str(relative), 'sql': target.name if args.source.is_file()
                                  else str(relative.with_suffix('.sql')),
                                  'sql_sha256': hashlib.sha256(plan.sql.encode()).hexdigest(), **plan.manifest})
    target_manifest = args.output.with_suffix('.json') if args.source.is_file() else args.output / 'manifest.json'
    target_manifest.write_text(json.dumps(manifest, indent=2) + '\n')
    print(f'Translated {len(files)} programs; manifest: {target_manifest}')


if __name__ == '__main__':
    # The CTE backend imports these AST classes by the package module name.
    # Running with -m must not hand it instances of a second __main__.Atom.
    from .translate import main as package_main
    package_main()
