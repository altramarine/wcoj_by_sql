"""Ordered WITH backend following the original wcoj_by_sql execution shape.

Degree/routing and reused producers get MATERIALIZED boundaries. Single-use
joins flow through NOT MATERIALIZED CTEs into COUNT. Closing predicates become
SEMI JOINs; UNION ALL requires a symbolic proof of disjoint complete head tuples.
"""
from collections import Counter, defaultdict
from dataclasses import dataclass

from .conditions import INVERSE, disjoint_union_proof, guaranteed_guards
from .translate import (Atom, Comparison, Count, Rule, SQLPlan, _expression,
                        compile_program, degree_spec, identifier, rule_sql,
                        uniqueness_proof)


@dataclass(frozen=True)
class CTEPlan(SQLPlan):
    with_clause: str
    output_query: str

    def relation_query(self, relation):
        """Diagnostic full tuple query, without making every stage a temp table."""
        query = self.output_query if relation == self.manifest['output'] else (
            f'SELECT * FROM {identifier(self.tables[relation])}')
        return self.with_clause + '\n' + query


def ordered_rule_sql(program, rule, tables, *, distinct, route=None):
    if any(isinstance(arg, Count) for arg in rule.head.arguments):
        return rule_sql(program, rule, tables), []
    bindings = {}
    filters = []
    from_sql = ''
    joins = []
    atoms = [lit for lit in rule.body if isinstance(lit, Atom)]
    for index, lit in enumerate(atoms):
        alias = f'r{index}'
        # No named output or future guard can need a new column from this atom.
        # Anonymous witnesses are existential, so a SEMI JOIN is valid as well.
        semi = bool(index) and all(arg == '_' or isinstance(arg, int) or arg in bindings
                                   for arg in lit.arguments)
        conditions = []
        for column, argument in zip(program.columns[lit.name], lit.arguments):
            expr = f'{alias}.{identifier(column)}'
            if argument == '_':
                continue
            if isinstance(argument, int):
                conditions.append(f'{expr} = {argument}')
            elif argument in bindings:
                conditions.append(f'{expr} = {bindings[argument]}')
            else:
                bindings[argument] = expr
        source = f'{identifier(tables[lit.name])} AS {alias}'
        if not index:
            from_sql = source
            filters.extend(conditions)
        else:
            join_type = 'SEMI JOIN' if semi else 'JOIN'
            from_sql = '(' + from_sql + (f' {join_type} {source} ON ' + ' AND '.join(conditions)
                        if conditions else f' {join_type} {source} ON true' if semi
                        else f' CROSS JOIN {source}') + ')'
            joins.append({'position': index, 'relation': lit.name,
                          'operator': 'SEMI' if semi else 'INNER' if conditions else 'CROSS'})
    for lit in rule.body:
        if isinstance(lit, Comparison):
            filters.append(f'{_expression(lit.left, bindings)} {lit.operator} {_expression(lit.right, bindings)}')
    selection = [f'{_expression(value, bindings)} AS {identifier(column)}'
                 for column, value in zip(program.columns[rule.head.name], rule.head.arguments)]
    if route:
        comparison, tag_column = route
        condition = (f'{_expression(comparison.left, bindings)} {comparison.operator} '
                     f'{_expression(comparison.right, bindings)}')
        selection.append(f'CASE WHEN {condition} THEN 0 ELSE 1 END AS {identifier(tag_column)}')
    return ('SELECT ' + ('DISTINCT ' if distinct else '') + ', '.join(selection) + ' FROM ' + from_sql
            + (' WHERE ' + ' AND '.join(filters) if filters else '')), joins


def _complement(left, right):
    opposite = {'<': '>=', '<=': '>', '>': '<=', '>=': '<', '=': '!=', '!=': '='}
    op = right.operator
    if (left.left, left.right) == (right.right, right.left):
        op = INVERSE[op]
    elif (left.left, left.right) != (right.left, right.right):
        return False
    return opposite[left.operator] == op


def base_routes(program, order, keys):
    """Factor only identical unrestricted Arc+degree producers into tagged rows."""
    definitions = program.definitions()
    degree_names = {name for name in order if any(isinstance(arg, Count)
                    for rule in definitions.get(name, []) for arg in rule.head.arguments)}
    groups = defaultdict(list)
    for name in order:
        rules = definitions.get(name, [])
        if len(rules) != 1:
            continue
        rule = rules[0]
        atoms = tuple(lit for lit in rule.body if isinstance(lit, Atom))
        comparisons = [lit for lit in rule.body if isinstance(lit, Comparison)]
        if (len(comparisons) != 1 or len(atoms) != 3 or atoms[0].name != 'Arc'
                or atoms[0].arguments != rule.head.arguments or len(rule.head.arguments) != 2
                or any(lit.name not in degree_names for lit in atoms[1:])):
            continue
        if any(degree_spec(definitions[lit.name][0])[0].name != 'Arc' for lit in atoms[1:]):
            continue
        positive = Rule(rule.head, atoms)
        if not uniqueness_proof(program, positive, keys)['unique']:
            continue
        groups[(program.columns[name], rule.head.arguments, atoms)].append((name, rule, comparisons[0]))
    result = []
    for choices in groups.values():
        remaining = list(choices)
        while remaining:
            first = remaining.pop(0)
            matched = next((item for item in remaining if _complement(first[2], item[2])), None)
            if matched is None:
                continue
            remaining.remove(matched)
            # Give the non-strict (tie-accepting) rule tag 0 when possible.
            if matched[2].operator in ('<=', '>='):
                first, matched = matched, first
            result.append((first, matched))
    return result


def compile_cte_program(program, *, input_table='R', input_columns=('col0', 'col1'),
                        prefix='fl', force_order=True, assume_set_input=False,
                        materialization='auto'):
    if materialization not in ('auto', 'inline', 'all'):
        raise ValueError('Unknown CTE materialization policy')
    reference = compile_program(program, input_table=input_table, input_columns=input_columns,
                                prefix=prefix, force_order=force_order)
    order = reference.manifest['reachable_order']
    tables = reference.tables
    definitions = program.definitions()
    keys = {name: (tuple(range(len(program.columns[name]))),) for name in order}
    degree_names = set()
    for name in order:
        if any(isinstance(arg, Count) for rule in definitions.get(name, []) for arg in rule.head.arguments):
            keys[name] = ((0,), *keys[name])
            degree_names.add(name)
    facts = guaranteed_guards(program)
    tag_column = '__fl_case'
    while any(tag_column in columns for columns in program.columns.values()):
        tag_column += '_'
    routes = []
    routed = {}
    occupied = ({table.lower() for table in tables.values()} | {input_table.lower()}
                | {name.lower() for name in program.columns})
    for first, second in base_routes(program, order, keys):
        index = len(routes)
        name = f'{prefix}_route_{index}'
        while name.lower() in occupied:
            name += '_'
        occupied.add(name.lower())
        route = {'id': name, 'true_predicate': first[0], 'false_predicate': second[0],
                 'tag_column': tag_column, 'heads': program.columns[first[0]], 'rule': first[1],
                 'comparison': first[2],
                 'producer_policy': 'same complete positive body and full Arc head; complementary guard retained by tag'}
        routes.append(route)
        routed[first[0]], routed[second[0]] = (route, 0), (route, 1)
    queries = {}
    dependencies = {}
    stages = []
    stage_order = []
    input_selection = ', '.join(f'{identifier(source)} AS {identifier(target)}'
                                for source, target in zip(input_columns, program.columns['Arc']))
    queries['Arc'] = f'SELECT {"" if assume_set_input else "DISTINCT "}{input_selection} FROM {identifier(input_table)}'
    dependencies['Arc'] = []
    stage_order.append('Arc')
    emitted_routes = set()
    for name in order:
        if name in program.inputs:
            continue
        rules = definitions[name]
        if name in routed:
            route, tag = routed[name]
            parent = route['id']
            if parent not in emitted_routes:
                positive = Rule(route['rule'].head, tuple(lit for lit in route['rule'].body if isinstance(lit, Atom)))
                queries[parent], joins = ordered_rule_sql(program, positive, tables, distinct=False,
                                                         route=(route['comparison'], tag_column))
                dependencies[parent] = [lit.name for lit in positive.body]
                stage_order.append(parent)
                stages.append({'relation': parent, 'table': parent, 'kind': 'tagged base routing',
                               'rule_joins': [joins], 'true_predicate': route['true_predicate'],
                               'false_predicate': route['false_predicate'],
                               'guard': repr(route['comparison']), 'tag_column': tag_column,
                               'producer_policy': route['producer_policy']})
                emitted_routes.add(parent)
            selection = ', '.join(identifier(column) for column in program.columns[name])
            queries[name] = (f'SELECT {selection} FROM {identifier(parent)} '
                             f'WHERE {identifier(tag_column)} = {tag}')
            dependencies[name] = [parent]
            union_proof = None
            joins = []
            kind = 'case-specific view of tagged routing'
        else:
            union_proof = disjoint_union_proof(rules, facts) if len(rules) > 1 else None
            union_all = bool(union_proof and union_proof['disjoint'])
            parts = []
            joins = []
            for rule in rules:
                unique = uniqueness_proof(program, rule, keys)['unique']
                needs_distinct = (len(rules) == 1 or union_all) and not unique
                query, rule_joins = ordered_rule_sql(program, rule, tables, distinct=needs_distinct)
                parts.append(query)
                joins.append(rule_joins)
            queries[name] = ('\nUNION ALL\n' if union_all else '\nUNION\n').join(parts)
            dependencies[name] = [lit.name for rule in rules for lit in rule.body if isinstance(lit, Atom)]
            kind = 'degree grouping' if name in degree_names else 'ordered rule join'
        stage_order.append(name)
        stages.append({'relation': name, 'table': tables[name], 'kind': kind,
                       'rule_joins': joins, 'union': 'UNION ALL' if union_proof and union_proof['disjoint']
                       else 'UNION' if union_proof else None, 'disjointness_proof': union_proof})
    uses = Counter(dep for name, deps in dependencies.items() for dep in deps)
    clauses = []
    hints = {}
    reasons = {}
    route_ids = {route['id'] for route in routes}
    for name in stage_order:
        if name == program.output:
            continue  # Stream the final expression directly to COUNT.
        if name in routed:
            keep, reason = False, 'keep tag-specific view inline; shared parent owns routing'
        elif name == 'Arc':
            keep, reason = not assume_set_input, 'deduplicate the input once' if not assume_set_input else 'caller certifies an existing edge set'
        elif name in degree_names:
            keep, reason = True, 'degree preprocessing boundary'
        elif name in route_ids:
            keep, reason = True, 'shared routing boundary, like node_best'
        else:
            keep, reason = uses[name] > 1, 'reused complete producer' if uses[name] > 1 else 'single-use join/projection pipeline'
        if materialization != 'auto':
            keep, reason = materialization == 'all', f'explicit {materialization} experiment'
        hint = 'MATERIALIZED' if keep else 'NOT MATERIALIZED'
        hints[name], reasons[name] = hint, reason
        table = name if name in route_ids else tables[name]
        clauses.append(f'  {identifier(table)} AS {hint} (\n    ' + queries[name].replace('\n', '\n    ') + '\n  )')
    with_clause = 'WITH\n' + ',\n'.join(clauses)
    query = ('SELECT count(*) AS result_count FROM (\n' + with_clause + '\n'
             + queries[program.output] + '\n) AS answers;')
    setting = 'join_order,build_side_probe_side' if force_order else ''
    statements = (f"SET disabled_optimizers = '{setting}';", query,
                  "SET disabled_optimizers = '';")
    for stage in stages:
        name = stage['relation']
        stage.update({'materialization': hints.get(name, 'streamed final expression'),
                      'reason': reasons.get(name, 'COUNT consumes final rows without a Result table'),
                      'references': uses[name]})
    manifest = {**reference.manifest, 'layout': 'ordered WITH closure',
                'semantics': 'Datalog sets; COUNT consumes the final expression without Result CTAS',
                'assume_set_input': assume_set_input, 'materialization_policy': materialization,
                'input_requirements': ('nonNULL integer values; caller certifies duplicate-free input'
                                       if assume_set_input else reference.manifest['input_requirements']),
                'stages': stages, 'routing_count': len(routes),
                'cross_relation_merge_policy': 'named child producers stay independent; only identical public base routing bodies factored with retained tags',
                'union_policy': 'UNION ALL only after static pairwise necessary-guard contradiction proof',
                'source_project_examples': ['queries/q4_plans/plan1-merged-cte.sql', 'make_split_plan.py', 'make_binary_plan.py']}
    return CTEPlan(statements, manifest, tables, with_clause, queries[program.output])
