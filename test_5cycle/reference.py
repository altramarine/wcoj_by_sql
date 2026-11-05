"""Independent set evaluator and brute-force original-CQ oracle for small graphs."""
from collections import Counter, defaultdict
from itertools import product
import operator
import re

from .translate import Atom, Comparison, Count, Program, Rule, atom, degree_spec, dependency_order


OPERATORS = {'<': operator.lt, '<=': operator.le, '>': operator.gt,
             '>=': operator.ge, '=': operator.eq, '!=': operator.ne}


def value(term, environment):
    return term if isinstance(term, int) else environment[term]


def evaluate_rule(rule: Rule, relations):
    if any(isinstance(argument, Count) for argument in rule.head.arguments):
        source, position = degree_spec(rule)
        return set(Counter(row[position] for row in relations[source.name]).items())
    environments = [{}]
    comparisons = [literal for literal in rule.body if isinstance(literal, Comparison)]
    for literal in rule.body:
        if not isinstance(literal, Atom):
            continue
        result = []
        # All current environments bind the same variables. Index the next
        # relation by already-bound positions, independently from SQL generation.
        bound = set(environments[0]) if environments else set()
        positions = [index for index, argument in enumerate(literal.arguments)
                     if isinstance(argument, int) or argument in bound]
        index = defaultdict(list)
        for row in relations[literal.name]:
            index[tuple(row[position] for position in positions)].append(row)
        for environment in environments:
            key = tuple(value(literal.arguments[position], environment) for position in positions)
            for row in index.get(key, ()):
                extended = dict(environment)
                for argument, item in zip(literal.arguments, row):
                    if argument == '_':
                        continue
                    if isinstance(argument, int):
                        if argument != item:
                            break
                    elif argument in extended:
                        if extended[argument] != item:
                            break
                    else:
                        extended[argument] = item
                else:
                    if all(OPERATORS[c.operator](value(c.left, extended), value(c.right, extended))
                           for c in comparisons if all(isinstance(t, int) or t in extended
                                                       for t in (c.left, c.right))):
                        result.append(extended)
        environments = result
        if not environments:
            break
    return {tuple(value(argument, environment) for argument in rule.head.arguments)
            for environment in environments
            if all(OPERATORS[c.operator](value(c.left, environment), value(c.right, environment))
                   for c in comparisons)}


def evaluate(program: Program, edges):
    relations = {'Arc': set(map(tuple, edges))}
    definitions = program.definitions()
    for name in dependency_order(program):
        if name in program.inputs:
            continue
        relations[name] = set().union(*(evaluate_rule(rule, relations) for rule in definitions[name]))
    return relations


def original_query(program: Program):
    match = re.search(r'(?m)^//\s*query\s*:\s*(.*)', program.source)
    if not match:
        raise ValueError('No original CQ header; cannot validate query equivalence')
    from .translate import split_top_level
    atoms = tuple(atom(part) for part in split_top_level(match[1]))
    if any(relation.name != 'Arc' for relation in atoms):
        raise ValueError('Original-query oracle supports Arc atoms only')
    heads = [rule.head.arguments for rule in program.rules if rule.head.name == program.output]
    if len(set(heads)) != 1:
        raise ValueError('Original-query oracle requires a consistent named output order')
    return atoms, heads[0]


def brute_force_query(program: Program, edges):
    atoms, output = original_query(program)
    edges = set(map(tuple, edges))
    domain = sorted({item for edge in edges for item in edge})
    variables = sorted({argument for relation in atoms for argument in relation.arguments})
    answers = set()
    # This enumerates assignments, allowing equal vertex values and self-loops.
    # It does not use the translated join tree, degree rules, or SQL compiler.
    for assignment in product(domain, repeat=len(variables)):
        environment = dict(zip(variables, assignment))
        if all(tuple(environment[arg] for arg in relation.arguments) in edges for relation in atoms):
            answers.add(tuple(environment[arg] for arg in output))
    return answers
