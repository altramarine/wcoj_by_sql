"""Necessary guards on output tuples, used to certify disjoint rule unions.

Facts refer to head-column positions and complete named degree producers in one
program. Projected-away witnesses cannot appear in a published fact. This is a
conservative syntactic proof, with no sampling, statistics or cross-case pooling.
"""
from dataclasses import dataclass
from itertools import combinations

from .translate import Atom, Comparison, Count, dependency_order


@dataclass(frozen=True)
class Expr:
    kind: str
    value: str | int
    arguments: tuple = ()

    def show(self):
        if self.kind == 'column':
            return f'${self.value}'
        if self.kind == 'degree':
            return f'{self.value}({self.arguments[0].show()})'
        return str(self.value)


@dataclass(frozen=True)
class Fact:
    left: Expr
    operator: str
    right: Expr

    def show(self):
        return f'{self.left.show()} {self.operator} {self.right.show()}'


INVERSE = {'<': '>', '<=': '>=', '>': '<', '>=': '<=', '=': '=', '!=': '!='}
OUTCOMES = {'<': {-1}, '<=': {-1, 0}, '>': {1}, '>=': {0, 1}, '=': {0}, '!=': {-1, 1}}


def normalized(left, op, right):
    if repr(left) > repr(right):
        left, right, op = right, left, INVERSE[op]
    return Fact(left, op, right)


def term_expr(value):
    return Expr('constant' if isinstance(value, int) else 'variable', value)


def replace_columns(expression, arguments):
    if expression.kind == 'column':
        return term_expr(arguments[expression.value])
    return Expr(expression.kind, expression.value,
                tuple(replace_columns(arg, arguments) for arg in expression.arguments))


def guaranteed_guards(program):
    definitions = program.definitions()
    predicates = {}
    rule_facts = {}
    degree_names = {name for name, rules in definitions.items()
                    if len(rules) == 1 and isinstance(rules[0].head.arguments[-1], Count)}
    for name in dependency_order(program):
        variants = []
        for rule in definitions.get(name, []):
            head = {arg: index for index, arg in enumerate(rule.head.arguments)
                    if isinstance(arg, str) and arg != '_'}
            degree_values = {}
            atoms = [lit for lit in rule.body if isinstance(lit, Atom)]
            for lit in atoms:
                if lit.name in degree_names and isinstance(lit.arguments[1], str):
                    degree_values.setdefault(lit.arguments[1], Expr('degree', lit.name,
                                                                   (term_expr(lit.arguments[0]),)))

            def publish(expression, seen=frozenset()):
                if expression.kind == 'variable':
                    variable = expression.value
                    if variable in head:
                        return Expr('column', head[variable])
                    if variable != '_' and variable in degree_values and variable not in seen:
                        return publish(degree_values[variable], seen | {variable})
                    raise ValueError('Guard depends on a projected/private variable')
                return Expr(expression.kind, expression.value,
                            tuple(publish(arg, seen) for arg in expression.arguments))

            candidates = []
            for lit in rule.body:
                if isinstance(lit, Comparison):
                    candidates.append((term_expr(lit.left), lit.operator, term_expr(lit.right)))
                else:
                    for fact in predicates.get(lit.name, ()):
                        candidates.append((replace_columns(fact.left, lit.arguments), fact.operator,
                                           replace_columns(fact.right, lit.arguments)))
            facts = set()
            for left, op, right in candidates:
                try:
                    facts.add(normalized(publish(left), op, publish(right)))
                except ValueError:
                    continue
            rule_facts[rule] = frozenset(facts)
            variants.append(facts)
        predicates[name] = frozenset(set.intersection(*variants)) if variants else frozenset()
    return rule_facts


def disjoint_union_proof(rules, rule_facts):
    proofs = []
    unproved = []
    for left_index, right_index in combinations(range(len(rules)), 2):
        witness = None
        for left in sorted(rule_facts[rules[left_index]], key=repr):
            for right in sorted(rule_facts[rules[right_index]], key=repr):
                if (left.left == right.left and left.right == right.right
                        and not OUTCOMES[left.operator] & OUTCOMES[right.operator]):
                    witness = {'rules': [left_index, right_index],
                               'left_guard': left.show(), 'right_guard': right.show()}
                    break
            if witness:
                break
        if witness:
            proofs.append(witness)
        else:
            unproved.append([left_index, right_index])
    return {'disjoint': not unproved, 'pairwise_witnesses': proofs, 'unproved_pairs': unproved,
            'scope': 'necessary guards evaluated on the same complete output tuple; private witness guards discarded'}
