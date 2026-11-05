"""Regressions for projection, branch identity, set unions and degree semantics."""
import unittest
from pathlib import Path

import duckdb

from .reference import evaluate, brute_force_query
from .translate import compile_program, parse_program


INPUT = '.decl Arc(x: int32,y: int32)\n.input Arc\n'
DEGREES = '''
.decl OutDeg(x: int32,k: int32)
.decl InDeg(x: int32,k: int32)
OutDeg(x,count : {Arc(x,y)}) :- Arc(x,_).
InDeg(y,count : {Arc(x,y)}) :- Arc(_,y).
'''


class TranslateTest(unittest.TestCase):
    def check(self, body, edges, expected):
        program = parse_program(INPUT + body + '\n.printsize Result\n')
        for force in (False, True):
            with self.subTest(force_order=force), duckdb.connect(config={'threads': 1}) as con:
                con.execute('CREATE TABLE R(col0 BIGINT,col1 BIGINT)')
                if edges:
                    con.executemany('INSERT INTO R VALUES (?,?)', edges)
                plan = compile_program(program, force_order=force)
                count = None
                for statement in plan.statements:
                    result = con.execute(statement).fetchall()
                    if statement.startswith('SELECT'):
                        count = result[0][0]
                rows = con.execute(f'SELECT * FROM "{plan.tables[program.output]}"').fetchall()
                self.assertEqual(set(rows), expected)
                self.assertEqual(len(rows), len(expected))
                self.assertEqual(count, len(expected))
                self.assertEqual(evaluate(program, edges)[program.output], expected)
                self.assertEqual(con.execute("SELECT current_setting('disabled_optimizers')").fetchone()[0], '')
        return plan

    def test_projected_cases_overlap_and_need_set_union(self):
        self.check('''
.decl Result(x: int32)
Result(x) :- Arc(x,y),y<=0.
Result(x) :- Arc(x,z),z>=0.
''', [(1, -1), (1, 1), (2, 0), (2, 0), (3, 5)], {(1,), (2,), (3,)})

    def test_equal_schema_branch_tables_cannot_be_pooled(self):
        plan = self.check('''
.decl L(x: int32,y: int32)
.decl H(x: int32,y: int32)
.decl A(x: int32,z: int32)
.decl B(x: int32,z: int32)
.decl Result(x: int32,z: int32)
L(x,y) :- Arc(x,y),y<=2.
H(x,y) :- Arc(x,y),y>2.
A(x,z) :- L(x,y),Arc(y,z).
B(x,z) :- H(x,y),Arc(y,z).
Result(x,z) :- A(x,z),B(x,z).
''', [(1, 2), (2, 9), (1, 3), (3, 8), (4, 2), (4, 3), (3, 9)], {(1, 9), (4, 9)})
        self.assertNotEqual(plan.tables['A'], plan.tables['B'])

    def test_degree_counts_deduplicate_input_and_keep_missing_keys_absent(self):
        self.check(DEGREES + '''
.decl Result(x: int32,y: int32)
Result(x,y) :- Arc(x,y),OutDeg(y,d0),InDeg(x,d1),d0<=d1.
''', [(0, 1), (1, 2), (2, 0), (2, 3), (3, 3), (3, 3), (8, 9)],
                   {(0, 1), (2, 0), (2, 3), (3, 3)})

    def test_anonymous_variables_are_independent_existentials(self):
        self.check('''
.decl Result(x: int32)
Result(x) :- Arc(x,_),Arc(_,x).
''', [(1, 2), (3, 1)], {(1,)})

    def test_projection_must_deduplicate_multiple_triangle_witnesses(self):
        plan = self.check('''
.decl T(a: int32,b: int32,c: int32)
.decl P(b: int32,c: int32)
.decl Result(b: int32,c: int32)
T(a,b,c) :- Arc(a,b),Arc(b,c),Arc(a,c).
P(b,c) :- T(_,b,c).
Result(b,c) :- P(b,c).
''', [(0, 1), (0, 2), (3, 1), (3, 2), (1, 2)], {(1, 2)})
        stages = {stage['relation']: stage for stage in plan.manifest['stages']}
        self.assertEqual(stages['T']['set_boundary'], 'join is statically unique on head')
        self.assertEqual(stages['P']['set_boundary'], 'SELECT DISTINCT')

    def test_argument_permutation_uses_positions_not_declaration_names(self):
        self.check('''
.decl Reversed(right: int32,left: int32)
.decl Result(a: int32,b: int32)
Reversed(y,x) :- Arc(x,y).
Result(a,b) :- Reversed(b,a).
''', [(1, 2), (3, 4)], {(1, 2), (3, 4)})

    def test_repeated_variables_constants_and_inequality(self):
        self.check('''
.decl Result(a: int32,b: int32)
Result(a,b) :- Arc(a,a),Arc(b,-1),a!=b.
''', [(0, 0), (1, 1), (0, -1), (2, -1)], {(0, 2), (1, 0), (1, 2)})

    def test_empty_inputs_and_unused_degrees(self):
        plan = self.check(DEGREES + '''
.decl Result(x: int32,y: int32)
Result(x,y) :- Arc(x,y).
''', [], set())
        self.assertIn('OutDeg', plan.manifest['unused_relations'])
        self.assertNotIn('OutDeg', plan.tables)

    def test_recursion_negation_and_unsafe_rules_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Recursive'):
            parse_program(INPUT + '.decl Result(x: int32)\nResult(x):-Result(x).\n.printsize Result')
        with self.assertRaises(ValueError):
            parse_program(INPUT + '.decl Result(x: int32)\nResult(x):-!Arc(x,_).\n.printsize Result')
        unsafe = parse_program(INPUT + '.decl Result(x: int32)\nResult(x):-Arc(y,z).\n.printsize Result')
        with self.assertRaisesRegex(ValueError, 'Unsafe'):
            compile_program(unsafe)

    def test_readable_9rr_example_streams_the_original_cq_count(self):
        from .validate import graph_fixtures
        query = 'Arc(a,b),Arc(b,c),Arc(c,d),Arc(d,e),Arc(e,a),Arc(a,c),Arc(b,d)'
        reference = parse_program(INPUT + f'// query : {query}\n'
            + '.decl Result(a: int32,b: int32,c: int32,d: int32,e: int32)\n'
            + f'Result(a,b,c,d,e) :- {query}.\n.printsize Result\n')
        sql = (Path(__file__).parent / 'examples/5cycle_2_9rr.sql').read_text()
        for name, edges in graph_fixtures(random_graphs=3):
            with self.subTest(graph=name), duckdb.connect(config={'threads': 1}) as con:
                con.execute('CREATE TABLE R(col0 BIGINT,col1 BIGINT)')
                if edges:
                    con.executemany('INSERT INTO R VALUES (?,?)', edges)
                answer = None
                for statement in con.extract_statements(sql):
                    rows = con.execute(statement).fetchall()
                    if statement.type == duckdb.StatementType.SELECT:
                        answer = rows[0][0]
                self.assertEqual(answer, len(brute_force_query(reference, edges)))


if __name__ == '__main__':
    unittest.main()
