"""Ordered execution, set/projection safety, and proof-gated bag unions."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import duckdb

from .cte import compile_cte_program
from .reference import evaluate
from .test_translate import DEGREES, INPUT
from .translate import parse_program


class CTEBackendTests(unittest.TestCase):
    def test_module_cli_uses_the_backend_ast_types(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / 'query.dl', Path(directory) / 'query.sql'
            source.write_text(INPUT + '''
.decl Result(x: int32,y: int32)
Result(x,y) :- Arc(x,y).
.printsize Result
''')
            subprocess.run([sys.executable, '-m', 'test_5cycle.translate', str(source), str(target)],
                           cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True)
            self.assertIn('WITH\n', target.read_text())
            self.assertEqual(json.loads(target.with_suffix('.json').read_text())['layout'], 'cte')

    def check(self, body, edges, expected=None):
        program = parse_program(INPUT + body + '\n.printsize Result\n')
        oracle = evaluate(program, edges)
        if expected is not None:
            self.assertEqual(oracle['Result'], expected)
        for policy in ('auto', 'inline', 'all'):
            plan = compile_cte_program(program, materialization=policy)
            with self.subTest(policy=policy), duckdb.connect(config={'threads': 1}) as con:
                con.execute('CREATE TABLE R(col0 BIGINT,col1 BIGINT)')
                if edges:
                    con.executemany('INSERT INTO R VALUES (?,?)', edges)
                count = None
                for statement in plan.statements:
                    rows = con.execute(statement).fetchall()
                    if statement.startswith('SELECT'):
                        count = rows[0][0]
                self.assertEqual(count, len(oracle['Result']))
                self.assertEqual(con.execute('SHOW TABLES').fetchall(), [('R',)])
                self.assertEqual(con.execute("SELECT current_setting('disabled_optimizers')").fetchone()[0], '')
                con.execute(plan.statements[0])
                for name in plan.tables:
                    rows = con.execute(plan.relation_query(name)).fetchall()
                    self.assertEqual(set(rows), oracle[name])
                    self.assertEqual(len(rows), len(oracle[name]))
        return compile_cte_program(program)

    def test_disjoint_complete_head_cases_use_union_all(self):
        plan = self.check('''
.decl Result(x: int32,y: int32)
Result(x,y) :- Arc(x,y),y<=2.
Result(x,y) :- Arc(x,y),y>2.
''', [(1, 2), (1, 3), (1, 3)], {(1, 2), (1, 3)})
        self.assertEqual(plan.manifest['stages'][-1]['union'], 'UNION ALL')

    def test_private_projected_witnesses_do_not_prove_disjoint_outputs(self):
        plan = self.check('''
.decl Result(x: int32)
Result(x) :- Arc(x,y),y<=2.
Result(x) :- Arc(x,y),y>2.
''', [(1, 2), (1, 3), (2, 4)], {(1,), (2,)})
        self.assertEqual(plan.manifest['stages'][-1]['union'], 'UNION')

    def test_inherited_degree_guards_survive_column_permutation(self):
        plan = self.check(DEGREES + '''
.decl L(x: int32,y: int32)
.decl H(x: int32,y: int32)
.decl A(v: int32,u: int32)
.decl B(v: int32,u: int32)
.decl Result(v: int32,u: int32)
L(x,y) :- Arc(x,y),OutDeg(y,d0),InDeg(x,d1),d0<=d1.
H(x,y) :- Arc(x,y),OutDeg(y,d0),InDeg(x,d1),d1<d0.
A(y,x) :- L(x,y).
B(y,x) :- H(x,y).
Result(y,x) :- A(y,x).
Result(y,x) :- B(y,x).
''', [(0, 1), (1, 2), (2, 0), (2, 3), (3, 3), (8, 9)])
        self.assertEqual(plan.manifest['routing_count'], 1)
        self.assertEqual(plan.manifest['stages'][-1]['union'], 'UNION ALL')

    def test_different_degree_producers_cannot_certify_a_shared_case(self):
        plan = self.check('''
.decl FA(x: int32,y: int32)
.decl FB(x: int32,y: int32)
.decl DA(x: int32,k: int32)
.decl DB(x: int32,k: int32)
.decl Result(x: int32,y: int32)
FA(x,y) :- Arc(x,y),y<=2.
FB(x,y) :- Arc(x,y),y>2.
DA(x,count:{FA(x,y)}) :- FA(x,_).
DB(x,count:{FB(x,y)}) :- FB(x,_).
Result(x,y) :- Arc(x,y),DA(x,d0),DA(y,d1),d0<=d1.
Result(x,y) :- Arc(x,y),DB(x,d0),DB(y,d1),d0>d1.
''', [(0, 1), (1, 0), (1, 2), (0, 4), (0, 5), (1, 4)], {(0, 1)})
        self.assertEqual(plan.manifest['routing_count'], 0)
        self.assertEqual(plan.manifest['stages'][-1]['union'], 'UNION')

    def test_projection_is_distinct_and_reused_producer_is_materialized(self):
        plan = self.check('''
.decl T(a: int32,b: int32,c: int32)
.decl P(b: int32,c: int32)
.decl Result(a: int32,b: int32,c: int32)
T(a,b,c) :- Arc(a,b),Arc(b,c),Arc(a,c).
P(b,c) :- T(_,b,c).
Result(a,b,c) :- T(a,b,c),P(b,c).
''', [(0, 1), (0, 2), (3, 1), (3, 2), (1, 2)], {(0, 1, 2), (3, 1, 2)})
        stages = {stage['relation']: stage for stage in plan.manifest['stages']}
        self.assertEqual(stages['T']['materialization'], 'MATERIALIZED')
        self.assertIn('SELECT DISTINCT', plan.relation_query('P'))
        self.assertIn('SEMI JOIN', plan.output_query)

    def test_explain_keeps_expansion_before_semi_closure_without_result_ctas(self):
        program = parse_program(INPUT + '''
.decl T(a: int32,b: int32,c: int32)
.decl Result(a: int32,b: int32,c: int32)
T(a,b,c) :- Arc(a,b),Arc(b,c),Arc(a,c).
Result(a,b,c) :- T(a,b,c).
.printsize Result
''')
        plan = compile_cte_program(program, assume_set_input=True)
        with duckdb.connect(config={'threads': 1}) as con:
            con.execute('CREATE TABLE R AS SELECT a AS col0,b AS col1 FROM range(4) t(a),range(4) u(b)')
            con.execute(plan.statements[0])
            tree = json.loads(con.execute('EXPLAIN (FORMAT JSON) ' + plan.statements[1]).fetchone()[1])

        def nodes(items):
            for node in items:
                yield node
                yield from nodes(node.get('children', []))

        joins = [node for node in nodes(tree) if 'JOIN' in node['name']]
        semi = next(node for node in joins if node['extra_info'].get('Join Type') == 'SEMI')
        self.assertTrue(any(node['extra_info'].get('Join Type') == 'INNER'
                            for node in nodes(semi['children'][:1])))
        self.assertNotIn('CREATE TEMP TABLE', plan.sql)
        self.assertNotIn('"fl_Result" AS', plan.sql)
        self.assertIn('join_order,build_side_probe_side', plan.statements[0])


if __name__ == '__main__':
    unittest.main()
