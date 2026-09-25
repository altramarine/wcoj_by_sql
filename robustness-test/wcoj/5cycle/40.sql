CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, e), R5(e, a)
-- R4 [R3, R5]
-- [1] R1(a, b), R2(b, c), R3__R4(c, d, e), R5(e, a)
-- | R3__R4 [R2, R5]
-- | [2] R1(a, b), R2__R3__R4(b, c, d, e), R5(e, a)
-- | | R2__R3__R4 [R5, R1]
-- | | [3] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | [4] R1(a, b), R2(b, c), R3__R4__R5(c, d, e, a)
-- | | R1 [R2, R3__R4__R5]
-- | | [5] R1__R2(a, b, c), R3__R4__R5(c, d, e, a) [acyclic]
-- | | [6] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- [7] R1(a, b), R2(b, c), R3(c, d), R4__R5(d, e, a)
-- | R2 [R3, R1]
-- | [8] R1(a, b), R2__R3(b, c, d), R4__R5(d, e, a)
-- | | R1 [R4__R5, R2__R3]
-- | | [9] R1__R4__R5(a, b, d, e), R2__R3(b, c, d) [acyclic]
-- | | [10] R1__R2__R3(a, b, c, d), R4__R5(d, e, a) [acyclic]
-- | [11] R1__R2(a, b, c), R3(c, d), R4__R5(d, e, a)
-- | | R3 [R4__R5, R1__R2]
-- | | [12] R1__R2(a, b, c), R3__R4__R5(c, d, e, a) [acyclic]
-- | | [13] R1__R2__R3(a, b, c, d), R4__R5(d, e, a) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R3.cnt as cnt FROM R4, cnt_R3 WHERE cnt_R3.d = R4.d),
  cnt_R5 as (SELECT e, COUNT(*) as cnt FROM R5 GROUP BY e),
  best_R5 as (SELECT best_R3.d as d, best_R3.e as e, CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R3, cnt_R5 WHERE cnt_R5.e = best_R3.e)
SELECT d, e, tag FROM best_R5;
CREATE TEMP TABLE node1_R3__R4 AS SELECT R3.c as c, node0_best.d as d, node0_best.e as e FROM R3 JOIN node0_best ON node0_best.d = R3.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node7_R4__R5 AS SELECT node0_best.d as d, node0_best.e as e, R5.a as a FROM R5 JOIN node0_best ON node0_best.e = R5.e WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT node1_R3__R4.c as c, node1_R3__R4.d as d, node1_R3__R4.e as e, 0 as tag, cnt_R2.cnt as cnt FROM node1_R3__R4, cnt_R2 WHERE cnt_R2.c = node1_R3__R4.c),
  cnt_R5 as (SELECT e, COUNT(*) as cnt FROM R5 GROUP BY e),
  best_R5 as (SELECT best_R2.c as c, best_R2.d as d, best_R2.e as e, CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.cnt ELSE cnt_R5.cnt END as cnt FROM best_R2, cnt_R5 WHERE cnt_R5.e = best_R2.e)
SELECT c, d, e, tag FROM best_R5;
CREATE TEMP TABLE node2_R2__R3__R4 AS SELECT R2.b as b, node1_best.c as c, node1_best.d as d, node1_best.e as e FROM R2 JOIN node1_best ON node1_best.c = R2.c WHERE node1_best.tag = 0;
CREATE TEMP TABLE node4_R3__R4__R5 AS SELECT node1_best.c as c, node1_best.d as d, node1_best.e as e, R5.a as a FROM R5 JOIN node1_best ON node1_best.e = R5.e WHERE node1_best.tag = 1;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R3.cnt as cnt FROM R2, cnt_R3 WHERE cnt_R3.c = R2.c),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R3.b as b, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.cnt ELSE cnt_R1.cnt END as cnt FROM best_R3, cnt_R1 WHERE cnt_R1.b = best_R3.b)
SELECT b, c, tag FROM best_R1;
CREATE TEMP TABLE node8_R2__R3 AS SELECT node7_best.b as b, node7_best.c as c, R3.d as d FROM R3 JOIN node7_best ON node7_best.c = R3.c WHERE node7_best.tag = 0;
CREATE TEMP TABLE node11_R1__R2 AS SELECT R1.a as a, node7_best.b as b, node7_best.c as c FROM R1 JOIN node7_best ON node7_best.b = R1.b WHERE node7_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R5 as (SELECT e, COUNT(*) as cnt FROM R5 GROUP BY e),
  best_R5 as (SELECT node2_R2__R3__R4.b as b, node2_R2__R3__R4.c as c, node2_R2__R3__R4.d as d, node2_R2__R3__R4.e as e, 0 as tag, cnt_R5.cnt as cnt FROM node2_R2__R3__R4, cnt_R5 WHERE cnt_R5.e = node2_R2__R3__R4.e),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R5.b as b, best_R5.c as c, best_R5.d as d, best_R5.e as e, CASE WHEN best_R5.cnt < cnt_R1.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R1.cnt THEN best_R5.cnt ELSE cnt_R1.cnt END as cnt FROM best_R5, cnt_R1 WHERE cnt_R1.b = best_R5.b)
SELECT b, c, d, e, tag FROM best_R1;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2.cnt as cnt FROM R1, cnt_R2 WHERE cnt_R2.b = R1.b),
  cnt_R3__R4__R5 as (SELECT a, COUNT(*) as cnt FROM node4_R3__R4__R5 GROUP BY a),
  best_R3__R4__R5 as (SELECT best_R2.a as a, best_R2.b as b, CASE WHEN best_R2.cnt < cnt_R3__R4__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3__R4__R5.cnt THEN best_R2.cnt ELSE cnt_R3__R4__R5.cnt END as cnt FROM best_R2, cnt_R3__R4__R5 WHERE cnt_R3__R4__R5.a = best_R2.a)
SELECT a, b, tag FROM best_R3__R4__R5;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R4__R5 as (SELECT a, COUNT(*) as cnt FROM node7_R4__R5 GROUP BY a),
  best_R4__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R4__R5.cnt as cnt FROM R1, cnt_R4__R5 WHERE cnt_R4__R5.a = R1.a),
  cnt_R2__R3 as (SELECT b, COUNT(*) as cnt FROM node8_R2__R3 GROUP BY b),
  best_R2__R3 as (SELECT best_R4__R5.a as a, best_R4__R5.b as b, CASE WHEN best_R4__R5.cnt < cnt_R2__R3.cnt THEN best_R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R4__R5.cnt < cnt_R2__R3.cnt THEN best_R4__R5.cnt ELSE cnt_R2__R3.cnt END as cnt FROM best_R4__R5, cnt_R2__R3 WHERE cnt_R2__R3.b = best_R4__R5.b)
SELECT a, b, tag FROM best_R2__R3;
CREATE TEMP TABLE node11_best AS WITH
  cnt_R4__R5 as (SELECT d, COUNT(*) as cnt FROM node7_R4__R5 GROUP BY d),
  best_R4__R5 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R4__R5.cnt as cnt FROM R3, cnt_R4__R5 WHERE cnt_R4__R5.d = R3.d),
  cnt_R1__R2 as (SELECT c, COUNT(*) as cnt FROM node11_R1__R2 GROUP BY c),
  best_R1__R2 as (SELECT best_R4__R5.c as c, best_R4__R5.d as d, CASE WHEN best_R4__R5.cnt < cnt_R1__R2.cnt THEN best_R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R4__R5.cnt < cnt_R1__R2.cnt THEN best_R4__R5.cnt ELSE cnt_R1__R2.cnt END as cnt FROM best_R4__R5, cnt_R1__R2 WHERE cnt_R1__R2.c = best_R4__R5.c)
SELECT c, d, tag FROM best_R1__R2;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT R5.a as a, node2_best.b as b, node2_best.c as c, node2_best.d as d, node2_best.e as e FROM R5 JOIN node2_best ON node2_best.e = R5.e SEMI JOIN R1 ON R1.a = R5.a AND R1.b = node2_best.b WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT R1.a as a, node2_best.b as b, node2_best.c as c, node2_best.d as d, node2_best.e as e FROM R1 JOIN node2_best ON node2_best.b = R1.b SEMI JOIN R5 ON R5.e = node2_best.e AND R5.a = R1.a WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node5_R1__R2.a as a, node5_R1__R2.b as b, node5_R1__R2.c as c, node4_R3__R4__R5.d as d, node4_R3__R4__R5.e as e FROM (SELECT node4_best.a as a, node4_best.b as b, R2.c as c FROM R2 JOIN node4_best ON node4_best.b = R2.b WHERE node4_best.tag = 0) node5_R1__R2 JOIN node4_R3__R4__R5 ON node4_R3__R4__R5.c = node5_R1__R2.c AND node4_R3__R4__R5.a = node5_R1__R2.a
UNION ALL
SELECT node6_R1__R2__R3__R4__R5.a as a, node6_R1__R2__R3__R4__R5.b as b, node6_R1__R2__R3__R4__R5.c as c, node6_R1__R2__R3__R4__R5.d as d, node6_R1__R2__R3__R4__R5.e as e FROM (SELECT node4_best.a as a, node4_best.b as b, node4_R3__R4__R5.c as c, node4_R3__R4__R5.d as d, node4_R3__R4__R5.e as e FROM node4_R3__R4__R5 JOIN node4_best ON node4_best.a = node4_R3__R4__R5.a SEMI JOIN R2 ON R2.b = node4_best.b AND R2.c = node4_R3__R4__R5.c WHERE node4_best.tag = 1) node6_R1__R2__R3__R4__R5
UNION ALL
SELECT node9_R1__R4__R5.a as a, node9_R1__R4__R5.b as b, node8_R2__R3.c as c, node9_R1__R4__R5.d as d, node9_R1__R4__R5.e as e FROM (SELECT node8_best.a as a, node8_best.b as b, node7_R4__R5.d as d, node7_R4__R5.e as e FROM node7_R4__R5 JOIN node8_best ON node8_best.a = node7_R4__R5.a WHERE node8_best.tag = 0) node9_R1__R4__R5 JOIN node8_R2__R3 ON node8_R2__R3.b = node9_R1__R4__R5.b AND node8_R2__R3.d = node9_R1__R4__R5.d
UNION ALL
SELECT node10_R1__R2__R3.a as a, node10_R1__R2__R3.b as b, node10_R1__R2__R3.c as c, node10_R1__R2__R3.d as d, node7_R4__R5.e as e FROM (SELECT node8_best.a as a, node8_best.b as b, node8_R2__R3.c as c, node8_R2__R3.d as d FROM node8_R2__R3 JOIN node8_best ON node8_best.b = node8_R2__R3.b WHERE node8_best.tag = 1) node10_R1__R2__R3 JOIN node7_R4__R5 ON node7_R4__R5.d = node10_R1__R2__R3.d AND node7_R4__R5.a = node10_R1__R2__R3.a
UNION ALL
SELECT node12_R3__R4__R5.a as a, node11_R1__R2.b as b, node12_R3__R4__R5.c as c, node12_R3__R4__R5.d as d, node12_R3__R4__R5.e as e FROM (SELECT node11_best.c as c, node11_best.d as d, node7_R4__R5.e as e, node7_R4__R5.a as a FROM node7_R4__R5 JOIN node11_best ON node11_best.d = node7_R4__R5.d WHERE node11_best.tag = 0) node12_R3__R4__R5 JOIN node11_R1__R2 ON node11_R1__R2.a = node12_R3__R4__R5.a AND node11_R1__R2.c = node12_R3__R4__R5.c
UNION ALL
SELECT node13_R1__R2__R3.a as a, node13_R1__R2__R3.b as b, node13_R1__R2__R3.c as c, node13_R1__R2__R3.d as d, node7_R4__R5.e as e FROM (SELECT node11_R1__R2.a as a, node11_R1__R2.b as b, node11_best.c as c, node11_best.d as d FROM node11_R1__R2 JOIN node11_best ON node11_best.c = node11_R1__R2.c WHERE node11_best.tag = 1) node13_R1__R2__R3 JOIN node7_R4__R5 ON node7_R4__R5.d = node13_R1__R2__R3.d AND node7_R4__R5.a = node13_R1__R2__R3.a
);
