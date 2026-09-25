CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, e), R5(e, a)
-- R1 [R5, R2]
-- [1] R1__R5(a, b, e), R2(b, c), R3(c, d), R4(d, e)
-- | R2 [R1__R5, R3]
-- | [2] R1__R2__R5(a, b, e, c), R3(c, d), R4(d, e)
-- | | R1__R2__R5 [R3, R4]
-- | | [3] R1__R2__R3__R4__R5(a, b, e, c, d) [acyclic]
-- | [4] R1__R5(a, b, e), R2__R3(b, c, d), R4(d, e)
-- | | R4 [R1__R5, R2__R3]
-- | | [5] R1__R4__R5(a, b, e, d), R2__R3(b, c, d) [acyclic]
-- | | [6] R1__R5(a, b, e), R2__R3__R4(b, c, d, e) [acyclic]
-- [7] R1__R2(a, b, c), R3(c, d), R4(d, e), R5(e, a)
-- | R3 [R4, R1__R2]
-- | [8] R1__R2(a, b, c), R3__R4(c, d, e), R5(e, a)
-- | | R5 [R1__R2, R3__R4]
-- | | [9] R1__R2__R5(a, b, c, e), R3__R4(c, d, e) [acyclic]
-- | | [10] R1__R2(a, b, c), R3__R4__R5(c, d, e, a) [acyclic]
-- | [11] R1__R2__R3(a, b, c, d), R4(d, e), R5(e, a)
-- | | R5 [R4, R1__R2__R3]
-- | | [12] R1__R2__R3(a, b, c, d), R4__R5(d, e, a) [acyclic]
-- | | [13] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R5.cnt as cnt FROM R1, cnt_R5 WHERE cnt_R5.a = R1.a),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R5.a as a, best_R5.b as b, CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R5, cnt_R2 WHERE cnt_R2.b = best_R5.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node1_R1__R5 AS SELECT node0_best.a as a, node0_best.b as b, R5.e as e FROM R5 JOIN node0_best ON node0_best.a = R5.a WHERE node0_best.tag = 0;
CREATE TEMP TABLE node7_R1__R2 AS SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM R2 JOIN node0_best ON node0_best.b = R2.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R5 as (SELECT b, COUNT(*) as cnt FROM node1_R1__R5 GROUP BY b),
  best_R1__R5 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1__R5.cnt as cnt FROM R2, cnt_R1__R5 WHERE cnt_R1__R5.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1__R5.b as b, best_R1__R5.c as c, CASE WHEN best_R1__R5.cnt < cnt_R3.cnt THEN best_R1__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R5.cnt < cnt_R3.cnt THEN best_R1__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1__R5, cnt_R3 WHERE cnt_R3.c = best_R1__R5.c)
SELECT b, c, tag FROM best_R3;
CREATE TEMP TABLE node2_R1__R2__R5 AS SELECT node1_R1__R5.a as a, node1_best.b as b, node1_R1__R5.e as e, node1_best.c as c FROM node1_R1__R5 JOIN node1_best ON node1_best.b = node1_R1__R5.b WHERE node1_best.tag = 0;
CREATE TEMP TABLE node4_R2__R3 AS SELECT node1_best.b as b, node1_best.c as c, R3.d as d FROM R3 JOIN node1_best ON node1_best.c = R3.c WHERE node1_best.tag = 1;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R4.cnt as cnt FROM R3, cnt_R4 WHERE cnt_R4.d = R3.d),
  cnt_R1__R2 as (SELECT c, COUNT(*) as cnt FROM node7_R1__R2 GROUP BY c),
  best_R1__R2 as (SELECT best_R4.c as c, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R1__R2.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1__R2.cnt THEN best_R4.cnt ELSE cnt_R1__R2.cnt END as cnt FROM best_R4, cnt_R1__R2 WHERE cnt_R1__R2.c = best_R4.c)
SELECT c, d, tag FROM best_R1__R2;
CREATE TEMP TABLE node8_R3__R4 AS SELECT node7_best.c as c, node7_best.d as d, R4.e as e FROM R4 JOIN node7_best ON node7_best.d = R4.d WHERE node7_best.tag = 0;
CREATE TEMP TABLE node11_R1__R2__R3 AS SELECT node7_R1__R2.a as a, node7_R1__R2.b as b, node7_best.c as c, node7_best.d as d FROM node7_R1__R2 JOIN node7_best ON node7_best.c = node7_R1__R2.c WHERE node7_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT node2_R1__R2__R5.a as a, node2_R1__R2__R5.b as b, node2_R1__R2__R5.e as e, node2_R1__R2__R5.c as c, 0 as tag, cnt_R3.cnt as cnt FROM node2_R1__R2__R5, cnt_R3 WHERE cnt_R3.c = node2_R1__R2__R5.c),
  cnt_R4 as (SELECT e, COUNT(*) as cnt FROM R4 GROUP BY e),
  best_R4 as (SELECT best_R3.a as a, best_R3.b as b, best_R3.e as e, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.cnt ELSE cnt_R4.cnt END as cnt FROM best_R3, cnt_R4 WHERE cnt_R4.e = best_R3.e)
SELECT a, b, e, c, tag FROM best_R4;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1__R5 as (SELECT e, COUNT(*) as cnt FROM node1_R1__R5 GROUP BY e),
  best_R1__R5 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R1__R5.cnt as cnt FROM R4, cnt_R1__R5 WHERE cnt_R1__R5.e = R4.e),
  cnt_R2__R3 as (SELECT d, COUNT(*) as cnt FROM node4_R2__R3 GROUP BY d),
  best_R2__R3 as (SELECT best_R1__R5.d as d, best_R1__R5.e as e, CASE WHEN best_R1__R5.cnt < cnt_R2__R3.cnt THEN best_R1__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R5.cnt < cnt_R2__R3.cnt THEN best_R1__R5.cnt ELSE cnt_R2__R3.cnt END as cnt FROM best_R1__R5, cnt_R2__R3 WHERE cnt_R2__R3.d = best_R1__R5.d)
SELECT d, e, tag FROM best_R2__R3;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R1__R2 as (SELECT a, COUNT(*) as cnt FROM node7_R1__R2 GROUP BY a),
  best_R1__R2 as (SELECT R5.e as e, R5.a as a, 0 as tag, cnt_R1__R2.cnt as cnt FROM R5, cnt_R1__R2 WHERE cnt_R1__R2.a = R5.a),
  cnt_R3__R4 as (SELECT e, COUNT(*) as cnt FROM node8_R3__R4 GROUP BY e),
  best_R3__R4 as (SELECT best_R1__R2.e as e, best_R1__R2.a as a, CASE WHEN best_R1__R2.cnt < cnt_R3__R4.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R3__R4.cnt THEN best_R1__R2.cnt ELSE cnt_R3__R4.cnt END as cnt FROM best_R1__R2, cnt_R3__R4 WHERE cnt_R3__R4.e = best_R1__R2.e)
SELECT e, a, tag FROM best_R3__R4;
CREATE TEMP TABLE node11_best AS WITH
  cnt_R4 as (SELECT e, COUNT(*) as cnt FROM R4 GROUP BY e),
  best_R4 as (SELECT R5.e as e, R5.a as a, 0 as tag, cnt_R4.cnt as cnt FROM R5, cnt_R4 WHERE cnt_R4.e = R5.e),
  cnt_R1__R2__R3 as (SELECT a, COUNT(*) as cnt FROM node11_R1__R2__R3 GROUP BY a),
  best_R1__R2__R3 as (SELECT best_R4.e as e, best_R4.a as a, CASE WHEN best_R4.cnt < cnt_R1__R2__R3.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1__R2__R3.cnt THEN best_R4.cnt ELSE cnt_R1__R2__R3.cnt END as cnt FROM best_R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.a = best_R4.a)
SELECT e, a, tag FROM best_R1__R2__R3;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.e as e, node2_best.c as c, R3.d as d FROM R3 JOIN node2_best ON node2_best.c = R3.c SEMI JOIN R4 ON R4.d = R3.d AND R4.e = node2_best.e WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.e as e, node2_best.c as c, R4.d as d FROM R4 JOIN node2_best ON node2_best.e = R4.e SEMI JOIN R3 ON R3.c = node2_best.c AND R3.d = R4.d WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node5_R1__R4__R5.a as a, node5_R1__R4__R5.b as b, node4_R2__R3.c as c, node5_R1__R4__R5.d as d, node5_R1__R4__R5.e as e FROM (SELECT node1_R1__R5.a as a, node1_R1__R5.b as b, node4_best.e as e, node4_best.d as d FROM node1_R1__R5 JOIN node4_best ON node4_best.e = node1_R1__R5.e WHERE node4_best.tag = 0) node5_R1__R4__R5 JOIN node4_R2__R3 ON node4_R2__R3.b = node5_R1__R4__R5.b AND node4_R2__R3.d = node5_R1__R4__R5.d
UNION ALL
SELECT node1_R1__R5.a as a, node6_R2__R3__R4.b as b, node6_R2__R3__R4.c as c, node6_R2__R3__R4.d as d, node6_R2__R3__R4.e as e FROM (SELECT node4_R2__R3.b as b, node4_R2__R3.c as c, node4_best.d as d, node4_best.e as e FROM node4_R2__R3 JOIN node4_best ON node4_best.d = node4_R2__R3.d WHERE node4_best.tag = 1) node6_R2__R3__R4 JOIN node1_R1__R5 ON node1_R1__R5.b = node6_R2__R3__R4.b AND node1_R1__R5.e = node6_R2__R3__R4.e
UNION ALL
SELECT node9_R1__R2__R5.a as a, node9_R1__R2__R5.b as b, node9_R1__R2__R5.c as c, node8_R3__R4.d as d, node9_R1__R2__R5.e as e FROM (SELECT node8_best.a as a, node7_R1__R2.b as b, node7_R1__R2.c as c, node8_best.e as e FROM node7_R1__R2 JOIN node8_best ON node8_best.a = node7_R1__R2.a WHERE node8_best.tag = 0) node9_R1__R2__R5 JOIN node8_R3__R4 ON node8_R3__R4.c = node9_R1__R2__R5.c AND node8_R3__R4.e = node9_R1__R2__R5.e
UNION ALL
SELECT node10_R3__R4__R5.a as a, node7_R1__R2.b as b, node10_R3__R4__R5.c as c, node10_R3__R4__R5.d as d, node10_R3__R4__R5.e as e FROM (SELECT node8_R3__R4.c as c, node8_R3__R4.d as d, node8_best.e as e, node8_best.a as a FROM node8_R3__R4 JOIN node8_best ON node8_best.e = node8_R3__R4.e WHERE node8_best.tag = 1) node10_R3__R4__R5 JOIN node7_R1__R2 ON node7_R1__R2.a = node10_R3__R4__R5.a AND node7_R1__R2.c = node10_R3__R4__R5.c
UNION ALL
SELECT node12_R4__R5.a as a, node11_R1__R2__R3.b as b, node11_R1__R2__R3.c as c, node12_R4__R5.d as d, node12_R4__R5.e as e FROM (SELECT R4.d as d, node11_best.e as e, node11_best.a as a FROM R4 JOIN node11_best ON node11_best.e = R4.e WHERE node11_best.tag = 0) node12_R4__R5 JOIN node11_R1__R2__R3 ON node11_R1__R2__R3.a = node12_R4__R5.a AND node11_R1__R2__R3.d = node12_R4__R5.d
UNION ALL
SELECT node13_R1__R2__R3__R4__R5.a as a, node13_R1__R2__R3__R4__R5.b as b, node13_R1__R2__R3__R4__R5.c as c, node13_R1__R2__R3__R4__R5.d as d, node13_R1__R2__R3__R4__R5.e as e FROM (SELECT node11_best.a as a, node11_R1__R2__R3.b as b, node11_R1__R2__R3.c as c, node11_R1__R2__R3.d as d, node11_best.e as e FROM node11_R1__R2__R3 JOIN node11_best ON node11_best.a = node11_R1__R2__R3.a SEMI JOIN R4 ON R4.d = node11_R1__R2__R3.d AND R4.e = node11_best.e WHERE node11_best.tag = 1) node13_R1__R2__R3__R4__R5
);
