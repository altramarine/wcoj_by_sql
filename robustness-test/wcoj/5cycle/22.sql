CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, e), R5(e, a)
-- R2 [R1, R3]
-- [1] R1__R2(a, b, c), R3(c, d), R4(d, e), R5(e, a)
-- | R5 [R1__R2, R4]
-- | [2] R1__R2__R5(a, b, c, e), R3(c, d), R4(d, e)
-- | | R4 [R3, R1__R2__R5]
-- | | [3] R1__R2__R5(a, b, c, e), R3__R4(c, d, e) [acyclic]
-- | | [4] R1__R2__R3__R4__R5(a, b, c, e, d) [acyclic]
-- | [5] R1__R2(a, b, c), R3(c, d), R4__R5(d, e, a)
-- | | R1__R2 [R4__R5, R3]
-- | | [6] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | | [7] R1__R2__R3(a, b, c, d), R4__R5(d, e, a) [acyclic]
-- [8] R1(a, b), R2__R3(b, c, d), R4(d, e), R5(e, a)
-- | R4 [R2__R3, R5]
-- | [9] R1(a, b), R2__R3__R4(b, c, d, e), R5(e, a)
-- | | R5 [R1, R2__R3__R4]
-- | | [10] R1__R5(a, b, e), R2__R3__R4(b, c, d, e) [acyclic]
-- | | [11] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | [12] R1(a, b), R2__R3(b, c, d), R4__R5(d, e, a)
-- | | R4__R5 [R2__R3, R1]
-- | | [13] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | | [14] R1__R4__R5(a, b, d, e), R2__R3(b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.c = best_R1.c)
SELECT b, c, tag FROM best_R3;
CREATE TEMP TABLE node1_R1__R2 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node8_R2__R3 AS SELECT node0_best.b as b, node0_best.c as c, R3.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R2 as (SELECT a, COUNT(*) as cnt FROM node1_R1__R2 GROUP BY a),
  best_R1__R2 as (SELECT R5.e as e, R5.a as a, 0 as tag, cnt_R1__R2.cnt as cnt FROM R5, cnt_R1__R2 WHERE cnt_R1__R2.a = R5.a),
  cnt_R4 as (SELECT e, COUNT(*) as cnt FROM R4 GROUP BY e),
  best_R4 as (SELECT best_R1__R2.e as e, best_R1__R2.a as a, CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1__R2, cnt_R4 WHERE cnt_R4.e = best_R1__R2.e)
SELECT e, a, tag FROM best_R4;
CREATE TEMP TABLE node2_R1__R2__R5 AS SELECT node1_best.a as a, node1_R1__R2.b as b, node1_R1__R2.c as c, node1_best.e as e FROM node1_R1__R2 JOIN node1_best ON node1_best.a = node1_R1__R2.a WHERE node1_best.tag = 0;
CREATE TEMP TABLE node5_R4__R5 AS SELECT R4.d as d, node1_best.e as e, node1_best.a as a FROM R4 JOIN node1_best ON node1_best.e = R4.e WHERE node1_best.tag = 1;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R2__R3 as (SELECT d, COUNT(*) as cnt FROM node8_R2__R3 GROUP BY d),
  best_R2__R3 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R2__R3.cnt as cnt FROM R4, cnt_R2__R3 WHERE cnt_R2__R3.d = R4.d),
  cnt_R5 as (SELECT e, COUNT(*) as cnt FROM R5 GROUP BY e),
  best_R5 as (SELECT best_R2__R3.d as d, best_R2__R3.e as e, CASE WHEN best_R2__R3.cnt < cnt_R5.cnt THEN best_R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R2__R3.cnt < cnt_R5.cnt THEN best_R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R2__R3, cnt_R5 WHERE cnt_R5.e = best_R2__R3.e)
SELECT d, e, tag FROM best_R5;
CREATE TEMP TABLE node9_R2__R3__R4 AS SELECT node8_R2__R3.b as b, node8_R2__R3.c as c, node8_best.d as d, node8_best.e as e FROM node8_R2__R3 JOIN node8_best ON node8_best.d = node8_R2__R3.d WHERE node8_best.tag = 0;
CREATE TEMP TABLE node12_R4__R5 AS SELECT node8_best.d as d, node8_best.e as e, R5.a as a FROM R5 JOIN node8_best ON node8_best.e = R5.e WHERE node8_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R3.cnt as cnt FROM R4, cnt_R3 WHERE cnt_R3.d = R4.d),
  cnt_R1__R2__R5 as (SELECT e, COUNT(*) as cnt FROM node2_R1__R2__R5 GROUP BY e),
  best_R1__R2__R5 as (SELECT best_R3.d as d, best_R3.e as e, CASE WHEN best_R3.cnt < cnt_R1__R2__R5.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1__R2__R5.cnt THEN best_R3.cnt ELSE cnt_R1__R2__R5.cnt END as cnt FROM best_R3, cnt_R1__R2__R5 WHERE cnt_R1__R2__R5.e = best_R3.e)
SELECT d, e, tag FROM best_R1__R2__R5;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R4__R5 as (SELECT a, COUNT(*) as cnt FROM node5_R4__R5 GROUP BY a),
  best_R4__R5 as (SELECT node1_R1__R2.a as a, node1_R1__R2.b as b, node1_R1__R2.c as c, 0 as tag, cnt_R4__R5.cnt as cnt FROM node1_R1__R2, cnt_R4__R5 WHERE cnt_R4__R5.a = node1_R1__R2.a),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R4__R5.a as a, best_R4__R5.b as b, best_R4__R5.c as c, CASE WHEN best_R4__R5.cnt < cnt_R3.cnt THEN best_R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R4__R5.cnt < cnt_R3.cnt THEN best_R4__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R4__R5, cnt_R3 WHERE cnt_R3.c = best_R4__R5.c)
SELECT a, b, c, tag FROM best_R3;
CREATE TEMP TABLE node9_best AS WITH
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT R5.e as e, R5.a as a, 0 as tag, cnt_R1.cnt as cnt FROM R5, cnt_R1 WHERE cnt_R1.a = R5.a),
  cnt_R2__R3__R4 as (SELECT e, COUNT(*) as cnt FROM node9_R2__R3__R4 GROUP BY e),
  best_R2__R3__R4 as (SELECT best_R1.e as e, best_R1.a as a, CASE WHEN best_R1.cnt < cnt_R2__R3__R4.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2__R3__R4.cnt THEN best_R1.cnt ELSE cnt_R2__R3__R4.cnt END as cnt FROM best_R1, cnt_R2__R3__R4 WHERE cnt_R2__R3__R4.e = best_R1.e)
SELECT e, a, tag FROM best_R2__R3__R4;
CREATE TEMP TABLE node12_best AS WITH
  cnt_R2__R3 as (SELECT d, COUNT(*) as cnt FROM node8_R2__R3 GROUP BY d),
  best_R2__R3 as (SELECT node12_R4__R5.d as d, node12_R4__R5.e as e, node12_R4__R5.a as a, 0 as tag, cnt_R2__R3.cnt as cnt FROM node12_R4__R5, cnt_R2__R3 WHERE cnt_R2__R3.d = node12_R4__R5.d),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R2__R3.d as d, best_R2__R3.e as e, best_R2__R3.a as a, CASE WHEN best_R2__R3.cnt < cnt_R1.cnt THEN best_R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R2__R3.cnt < cnt_R1.cnt THEN best_R2__R3.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2__R3, cnt_R1 WHERE cnt_R1.a = best_R2__R3.a)
SELECT d, e, a, tag FROM best_R1;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R5.a as a, node2_R1__R2__R5.b as b, node3_R3__R4.c as c, node3_R3__R4.d as d, node3_R3__R4.e as e FROM (SELECT R3.c as c, node2_best.d as d, node2_best.e as e FROM R3 JOIN node2_best ON node2_best.d = R3.d WHERE node2_best.tag = 0) node3_R3__R4 JOIN node2_R1__R2__R5 ON node2_R1__R2__R5.c = node3_R3__R4.c AND node2_R1__R2__R5.e = node3_R3__R4.e
UNION ALL
SELECT node4_R1__R2__R3__R4__R5.a as a, node4_R1__R2__R3__R4__R5.b as b, node4_R1__R2__R3__R4__R5.c as c, node4_R1__R2__R3__R4__R5.d as d, node4_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_R1__R2__R5.a as a, node2_R1__R2__R5.b as b, node2_R1__R2__R5.c as c, node2_best.e as e, node2_best.d as d FROM node2_R1__R2__R5 JOIN node2_best ON node2_best.e = node2_R1__R2__R5.e SEMI JOIN R3 ON R3.c = node2_R1__R2__R5.c AND R3.d = node2_best.d WHERE node2_best.tag = 1) node4_R1__R2__R3__R4__R5
UNION ALL
SELECT node6_R1__R2__R3__R4__R5.a as a, node6_R1__R2__R3__R4__R5.b as b, node6_R1__R2__R3__R4__R5.c as c, node6_R1__R2__R3__R4__R5.d as d, node6_R1__R2__R3__R4__R5.e as e FROM (SELECT node5_best.a as a, node5_best.b as b, node5_best.c as c, node5_R4__R5.d as d, node5_R4__R5.e as e FROM node5_R4__R5 JOIN node5_best ON node5_best.a = node5_R4__R5.a SEMI JOIN R3 ON R3.c = node5_best.c AND R3.d = node5_R4__R5.d WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5
UNION ALL
SELECT node7_R1__R2__R3.a as a, node7_R1__R2__R3.b as b, node7_R1__R2__R3.c as c, node7_R1__R2__R3.d as d, node5_R4__R5.e as e FROM (SELECT node5_best.a as a, node5_best.b as b, node5_best.c as c, R3.d as d FROM R3 JOIN node5_best ON node5_best.c = R3.c WHERE node5_best.tag = 1) node7_R1__R2__R3 JOIN node5_R4__R5 ON node5_R4__R5.d = node7_R1__R2__R3.d AND node5_R4__R5.a = node7_R1__R2__R3.a
UNION ALL
SELECT node10_R1__R5.a as a, node10_R1__R5.b as b, node9_R2__R3__R4.c as c, node9_R2__R3__R4.d as d, node10_R1__R5.e as e FROM (SELECT node9_best.a as a, R1.b as b, node9_best.e as e FROM R1 JOIN node9_best ON node9_best.a = R1.a WHERE node9_best.tag = 0) node10_R1__R5 JOIN node9_R2__R3__R4 ON node9_R2__R3__R4.b = node10_R1__R5.b AND node9_R2__R3__R4.e = node10_R1__R5.e
UNION ALL
SELECT node11_R1__R2__R3__R4__R5.a as a, node11_R1__R2__R3__R4__R5.b as b, node11_R1__R2__R3__R4__R5.c as c, node11_R1__R2__R3__R4__R5.d as d, node11_R1__R2__R3__R4__R5.e as e FROM (SELECT node9_best.a as a, node9_R2__R3__R4.b as b, node9_R2__R3__R4.c as c, node9_R2__R3__R4.d as d, node9_best.e as e FROM node9_R2__R3__R4 JOIN node9_best ON node9_best.e = node9_R2__R3__R4.e SEMI JOIN R1 ON R1.a = node9_best.a AND R1.b = node9_R2__R3__R4.b WHERE node9_best.tag = 1) node11_R1__R2__R3__R4__R5
UNION ALL
SELECT node13_R1__R2__R3__R4__R5.a as a, node13_R1__R2__R3__R4__R5.b as b, node13_R1__R2__R3__R4__R5.c as c, node13_R1__R2__R3__R4__R5.d as d, node13_R1__R2__R3__R4__R5.e as e FROM (SELECT node12_best.a as a, node8_R2__R3.b as b, node8_R2__R3.c as c, node12_best.d as d, node12_best.e as e FROM node8_R2__R3 JOIN node12_best ON node12_best.d = node8_R2__R3.d SEMI JOIN R1 ON R1.a = node12_best.a AND R1.b = node8_R2__R3.b WHERE node12_best.tag = 0) node13_R1__R2__R3__R4__R5
UNION ALL
SELECT node14_R1__R4__R5.a as a, node14_R1__R4__R5.b as b, node8_R2__R3.c as c, node14_R1__R4__R5.d as d, node14_R1__R4__R5.e as e FROM (SELECT node12_best.a as a, R1.b as b, node12_best.d as d, node12_best.e as e FROM R1 JOIN node12_best ON node12_best.a = R1.a WHERE node12_best.tag = 1) node14_R1__R4__R5 JOIN node8_R2__R3 ON node8_R2__R3.b = node14_R1__R4__R5.b AND node8_R2__R3.d = node14_R1__R4__R5.d
);
