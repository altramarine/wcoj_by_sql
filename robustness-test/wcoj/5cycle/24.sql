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
-- | R2 [R1, R3__R4]
-- | [2] R1__R2(a, b, c), R3__R4(c, d, e), R5(e, a)
-- | | R5 [R1__R2, R3__R4]
-- | | [3] R1__R2__R5(a, b, c, e), R3__R4(c, d, e) [acyclic]
-- | | [4] R1__R2(a, b, c), R3__R4__R5(c, d, e, a) [acyclic]
-- | [5] R1(a, b), R2__R3__R4(b, c, d, e), R5(e, a)
-- | | R5 [R2__R3__R4, R1]
-- | | [6] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | | [7] R1__R5(a, b, e), R2__R3__R4(b, c, d, e) [acyclic]
-- [8] R1(a, b), R2(b, c), R3(c, d), R4__R5(d, e, a)
-- | R4__R5 [R3, R1]
-- | [9] R1(a, b), R2(b, c), R3__R4__R5(c, d, e, a)
-- | | R1 [R2, R3__R4__R5]
-- | | [10] R1__R2(a, b, c), R3__R4__R5(c, d, e, a) [acyclic]
-- | | [11] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | [12] R1__R4__R5(a, b, d, e), R2(b, c), R3(c, d)
-- | | R3 [R2, R1__R4__R5]
-- | | [13] R1__R4__R5(a, b, d, e), R2__R3(b, c, d) [acyclic]
-- | | [14] R1__R2__R3__R4__R5(a, b, d, e, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R3.cnt as cnt FROM R4, cnt_R3 WHERE cnt_R3.d = R4.d),
  cnt_R5 as (SELECT e, COUNT(*) as cnt FROM R5 GROUP BY e),
  best_R5 as (SELECT best_R3.d as d, best_R3.e as e, CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R3, cnt_R5 WHERE cnt_R5.e = best_R3.e)
SELECT d, e, tag FROM best_R5;
CREATE TEMP TABLE node1_R3__R4 AS SELECT R3.c as c, node0_best.d as d, node0_best.e as e FROM R3 JOIN node0_best ON node0_best.d = R3.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node8_R4__R5 AS SELECT node0_best.d as d, node0_best.e as e, R5.a as a FROM R5 JOIN node0_best ON node0_best.e = R5.e WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R3__R4 as (SELECT c, COUNT(*) as cnt FROM node1_R3__R4 GROUP BY c),
  best_R3__R4 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R3__R4.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3__R4.cnt THEN best_R1.cnt ELSE cnt_R3__R4.cnt END as cnt FROM best_R1, cnt_R3__R4 WHERE cnt_R3__R4.c = best_R1.c)
SELECT b, c, tag FROM best_R3__R4;
CREATE TEMP TABLE node2_R1__R2 AS SELECT R1.a as a, node1_best.b as b, node1_best.c as c FROM R1 JOIN node1_best ON node1_best.b = R1.b WHERE node1_best.tag = 0;
CREATE TEMP TABLE node5_R2__R3__R4 AS SELECT node1_best.b as b, node1_best.c as c, node1_R3__R4.d as d, node1_R3__R4.e as e FROM node1_R3__R4 JOIN node1_best ON node1_best.c = node1_R3__R4.c WHERE node1_best.tag = 1;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT node8_R4__R5.d as d, node8_R4__R5.e as e, node8_R4__R5.a as a, 0 as tag, cnt_R3.cnt as cnt FROM node8_R4__R5, cnt_R3 WHERE cnt_R3.d = node8_R4__R5.d),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R3.d as d, best_R3.e as e, best_R3.a as a, CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.cnt ELSE cnt_R1.cnt END as cnt FROM best_R3, cnt_R1 WHERE cnt_R1.a = best_R3.a)
SELECT d, e, a, tag FROM best_R1;
CREATE TEMP TABLE node9_R3__R4__R5 AS SELECT R3.c as c, node8_best.d as d, node8_best.e as e, node8_best.a as a FROM R3 JOIN node8_best ON node8_best.d = R3.d WHERE node8_best.tag = 0;
CREATE TEMP TABLE node12_R1__R4__R5 AS SELECT node8_best.a as a, R1.b as b, node8_best.d as d, node8_best.e as e FROM R1 JOIN node8_best ON node8_best.a = R1.a WHERE node8_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R1__R2 as (SELECT a, COUNT(*) as cnt FROM node2_R1__R2 GROUP BY a),
  best_R1__R2 as (SELECT R5.e as e, R5.a as a, 0 as tag, cnt_R1__R2.cnt as cnt FROM R5, cnt_R1__R2 WHERE cnt_R1__R2.a = R5.a),
  cnt_R3__R4 as (SELECT e, COUNT(*) as cnt FROM node1_R3__R4 GROUP BY e),
  best_R3__R4 as (SELECT best_R1__R2.e as e, best_R1__R2.a as a, CASE WHEN best_R1__R2.cnt < cnt_R3__R4.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R3__R4.cnt THEN best_R1__R2.cnt ELSE cnt_R3__R4.cnt END as cnt FROM best_R1__R2, cnt_R3__R4 WHERE cnt_R3__R4.e = best_R1__R2.e)
SELECT e, a, tag FROM best_R3__R4;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R2__R3__R4 as (SELECT e, COUNT(*) as cnt FROM node5_R2__R3__R4 GROUP BY e),
  best_R2__R3__R4 as (SELECT R5.e as e, R5.a as a, 0 as tag, cnt_R2__R3__R4.cnt as cnt FROM R5, cnt_R2__R3__R4 WHERE cnt_R2__R3__R4.e = R5.e),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R2__R3__R4.e as e, best_R2__R3__R4.a as a, CASE WHEN best_R2__R3__R4.cnt < cnt_R1.cnt THEN best_R2__R3__R4.tag ELSE 1 END as tag,CASE WHEN best_R2__R3__R4.cnt < cnt_R1.cnt THEN best_R2__R3__R4.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2__R3__R4, cnt_R1 WHERE cnt_R1.a = best_R2__R3__R4.a)
SELECT e, a, tag FROM best_R1;
CREATE TEMP TABLE node9_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2.cnt as cnt FROM R1, cnt_R2 WHERE cnt_R2.b = R1.b),
  cnt_R3__R4__R5 as (SELECT a, COUNT(*) as cnt FROM node9_R3__R4__R5 GROUP BY a),
  best_R3__R4__R5 as (SELECT best_R2.a as a, best_R2.b as b, CASE WHEN best_R2.cnt < cnt_R3__R4__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3__R4__R5.cnt THEN best_R2.cnt ELSE cnt_R3__R4__R5.cnt END as cnt FROM best_R2, cnt_R3__R4__R5 WHERE cnt_R3__R4__R5.a = best_R2.a)
SELECT a, b, tag FROM best_R3__R4__R5;
CREATE TEMP TABLE node12_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R3, cnt_R2 WHERE cnt_R2.c = R3.c),
  cnt_R1__R4__R5 as (SELECT d, COUNT(*) as cnt FROM node12_R1__R4__R5 GROUP BY d),
  best_R1__R4__R5 as (SELECT best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R1__R4__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1__R4__R5.cnt THEN best_R2.cnt ELSE cnt_R1__R4__R5.cnt END as cnt FROM best_R2, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.d = best_R2.d)
SELECT c, d, tag FROM best_R1__R4__R5;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R5.a as a, node3_R1__R2__R5.b as b, node3_R1__R2__R5.c as c, node1_R3__R4.d as d, node3_R1__R2__R5.e as e FROM (SELECT node2_best.a as a, node2_R1__R2.b as b, node2_R1__R2.c as c, node2_best.e as e FROM node2_R1__R2 JOIN node2_best ON node2_best.a = node2_R1__R2.a WHERE node2_best.tag = 0) node3_R1__R2__R5 JOIN node1_R3__R4 ON node1_R3__R4.c = node3_R1__R2__R5.c AND node1_R3__R4.e = node3_R1__R2__R5.e
UNION ALL
SELECT node4_R3__R4__R5.a as a, node2_R1__R2.b as b, node4_R3__R4__R5.c as c, node4_R3__R4__R5.d as d, node4_R3__R4__R5.e as e FROM (SELECT node1_R3__R4.c as c, node1_R3__R4.d as d, node2_best.e as e, node2_best.a as a FROM node1_R3__R4 JOIN node2_best ON node2_best.e = node1_R3__R4.e WHERE node2_best.tag = 1) node4_R3__R4__R5 JOIN node2_R1__R2 ON node2_R1__R2.a = node4_R3__R4__R5.a AND node2_R1__R2.c = node4_R3__R4__R5.c
UNION ALL
SELECT node6_R1__R2__R3__R4__R5.a as a, node6_R1__R2__R3__R4__R5.b as b, node6_R1__R2__R3__R4__R5.c as c, node6_R1__R2__R3__R4__R5.d as d, node6_R1__R2__R3__R4__R5.e as e FROM (SELECT node5_best.a as a, node5_R2__R3__R4.b as b, node5_R2__R3__R4.c as c, node5_R2__R3__R4.d as d, node5_best.e as e FROM node5_R2__R3__R4 JOIN node5_best ON node5_best.e = node5_R2__R3__R4.e SEMI JOIN R1 ON R1.a = node5_best.a AND R1.b = node5_R2__R3__R4.b WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5
UNION ALL
SELECT node7_R1__R5.a as a, node7_R1__R5.b as b, node5_R2__R3__R4.c as c, node5_R2__R3__R4.d as d, node7_R1__R5.e as e FROM (SELECT node5_best.a as a, R1.b as b, node5_best.e as e FROM R1 JOIN node5_best ON node5_best.a = R1.a WHERE node5_best.tag = 1) node7_R1__R5 JOIN node5_R2__R3__R4 ON node5_R2__R3__R4.b = node7_R1__R5.b AND node5_R2__R3__R4.e = node7_R1__R5.e
UNION ALL
SELECT node10_R1__R2.a as a, node10_R1__R2.b as b, node10_R1__R2.c as c, node9_R3__R4__R5.d as d, node9_R3__R4__R5.e as e FROM (SELECT node9_best.a as a, node9_best.b as b, R2.c as c FROM R2 JOIN node9_best ON node9_best.b = R2.b WHERE node9_best.tag = 0) node10_R1__R2 JOIN node9_R3__R4__R5 ON node9_R3__R4__R5.c = node10_R1__R2.c AND node9_R3__R4__R5.a = node10_R1__R2.a
UNION ALL
SELECT node11_R1__R2__R3__R4__R5.a as a, node11_R1__R2__R3__R4__R5.b as b, node11_R1__R2__R3__R4__R5.c as c, node11_R1__R2__R3__R4__R5.d as d, node11_R1__R2__R3__R4__R5.e as e FROM (SELECT node9_best.a as a, node9_best.b as b, node9_R3__R4__R5.c as c, node9_R3__R4__R5.d as d, node9_R3__R4__R5.e as e FROM node9_R3__R4__R5 JOIN node9_best ON node9_best.a = node9_R3__R4__R5.a SEMI JOIN R2 ON R2.b = node9_best.b AND R2.c = node9_R3__R4__R5.c WHERE node9_best.tag = 1) node11_R1__R2__R3__R4__R5
UNION ALL
SELECT node12_R1__R4__R5.a as a, node13_R2__R3.b as b, node13_R2__R3.c as c, node13_R2__R3.d as d, node12_R1__R4__R5.e as e FROM (SELECT R2.b as b, node12_best.c as c, node12_best.d as d FROM R2 JOIN node12_best ON node12_best.c = R2.c WHERE node12_best.tag = 0) node13_R2__R3 JOIN node12_R1__R4__R5 ON node12_R1__R4__R5.b = node13_R2__R3.b AND node12_R1__R4__R5.d = node13_R2__R3.d
UNION ALL
SELECT node14_R1__R2__R3__R4__R5.a as a, node14_R1__R2__R3__R4__R5.b as b, node14_R1__R2__R3__R4__R5.c as c, node14_R1__R2__R3__R4__R5.d as d, node14_R1__R2__R3__R4__R5.e as e FROM (SELECT node12_R1__R4__R5.a as a, node12_R1__R4__R5.b as b, node12_best.d as d, node12_R1__R4__R5.e as e, node12_best.c as c FROM node12_R1__R4__R5 JOIN node12_best ON node12_best.d = node12_R1__R4__R5.d SEMI JOIN R2 ON R2.b = node12_R1__R4__R5.b AND R2.c = node12_best.c WHERE node12_best.tag = 1) node14_R1__R2__R3__R4__R5
);
