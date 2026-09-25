CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, e), R5(e, a)
-- R2 [R3, R1]
-- [1] R1(a, b), R2__R3(b, c, d), R4(d, e), R5(e, a)
-- | R1 [R2__R3, R5]
-- | [2] R1__R2__R3(a, b, c, d), R4(d, e), R5(e, a)
-- | | R4 [R1__R2__R3, R5]
-- | | [3] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | | [4] R1__R2__R3(a, b, c, d), R4__R5(d, e, a) [acyclic]
-- | [5] R1__R5(a, b, e), R2__R3(b, c, d), R4(d, e)
-- | | R4 [R2__R3, R1__R5]
-- | | [6] R1__R5(a, b, e), R2__R3__R4(b, c, d, e) [acyclic]
-- | | [7] R1__R4__R5(a, b, e, d), R2__R3(b, c, d) [acyclic]
-- [8] R1__R2(a, b, c), R3(c, d), R4(d, e), R5(e, a)
-- | R1__R2 [R5, R3]
-- | [9] R1__R2__R5(a, b, c, e), R3(c, d), R4(d, e)
-- | | R3 [R1__R2__R5, R4]
-- | | [10] R1__R2__R3__R4__R5(a, b, c, e, d) [acyclic]
-- | | [11] R1__R2__R5(a, b, c, e), R3__R4(c, d, e) [acyclic]
-- | [12] R1__R2__R3(a, b, c, d), R4(d, e), R5(e, a)
-- | | R4 [R5, R1__R2__R3]
-- | | [13] R1__R2__R3(a, b, c, d), R4__R5(d, e, a) [acyclic]
-- | | [14] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R3.cnt as cnt FROM R2, cnt_R3 WHERE cnt_R3.c = R2.c),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R3.b as b, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.cnt ELSE cnt_R1.cnt END as cnt FROM best_R3, cnt_R1 WHERE cnt_R1.b = best_R3.b)
SELECT b, c, tag FROM best_R1;
CREATE TEMP TABLE node1_R2__R3 AS SELECT node0_best.b as b, node0_best.c as c, R3.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node8_R1__R2 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2__R3 as (SELECT b, COUNT(*) as cnt FROM node1_R2__R3 GROUP BY b),
  best_R2__R3 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R3.cnt as cnt FROM R1, cnt_R2__R3 WHERE cnt_R2__R3.b = R1.b),
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT best_R2__R3.a as a, best_R2__R3.b as b, CASE WHEN best_R2__R3.cnt < cnt_R5.cnt THEN best_R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R2__R3.cnt < cnt_R5.cnt THEN best_R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R2__R3, cnt_R5 WHERE cnt_R5.a = best_R2__R3.a)
SELECT a, b, tag FROM best_R5;
CREATE TEMP TABLE node2_R1__R2__R3 AS SELECT node1_best.a as a, node1_best.b as b, node1_R2__R3.c as c, node1_R2__R3.d as d FROM node1_R2__R3 JOIN node1_best ON node1_best.b = node1_R2__R3.b WHERE node1_best.tag = 0;
CREATE TEMP TABLE node5_R1__R5 AS SELECT node1_best.a as a, node1_best.b as b, R5.e as e FROM R5 JOIN node1_best ON node1_best.a = R5.a WHERE node1_best.tag = 1;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT node8_R1__R2.a as a, node8_R1__R2.b as b, node8_R1__R2.c as c, 0 as tag, cnt_R5.cnt as cnt FROM node8_R1__R2, cnt_R5 WHERE cnt_R5.a = node8_R1__R2.a),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R5.a as a, best_R5.b as b, best_R5.c as c, CASE WHEN best_R5.cnt < cnt_R3.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R3.cnt THEN best_R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R5, cnt_R3 WHERE cnt_R3.c = best_R5.c)
SELECT a, b, c, tag FROM best_R3;
CREATE TEMP TABLE node9_R1__R2__R5 AS SELECT node8_best.a as a, node8_best.b as b, node8_best.c as c, R5.e as e FROM R5 JOIN node8_best ON node8_best.a = R5.a WHERE node8_best.tag = 0;
CREATE TEMP TABLE node12_R1__R2__R3 AS SELECT node8_best.a as a, node8_best.b as b, node8_best.c as c, R3.d as d FROM R3 JOIN node8_best ON node8_best.c = R3.c WHERE node8_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R1__R2__R3 as (SELECT d, COUNT(*) as cnt FROM node2_R1__R2__R3 GROUP BY d),
  best_R1__R2__R3 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.d = R4.d),
  cnt_R5 as (SELECT e, COUNT(*) as cnt FROM R5 GROUP BY e),
  best_R5 as (SELECT best_R1__R2__R3.d as d, best_R1__R2__R3.e as e, CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R1__R2__R3, cnt_R5 WHERE cnt_R5.e = best_R1__R2__R3.e)
SELECT d, e, tag FROM best_R5;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R2__R3 as (SELECT d, COUNT(*) as cnt FROM node1_R2__R3 GROUP BY d),
  best_R2__R3 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R2__R3.cnt as cnt FROM R4, cnt_R2__R3 WHERE cnt_R2__R3.d = R4.d),
  cnt_R1__R5 as (SELECT e, COUNT(*) as cnt FROM node5_R1__R5 GROUP BY e),
  best_R1__R5 as (SELECT best_R2__R3.d as d, best_R2__R3.e as e, CASE WHEN best_R2__R3.cnt < cnt_R1__R5.cnt THEN best_R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R2__R3.cnt < cnt_R1__R5.cnt THEN best_R2__R3.cnt ELSE cnt_R1__R5.cnt END as cnt FROM best_R2__R3, cnt_R1__R5 WHERE cnt_R1__R5.e = best_R2__R3.e)
SELECT d, e, tag FROM best_R1__R5;
CREATE TEMP TABLE node9_best AS WITH
  cnt_R1__R2__R5 as (SELECT c, COUNT(*) as cnt FROM node9_R1__R2__R5 GROUP BY c),
  best_R1__R2__R5 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R1__R2__R5.cnt as cnt FROM R3, cnt_R1__R2__R5 WHERE cnt_R1__R2__R5.c = R3.c),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R1__R2__R5.c as c, best_R1__R2__R5.d as d, CASE WHEN best_R1__R2__R5.cnt < cnt_R4.cnt THEN best_R1__R2__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R5.cnt < cnt_R4.cnt THEN best_R1__R2__R5.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1__R2__R5, cnt_R4 WHERE cnt_R4.d = best_R1__R2__R5.d)
SELECT c, d, tag FROM best_R4;
CREATE TEMP TABLE node12_best AS WITH
  cnt_R5 as (SELECT e, COUNT(*) as cnt FROM R5 GROUP BY e),
  best_R5 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R5.cnt as cnt FROM R4, cnt_R5 WHERE cnt_R5.e = R4.e),
  cnt_R1__R2__R3 as (SELECT d, COUNT(*) as cnt FROM node12_R1__R2__R3 GROUP BY d),
  best_R1__R2__R3 as (SELECT best_R5.d as d, best_R5.e as e, CASE WHEN best_R5.cnt < cnt_R1__R2__R3.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R1__R2__R3.cnt THEN best_R5.cnt ELSE cnt_R1__R2__R3.cnt END as cnt FROM best_R5, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.d = best_R5.d)
SELECT d, e, tag FROM best_R1__R2__R3;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_R1__R2__R3.a as a, node2_R1__R2__R3.b as b, node2_R1__R2__R3.c as c, node2_best.d as d, node2_best.e as e FROM node2_R1__R2__R3 JOIN node2_best ON node2_best.d = node2_R1__R2__R3.d SEMI JOIN R5 ON R5.e = node2_best.e AND R5.a = node2_R1__R2__R3.a WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node4_R4__R5.a as a, node2_R1__R2__R3.b as b, node2_R1__R2__R3.c as c, node4_R4__R5.d as d, node4_R4__R5.e as e FROM (SELECT node2_best.d as d, node2_best.e as e, R5.a as a FROM R5 JOIN node2_best ON node2_best.e = R5.e WHERE node2_best.tag = 1) node4_R4__R5 JOIN node2_R1__R2__R3 ON node2_R1__R2__R3.a = node4_R4__R5.a AND node2_R1__R2__R3.d = node4_R4__R5.d
UNION ALL
SELECT node5_R1__R5.a as a, node6_R2__R3__R4.b as b, node6_R2__R3__R4.c as c, node6_R2__R3__R4.d as d, node6_R2__R3__R4.e as e FROM (SELECT node1_R2__R3.b as b, node1_R2__R3.c as c, node5_best.d as d, node5_best.e as e FROM node1_R2__R3 JOIN node5_best ON node5_best.d = node1_R2__R3.d WHERE node5_best.tag = 0) node6_R2__R3__R4 JOIN node5_R1__R5 ON node5_R1__R5.b = node6_R2__R3__R4.b AND node5_R1__R5.e = node6_R2__R3__R4.e
UNION ALL
SELECT node7_R1__R4__R5.a as a, node7_R1__R4__R5.b as b, node1_R2__R3.c as c, node7_R1__R4__R5.d as d, node7_R1__R4__R5.e as e FROM (SELECT node5_R1__R5.a as a, node5_R1__R5.b as b, node5_best.e as e, node5_best.d as d FROM node5_R1__R5 JOIN node5_best ON node5_best.e = node5_R1__R5.e WHERE node5_best.tag = 1) node7_R1__R4__R5 JOIN node1_R2__R3 ON node1_R2__R3.b = node7_R1__R4__R5.b AND node1_R2__R3.d = node7_R1__R4__R5.d
UNION ALL
SELECT node10_R1__R2__R3__R4__R5.a as a, node10_R1__R2__R3__R4__R5.b as b, node10_R1__R2__R3__R4__R5.c as c, node10_R1__R2__R3__R4__R5.d as d, node10_R1__R2__R3__R4__R5.e as e FROM (SELECT node9_R1__R2__R5.a as a, node9_R1__R2__R5.b as b, node9_best.c as c, node9_R1__R2__R5.e as e, node9_best.d as d FROM node9_R1__R2__R5 JOIN node9_best ON node9_best.c = node9_R1__R2__R5.c SEMI JOIN R4 ON R4.d = node9_best.d AND R4.e = node9_R1__R2__R5.e WHERE node9_best.tag = 0) node10_R1__R2__R3__R4__R5
UNION ALL
SELECT node9_R1__R2__R5.a as a, node9_R1__R2__R5.b as b, node11_R3__R4.c as c, node11_R3__R4.d as d, node11_R3__R4.e as e FROM (SELECT node9_best.c as c, node9_best.d as d, R4.e as e FROM R4 JOIN node9_best ON node9_best.d = R4.d WHERE node9_best.tag = 1) node11_R3__R4 JOIN node9_R1__R2__R5 ON node9_R1__R2__R5.c = node11_R3__R4.c AND node9_R1__R2__R5.e = node11_R3__R4.e
UNION ALL
SELECT node13_R4__R5.a as a, node12_R1__R2__R3.b as b, node12_R1__R2__R3.c as c, node13_R4__R5.d as d, node13_R4__R5.e as e FROM (SELECT node12_best.d as d, node12_best.e as e, R5.a as a FROM R5 JOIN node12_best ON node12_best.e = R5.e WHERE node12_best.tag = 0) node13_R4__R5 JOIN node12_R1__R2__R3 ON node12_R1__R2__R3.a = node13_R4__R5.a AND node12_R1__R2__R3.d = node13_R4__R5.d
UNION ALL
SELECT node14_R1__R2__R3__R4__R5.a as a, node14_R1__R2__R3__R4__R5.b as b, node14_R1__R2__R3__R4__R5.c as c, node14_R1__R2__R3__R4__R5.d as d, node14_R1__R2__R3__R4__R5.e as e FROM (SELECT node12_R1__R2__R3.a as a, node12_R1__R2__R3.b as b, node12_R1__R2__R3.c as c, node12_best.d as d, node12_best.e as e FROM node12_R1__R2__R3 JOIN node12_best ON node12_best.d = node12_R1__R2__R3.d SEMI JOIN R5 ON R5.e = node12_best.e AND R5.a = node12_R1__R2__R3.a WHERE node12_best.tag = 1) node14_R1__R2__R3__R4__R5
);
