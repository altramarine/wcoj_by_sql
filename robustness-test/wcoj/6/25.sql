CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R4 [R1, R6]
-- [1] R1__R4__R5(a, b, d), R2(b, c), R3(a, c), R6(c, d)
-- | R3 [R6, R2]
-- | [2] R1__R4__R5(a, b, d), R2(b, c), R3__R6(a, c, d)
-- | | R1__R4__R5 [R3__R6, R2]
-- | | [3] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [4] R1__R4__R5(a, b, d), R2__R3(b, c, a), R6(c, d)
-- | | R2__R3 [R6, R1__R4__R5]
-- | | [5] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- [6] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R1 [R5, R2]
-- | [7] R1__R5(a, b, d), R2(b, c), R3__R4__R6(a, c, d)
-- | | R3__R4__R6 [R2, R1__R5]
-- | | [8] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [9] R1__R2(a, b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | | R5 [R3__R4__R6, R1__R2]
-- | | [10] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R1.cnt as cnt FROM R4, cnt_R1 WHERE cnt_R1.a = R4.a),
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT best_R1.a as a, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R6.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R6.cnt THEN best_R1.cnt ELSE cnt_R6.cnt END as cnt FROM best_R1, cnt_R6 WHERE cnt_R6.d = best_R1.d)
SELECT a, d, tag FROM best_R6;
CREATE TEMP TABLE node1_R1__R4__R5 AS SELECT node0_best.a as a, R1.b as b, node0_best.d as d FROM R1 JOIN node0_best ON node0_best.a = R1.a WHERE node0_best.tag = 0;
CREATE TEMP TABLE node6_R3__R4__R6 AS SELECT node0_best.a as a, R6.c as c, node0_best.d as d FROM R6 JOIN node0_best ON node0_best.d = R6.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R6.cnt as cnt FROM R3, cnt_R6 WHERE cnt_R6.c = R3.c),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R6.a as a, best_R6.c as c, CASE WHEN best_R6.cnt < cnt_R2.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R2.cnt THEN best_R6.cnt ELSE cnt_R2.cnt END as cnt FROM best_R6, cnt_R2 WHERE cnt_R2.c = best_R6.c)
SELECT a, c, tag FROM best_R2;
CREATE TEMP TABLE node2_R3__R6 AS SELECT node1_best.a as a, node1_best.c as c, R6.d as d FROM R6 JOIN node1_best ON node1_best.c = R6.c WHERE node1_best.tag = 0;
CREATE TEMP TABLE node4_R2__R3 AS SELECT R2.b as b, node1_best.c as c, node1_best.a as a FROM R2 JOIN node1_best ON node1_best.c = R2.c WHERE node1_best.tag = 1;
CREATE TEMP TABLE node6_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R5.cnt as cnt FROM R1, cnt_R5 WHERE cnt_R5.b = R1.b),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R5.a as a, best_R5.b as b, CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R5, cnt_R2 WHERE cnt_R2.b = best_R5.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node7_R1__R5 AS SELECT node6_best.a as a, node6_best.b as b, R5.d as d FROM R5 JOIN node6_best ON node6_best.b = R5.b WHERE node6_best.tag = 0;
CREATE TEMP TABLE node9_R1__R2 AS SELECT node6_best.a as a, node6_best.b as b, R2.c as c FROM R2 JOIN node6_best ON node6_best.b = R2.b WHERE node6_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R3__R6 as (SELECT a,d, COUNT(*) as cnt FROM node2_R3__R6 GROUP BY a,d),
  best_R3__R6 as (SELECT node1_R1__R4__R5.a as a, node1_R1__R4__R5.b as b, node1_R1__R4__R5.d as d, 0 as tag, cnt_R3__R6.cnt as cnt FROM node1_R1__R4__R5, cnt_R3__R6 WHERE cnt_R3__R6.a = node1_R1__R4__R5.a AND cnt_R3__R6.d = node1_R1__R4__R5.d),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R3__R6.a as a, best_R3__R6.b as b, best_R3__R6.d as d, CASE WHEN best_R3__R6.cnt < cnt_R2.cnt THEN best_R3__R6.tag ELSE 1 END as tag,CASE WHEN best_R3__R6.cnt < cnt_R2.cnt THEN best_R3__R6.cnt ELSE cnt_R2.cnt END as cnt FROM best_R3__R6, cnt_R2 WHERE cnt_R2.b = best_R3__R6.b)
SELECT a, b, d, tag FROM best_R2;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT node4_R2__R3.b as b, node4_R2__R3.c as c, node4_R2__R3.a as a, 0 as tag, cnt_R6.cnt as cnt FROM node4_R2__R3, cnt_R6 WHERE cnt_R6.c = node4_R2__R3.c),
  cnt_R1__R4__R5 as (SELECT a,b, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY a,b),
  best_R1__R4__R5 as (SELECT best_R6.b as b, best_R6.c as c, best_R6.a as a, CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.cnt ELSE cnt_R1__R4__R5.cnt END as cnt FROM best_R6, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.a = best_R6.a AND cnt_R1__R4__R5.b = best_R6.b)
SELECT b, c, a, tag FROM best_R1__R4__R5;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT node6_R3__R4__R6.a as a, node6_R3__R4__R6.c as c, node6_R3__R4__R6.d as d, 0 as tag, cnt_R2.cnt as cnt FROM node6_R3__R4__R6, cnt_R2 WHERE cnt_R2.c = node6_R3__R4__R6.c),
  cnt_R1__R5 as (SELECT a,d, COUNT(*) as cnt FROM node7_R1__R5 GROUP BY a,d),
  best_R1__R5 as (SELECT best_R2.a as a, best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R1__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1__R5.cnt THEN best_R2.cnt ELSE cnt_R1__R5.cnt END as cnt FROM best_R2, cnt_R1__R5 WHERE cnt_R1__R5.a = best_R2.a AND cnt_R1__R5.d = best_R2.d)
SELECT a, c, d, tag FROM best_R1__R5;
CREATE TEMP TABLE node9_best AS WITH
  cnt_R3__R4__R6 as (SELECT d, COUNT(*) as cnt FROM node6_R3__R4__R6 GROUP BY d),
  best_R3__R4__R6 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R3__R4__R6.cnt as cnt FROM R5, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.d = R5.d),
  cnt_R1__R2 as (SELECT b, COUNT(*) as cnt FROM node9_R1__R2 GROUP BY b),
  best_R1__R2 as (SELECT best_R3__R4__R6.b as b, best_R3__R4__R6.d as d, CASE WHEN best_R3__R4__R6.cnt < cnt_R1__R2.cnt THEN best_R3__R4__R6.tag ELSE 1 END as tag,CASE WHEN best_R3__R4__R6.cnt < cnt_R1__R2.cnt THEN best_R3__R4__R6.cnt ELSE cnt_R1__R2.cnt END as cnt FROM best_R3__R4__R6, cnt_R1__R2 WHERE cnt_R1__R2.b = best_R3__R4__R6.b)
SELECT b, d, tag FROM best_R1__R2;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.d as d, node2_R3__R6.c as c FROM node2_R3__R6 JOIN node2_best ON node2_best.a = node2_R3__R6.a AND node2_best.d = node2_R3__R6.d SEMI JOIN R2 ON R2.b = node2_best.b AND R2.c = node2_R3__R6.c WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.d as d, R2.c as c FROM R2 JOIN node2_best ON node2_best.b = R2.b WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node4_best.a as a, node4_best.b as b, R6.d as d, node4_best.c as c FROM R6 JOIN node4_best ON node4_best.c = R6.c WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node4_best.a as a, node4_best.b as b, node1_R1__R4__R5.d as d, node4_best.c as c FROM node1_R1__R4__R5 JOIN node4_best ON node4_best.b = node1_R1__R4__R5.b AND node4_best.a = node1_R1__R4__R5.a SEMI JOIN R6 ON R6.c = node4_best.c AND R6.d = node1_R1__R4__R5.d WHERE node4_best.tag = 1) node5_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node7_best.a as a, R2.b as b, node7_best.d as d, node7_best.c as c FROM R2 JOIN node7_best ON node7_best.c = R2.c WHERE node7_best.tag = 0) node8_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node7_best.a as a, node7_R1__R5.b as b, node7_best.d as d, node7_best.c as c FROM node7_R1__R5 JOIN node7_best ON node7_best.a = node7_R1__R5.a AND node7_best.d = node7_R1__R5.d SEMI JOIN R2 ON R2.b = node7_R1__R5.b AND R2.c = node7_best.c WHERE node7_best.tag = 1) node8_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node10_R1__R2__R3__R4__R5__R6.a as a, node10_R1__R2__R3__R4__R5__R6.b as b, node10_R1__R2__R3__R4__R5__R6.c as c, node10_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_R3__R4__R6.a as a, node9_best.b as b, node6_R3__R4__R6.c as c, node9_best.d as d FROM node6_R3__R4__R6 JOIN node9_best ON node9_best.d = node6_R3__R4__R6.d SEMI JOIN node9_R1__R2 ON node9_R1__R2.a = node6_R3__R4__R6.a AND node9_R1__R2.b = node9_best.b AND node9_R1__R2.c = node6_R3__R4__R6.c WHERE node9_best.tag = 0) node10_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node10_R1__R2__R3__R4__R5__R6.a as a, node10_R1__R2__R3__R4__R5__R6.b as b, node10_R1__R2__R3__R4__R5__R6.c as c, node10_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node9_R1__R2.a as a, node9_best.b as b, node9_R1__R2.c as c, node9_best.d as d FROM node9_R1__R2 JOIN node9_best ON node9_best.b = node9_R1__R2.b WHERE node9_best.tag = 1) node10_R1__R2__R3__R4__R5__R6
);
