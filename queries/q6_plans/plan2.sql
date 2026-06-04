CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R3 [R1, R2, R4, R6]
-- [1] R1__R2__R3(a, b, c), R4(a, d), R5(b, d), R6(c, d)
-- | R5 [R4, R6, R1__R2__R3]
-- | [2] R1__R2__R3(a, b, c), R4__R5(a, d, b), R6(c, d)
-- | | R6 [R4__R5, R1__R2__R3]
-- | | [3] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [4] R1__R2__R3(a, b, c), R4(a, d), R5__R6(b, d, c)
-- | | R4 [R5__R6, R1__R2__R3]
-- | | [5] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [6] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- [7] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R5 [R1, R2, R3__R4__R6]
-- | [8] R1__R5(a, b, d), R2(b, c), R3__R4__R6(a, c, d)
-- | | R2 [R1__R5, R3__R4__R6]
-- | | [9] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [10] R1(a, b), R2__R5(b, c, d), R3__R4__R6(a, c, d)
-- | | R1 [R2__R5, R3__R4__R6]
-- | | [11] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [12] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R3, cnt_R1 WHERE cnt_R1.a = R3.a),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R1.a as a, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1, cnt_R2 WHERE cnt_R2.c = best_R1.c),
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT best_R2.a as a, best_R2.c as c, CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.tag ELSE 2 END as tag,CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R2, cnt_R4 WHERE cnt_R4.a = best_R2.a),
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT best_R4.a as a, best_R4.c as c, CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.tag ELSE 3 END as tag,CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.cnt ELSE cnt_R6.cnt END as cnt FROM best_R4, cnt_R6 WHERE cnt_R6.c = best_R4.c),
SELECT a, c, tag FROM best_R6;
CREATE TEMP TABLE node1_R1__R2__R3 AS SELECT node0_best.a as a, R1.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.a = R1.a SEMI JOIN R2 ON R2.b = R1.b AND R2.c = node0_best.c WHERE node0_best.tag = 0
UNION ALL
SELECT node0_best.a as a, R2.b as b, node0_best.c as c FROM R2 JOIN node0_best ON node0_best.c = R2.c SEMI JOIN R1 ON R1.a = node0_best.a AND R1.b = R2.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node7_R3__R4__R6 AS SELECT node0_best.a as a, node0_best.c as c, R4.d as d FROM R4 JOIN node0_best ON node0_best.a = R4.a SEMI JOIN R6 ON R6.c = node0_best.c AND R6.d = R4.d WHERE node0_best.tag = 2
UNION ALL
SELECT node0_best.a as a, node0_best.c as c, R6.d as d FROM R6 JOIN node0_best ON node0_best.c = R6.c SEMI JOIN R4 ON R4.a = node0_best.a AND R4.d = R6.d WHERE node0_best.tag = 3;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R4.cnt as cnt FROM R5, cnt_R4 WHERE cnt_R4.d = R5.d),
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT best_R4.b as b, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.cnt ELSE cnt_R6.cnt END as cnt FROM best_R4, cnt_R6 WHERE cnt_R6.d = best_R4.d),
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM node1_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT best_R6.b as b, best_R6.d as d, CASE WHEN best_R6.cnt < cnt_R1__R2__R3.cnt THEN best_R6.tag ELSE 2 END as tag,CASE WHEN best_R6.cnt < cnt_R1__R2__R3.cnt THEN best_R6.cnt ELSE cnt_R1__R2__R3.cnt END as cnt FROM best_R6, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = best_R6.b),
SELECT b, d, tag FROM best_R1__R2__R3;
CREATE TEMP TABLE node2_R4__R5 AS SELECT R4.a as a, node1_best.d as d, node1_best.b as b FROM R4 JOIN node1_best ON node1_best.d = R4.d WHERE node1_best.tag = 0;
CREATE TEMP TABLE node4_R5__R6 AS SELECT node1_best.b as b, node1_best.d as d, R6.c as c FROM R6 JOIN node1_best ON node1_best.d = R6.d WHERE node1_best.tag = 1;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R1.cnt as cnt FROM R5, cnt_R1 WHERE cnt_R1.b = R5.b),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R1.b as b, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1, cnt_R2 WHERE cnt_R2.b = best_R1.b),
  cnt_R3__R4__R6 as (SELECT d, COUNT(*) as cnt FROM node7_R3__R4__R6 GROUP BY d),
  best_R3__R4__R6 as (SELECT best_R2.b as b, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R3__R4__R6.cnt THEN best_R2.tag ELSE 2 END as tag,CASE WHEN best_R2.cnt < cnt_R3__R4__R6.cnt THEN best_R2.cnt ELSE cnt_R3__R4__R6.cnt END as cnt FROM best_R2, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.d = best_R2.d),
SELECT b, d, tag FROM best_R3__R4__R6;
CREATE TEMP TABLE node8_R1__R5 AS SELECT R1.a as a, node7_best.b as b, node7_best.d as d FROM R1 JOIN node7_best ON node7_best.b = R1.b WHERE node7_best.tag = 0;
CREATE TEMP TABLE node10_R2__R5 AS SELECT node7_best.b as b, R2.c as c, node7_best.d as d FROM R2 JOIN node7_best ON node7_best.b = R2.b WHERE node7_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R4__R5 as (SELECT d, COUNT(*) as cnt FROM node2_R4__R5 GROUP BY d),
  best_R4__R5 as (SELECT R6.c as c, R6.d as d, 0 as tag, cnt_R4__R5.cnt as cnt FROM R6, cnt_R4__R5 WHERE cnt_R4__R5.d = R6.d),
  cnt_R1__R2__R3 as (SELECT c, COUNT(*) as cnt FROM node1_R1__R2__R3 GROUP BY c),
  best_R1__R2__R3 as (SELECT best_R4__R5.c as c, best_R4__R5.d as d, CASE WHEN best_R4__R5.cnt < cnt_R1__R2__R3.cnt THEN best_R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R4__R5.cnt < cnt_R1__R2__R3.cnt THEN best_R4__R5.cnt ELSE cnt_R1__R2__R3.cnt END as cnt FROM best_R4__R5, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.c = best_R4__R5.c),
SELECT c, d, tag FROM best_R1__R2__R3;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R5__R6 as (SELECT d, COUNT(*) as cnt FROM node4_R5__R6 GROUP BY d),
  best_R5__R6 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R5__R6.cnt as cnt FROM R4, cnt_R5__R6 WHERE cnt_R5__R6.d = R4.d),
  cnt_R1__R2__R3 as (SELECT a, COUNT(*) as cnt FROM node1_R1__R2__R3 GROUP BY a),
  best_R1__R2__R3 as (SELECT best_R5__R6.a as a, best_R5__R6.d as d, CASE WHEN best_R5__R6.cnt < cnt_R1__R2__R3.cnt THEN best_R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R5__R6.cnt < cnt_R1__R2__R3.cnt THEN best_R5__R6.cnt ELSE cnt_R1__R2__R3.cnt END as cnt FROM best_R5__R6, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.a = best_R5__R6.a),
SELECT a, d, tag FROM best_R1__R2__R3;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R1__R5 as (SELECT b, COUNT(*) as cnt FROM node8_R1__R5 GROUP BY b),
  best_R1__R5 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1__R5.cnt as cnt FROM R2, cnt_R1__R5 WHERE cnt_R1__R5.b = R2.b),
  cnt_R3__R4__R6 as (SELECT c, COUNT(*) as cnt FROM node7_R3__R4__R6 GROUP BY c),
  best_R3__R4__R6 as (SELECT best_R1__R5.b as b, best_R1__R5.c as c, CASE WHEN best_R1__R5.cnt < cnt_R3__R4__R6.cnt THEN best_R1__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R5.cnt < cnt_R3__R4__R6.cnt THEN best_R1__R5.cnt ELSE cnt_R3__R4__R6.cnt END as cnt FROM best_R1__R5, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.c = best_R1__R5.c),
SELECT b, c, tag FROM best_R3__R4__R6;
CREATE TEMP TABLE node10_best AS WITH
  cnt_R2__R5 as (SELECT b, COUNT(*) as cnt FROM node10_R2__R5 GROUP BY b),
  best_R2__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R5.cnt as cnt FROM R1, cnt_R2__R5 WHERE cnt_R2__R5.b = R1.b),
  cnt_R3__R4__R6 as (SELECT a, COUNT(*) as cnt FROM node7_R3__R4__R6 GROUP BY a),
  best_R3__R4__R6 as (SELECT best_R2__R5.a as a, best_R2__R5.b as b, CASE WHEN best_R2__R5.cnt < cnt_R3__R4__R6.cnt THEN best_R2__R5.tag ELSE 1 END as tag,CASE WHEN best_R2__R5.cnt < cnt_R3__R4__R6.cnt THEN best_R2__R5.cnt ELSE cnt_R3__R4__R6.cnt END as cnt FROM best_R2__R5, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.a = best_R2__R5.a),
SELECT a, b, tag FROM best_R3__R4__R6;
SELECT COUNT(*) FROM (
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_R1__R2__R3.a as a, node1_best.b as b, node1_R1__R2__R3.c as c, node1_best.d as d FROM node1_R1__R2__R3 JOIN node1_best ON node1_best.b = node1_R1__R2__R3.b SEMI JOIN R4 ON R4.a = node1_R1__R2__R3.a AND R4.d = node1_best.d SEMI JOIN R6 ON R6.c = node1_R1__R2__R3.c AND R6.d = node1_best.d WHERE node1_best.tag = 2) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node12_R1__R2__R3__R4__R5__R6.a as a, node12_R1__R2__R3__R4__R5__R6.b as b, node12_R1__R2__R3__R4__R5__R6.c as c, node12_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node7_R3__R4__R6.a as a, node7_best.b as b, node7_R3__R4__R6.c as c, node7_best.d as d FROM node7_R3__R4__R6 JOIN node7_best ON node7_best.d = node7_R3__R4__R6.d SEMI JOIN R1 ON R1.a = node7_R3__R4__R6.a AND R1.b = node7_best.b SEMI JOIN R2 ON R2.b = node7_best.b AND R2.c = node7_R3__R4__R6.c WHERE node7_best.tag = 2) node12_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_R4__R5.a as a, node2_R4__R5.b as b, node2_best.c as c, node2_best.d as d FROM node2_R4__R5 JOIN node2_best ON node2_best.d = node2_R4__R5.d SEMI JOIN node1_R1__R2__R3 ON node1_R1__R2__R3.a = node2_R4__R5.a AND node1_R1__R2__R3.b = node2_R4__R5.b AND node1_R1__R2__R3.c = node2_best.c WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_R1__R2__R3.a as a, node1_R1__R2__R3.b as b, node2_best.c as c, node2_best.d as d FROM node1_R1__R2__R3 JOIN node2_best ON node2_best.c = node1_R1__R2__R3.c SEMI JOIN node2_R4__R5 ON node2_R4__R5.a = node1_R1__R2__R3.a AND node2_R4__R5.d = node2_best.d AND node2_R4__R5.b = node1_R1__R2__R3.b WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node4_best.a as a, node4_R5__R6.b as b, node4_R5__R6.c as c, node4_best.d as d FROM node4_R5__R6 JOIN node4_best ON node4_best.d = node4_R5__R6.d SEMI JOIN node1_R1__R2__R3 ON node1_R1__R2__R3.a = node4_best.a AND node1_R1__R2__R3.b = node4_R5__R6.b AND node1_R1__R2__R3.c = node4_R5__R6.c WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node4_best.a as a, node1_R1__R2__R3.b as b, node1_R1__R2__R3.c as c, node4_best.d as d FROM node1_R1__R2__R3 JOIN node4_best ON node4_best.a = node1_R1__R2__R3.a SEMI JOIN node4_R5__R6 ON node4_R5__R6.b = node1_R1__R2__R3.b AND node4_R5__R6.d = node4_best.d AND node4_R5__R6.c = node1_R1__R2__R3.c WHERE node4_best.tag = 1) node5_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node9_R1__R2__R3__R4__R5__R6.a as a, node9_R1__R2__R3__R4__R5__R6.b as b, node9_R1__R2__R3__R4__R5__R6.c as c, node9_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node8_R1__R5.a as a, node8_best.b as b, node8_R1__R5.d as d, node8_best.c as c FROM node8_R1__R5 JOIN node8_best ON node8_best.b = node8_R1__R5.b WHERE node8_best.tag = 0) node9_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node9_R1__R2__R3__R4__R5__R6.a as a, node9_R1__R2__R3__R4__R5__R6.b as b, node9_R1__R2__R3__R4__R5__R6.c as c, node9_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node7_R3__R4__R6.a as a, node8_best.b as b, node7_R3__R4__R6.d as d, node8_best.c as c FROM node7_R3__R4__R6 JOIN node8_best ON node8_best.c = node7_R3__R4__R6.c WHERE node8_best.tag = 1) node9_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node11_R1__R2__R3__R4__R5__R6.a as a, node11_R1__R2__R3__R4__R5__R6.b as b, node11_R1__R2__R3__R4__R5__R6.c as c, node11_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node10_best.a as a, node10_best.b as b, node10_R2__R5.c as c, node10_R2__R5.d as d FROM node10_R2__R5 JOIN node10_best ON node10_best.b = node10_R2__R5.b WHERE node10_best.tag = 0) node11_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node11_R1__R2__R3__R4__R5__R6.a as a, node11_R1__R2__R3__R4__R5__R6.b as b, node11_R1__R2__R3__R4__R5__R6.c as c, node11_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node10_best.a as a, node10_best.b as b, node7_R3__R4__R6.c as c, node7_R3__R4__R6.d as d FROM node7_R3__R4__R6 JOIN node10_best ON node10_best.a = node7_R3__R4__R6.a WHERE node10_best.tag = 1) node11_R1__R2__R3__R4__R5__R6
);
