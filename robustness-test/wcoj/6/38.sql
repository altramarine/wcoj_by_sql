CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R6 [R5, R3]
-- [1] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R1 [R2__R5__R6, R3]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [3] R1__R3(a, b, c), R2__R5__R6(b, c, d), R4(a, d)
-- | | R4 [R2__R5__R6, R1__R3]
-- | | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- [5] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R1 [R5, R2]
-- | [6] R1__R5(a, b, d), R2(b, c), R3__R4__R6(a, c, d)
-- | | R3__R4__R6 [R2, R1__R5]
-- | | [7] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [8] R1__R2(a, b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | | R3__R4__R6 [R5, R1__R2]
-- | | [9] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT R6.c as c, R6.d as d, 0 as tag, cnt_R5.cnt as cnt FROM R6, cnt_R5 WHERE cnt_R5.d = R6.d),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R5.c as c, best_R5.d as d, CASE WHEN best_R5.cnt < cnt_R3.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R3.cnt THEN best_R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R5, cnt_R3 WHERE cnt_R3.c = best_R5.c)
SELECT c, d, tag FROM best_R3;
CREATE TEMP TABLE node1_R2__R5__R6 AS SELECT R5.b as b, node0_best.c as c, node0_best.d as d FROM R5 JOIN node0_best ON node0_best.d = R5.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R3__R4__R6 AS SELECT R3.a as a, node0_best.c as c, node0_best.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2__R5__R6 as (SELECT b, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY b),
  best_R2__R5__R6 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R1, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.b as b, CASE WHEN best_R2__R5__R6.cnt < cnt_R3.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R3.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2__R5__R6, cnt_R3 WHERE cnt_R3.a = best_R2__R5__R6.a)
SELECT a, b, tag FROM best_R3;
CREATE TEMP TABLE node3_R1__R3 AS SELECT node1_best.a as a, node1_best.b as b, R3.c as c FROM R3 JOIN node1_best ON node1_best.a = R3.a WHERE node1_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R5.cnt as cnt FROM R1, cnt_R5 WHERE cnt_R5.b = R1.b),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R5.a as a, best_R5.b as b, CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R5, cnt_R2 WHERE cnt_R2.b = best_R5.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node6_R1__R5 AS SELECT node5_best.a as a, node5_best.b as b, R5.d as d FROM R5 JOIN node5_best ON node5_best.b = R5.b WHERE node5_best.tag = 0;
CREATE TEMP TABLE node8_R1__R2 AS SELECT node5_best.a as a, node5_best.b as b, R2.c as c FROM R2 JOIN node5_best ON node5_best.b = R2.b WHERE node5_best.tag = 1;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R2__R5__R6 as (SELECT d, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY d),
  best_R2__R5__R6 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R4, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.d = R4.d),
  cnt_R1__R3 as (SELECT a, COUNT(*) as cnt FROM node3_R1__R3 GROUP BY a),
  best_R1__R3 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.d as d, CASE WHEN best_R2__R5__R6.cnt < cnt_R1__R3.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R1__R3.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R1__R3.cnt END as cnt FROM best_R2__R5__R6, cnt_R1__R3 WHERE cnt_R1__R3.a = best_R2__R5__R6.a)
SELECT a, d, tag FROM best_R1__R3;
CREATE TEMP TABLE node6_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT node5_R3__R4__R6.a as a, node5_R3__R4__R6.c as c, node5_R3__R4__R6.d as d, 0 as tag, cnt_R2.cnt as cnt FROM node5_R3__R4__R6, cnt_R2 WHERE cnt_R2.c = node5_R3__R4__R6.c),
  cnt_R1__R5 as (SELECT a,d, COUNT(*) as cnt FROM node6_R1__R5 GROUP BY a,d),
  best_R1__R5 as (SELECT best_R2.a as a, best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R1__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1__R5.cnt THEN best_R2.cnt ELSE cnt_R1__R5.cnt END as cnt FROM best_R2, cnt_R1__R5 WHERE cnt_R1__R5.a = best_R2.a AND cnt_R1__R5.d = best_R2.d)
SELECT a, c, d, tag FROM best_R1__R5;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT node5_R3__R4__R6.a as a, node5_R3__R4__R6.c as c, node5_R3__R4__R6.d as d, 0 as tag, cnt_R5.cnt as cnt FROM node5_R3__R4__R6, cnt_R5 WHERE cnt_R5.d = node5_R3__R4__R6.d),
  cnt_R1__R2 as (SELECT a,c, COUNT(*) as cnt FROM node8_R1__R2 GROUP BY a,c),
  best_R1__R2 as (SELECT best_R5.a as a, best_R5.c as c, best_R5.d as d, CASE WHEN best_R5.cnt < cnt_R1__R2.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R1__R2.cnt THEN best_R5.cnt ELSE cnt_R1__R2.cnt END as cnt FROM best_R5, cnt_R1__R2 WHERE cnt_R1__R2.a = best_R5.a AND cnt_R1__R2.c = best_R5.c)
SELECT a, c, d, tag FROM best_R1__R2;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_R2__R5__R6.c as c, node1_R2__R5__R6.d as d FROM node1_R2__R5__R6 JOIN node1_best ON node1_best.b = node1_R2__R5__R6.b SEMI JOIN R3 ON R3.a = node1_best.a AND R3.c = node1_R2__R5__R6.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node1_R2__R5__R6.b as b, node1_R2__R5__R6.c as c, node3_best.d as d FROM node1_R2__R5__R6 JOIN node3_best ON node3_best.d = node1_R2__R5__R6.d WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_R1__R3.b as b, node3_R1__R3.c as c, node3_best.d as d FROM node3_R1__R3 JOIN node3_best ON node3_best.a = node3_R1__R3.a WHERE node3_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, R2.b as b, node6_best.d as d, node6_best.c as c FROM R2 JOIN node6_best ON node6_best.c = R2.c WHERE node6_best.tag = 0) node7_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, node6_R1__R5.b as b, node6_best.d as d, node6_best.c as c FROM node6_R1__R5 JOIN node6_best ON node6_best.a = node6_R1__R5.a AND node6_best.d = node6_R1__R5.d SEMI JOIN R2 ON R2.b = node6_R1__R5.b AND R2.c = node6_best.c WHERE node6_best.tag = 1) node7_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node9_R1__R2__R3__R4__R5__R6.a as a, node9_R1__R2__R3__R4__R5__R6.b as b, node9_R1__R2__R3__R4__R5__R6.c as c, node9_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node8_best.a as a, R5.b as b, node8_best.c as c, node8_best.d as d FROM R5 JOIN node8_best ON node8_best.d = R5.d SEMI JOIN node8_R1__R2 ON node8_R1__R2.a = node8_best.a AND node8_R1__R2.b = R5.b AND node8_R1__R2.c = node8_best.c WHERE node8_best.tag = 0) node9_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node9_R1__R2__R3__R4__R5__R6.a as a, node9_R1__R2__R3__R4__R5__R6.b as b, node9_R1__R2__R3__R4__R5__R6.c as c, node9_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node8_best.a as a, node8_R1__R2.b as b, node8_best.c as c, node8_best.d as d FROM node8_R1__R2 JOIN node8_best ON node8_best.a = node8_R1__R2.a AND node8_best.c = node8_R1__R2.c SEMI JOIN R5 ON R5.b = node8_R1__R2.b AND R5.d = node8_best.d WHERE node8_best.tag = 1) node9_R1__R2__R3__R4__R5__R6
);
