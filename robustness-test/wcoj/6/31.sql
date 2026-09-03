CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R1 [R5, R2]
-- [1] R1__R4__R5(a, b, d), R2(b, c), R3(a, c), R6(c, d)
-- | R6 [R1__R4__R5, R3]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [3] R1__R4__R5(a, b, d), R2(b, c), R3__R6(a, c, d)
-- | | R1__R4__R5 [R2, R3__R6]
-- | | [4] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- [5] R1__R2__R3(a, b, c), R4(a, d), R5(b, d), R6(c, d)
-- | R4 [R1__R2__R3, R6]
-- | [6] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [7] R1__R2__R3(a, b, c), R4__R6(a, d, c), R5(b, d)
-- | | R5 [R1__R2__R3, R4__R6]
-- | | [8] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R5.cnt as cnt FROM R1, cnt_R5 WHERE cnt_R5.b = R1.b),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R5.a as a, best_R5.b as b, CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R5, cnt_R2 WHERE cnt_R2.b = best_R5.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node1_R1__R4__R5 AS SELECT node0_best.a as a, node0_best.b as b, R5.d as d FROM R5 JOIN node0_best ON node0_best.b = R5.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R1__R2__R3 AS SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM R2 JOIN node0_best ON node0_best.b = R2.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R4__R5 as (SELECT d, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY d),
  best_R1__R4__R5 as (SELECT R6.c as c, R6.d as d, 0 as tag, cnt_R1__R4__R5.cnt as cnt FROM R6, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.d = R6.d),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1__R4__R5.c as c, best_R1__R4__R5.d as d, CASE WHEN best_R1__R4__R5.cnt < cnt_R3.cnt THEN best_R1__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R4__R5.cnt < cnt_R3.cnt THEN best_R1__R4__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1__R4__R5, cnt_R3 WHERE cnt_R3.c = best_R1__R4__R5.c)
SELECT c, d, tag FROM best_R3;
CREATE TEMP TABLE node3_R3__R6 AS SELECT R3.a as a, node1_best.c as c, node1_best.d as d FROM R3 JOIN node1_best ON node1_best.c = R3.c WHERE node1_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R1__R2__R3 as (SELECT a, COUNT(*) as cnt FROM node5_R1__R2__R3 GROUP BY a),
  best_R1__R2__R3 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.a = R4.a),
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT best_R1__R2__R3.a as a, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R6.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R6.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R6.cnt END as cnt FROM best_R1__R2__R3, cnt_R6 WHERE cnt_R6.d = best_R1__R2__R3.d)
SELECT a, d, tag FROM best_R6;
CREATE TEMP TABLE node7_R4__R6 AS SELECT node5_best.a as a, node5_best.d as d, R6.c as c FROM R6 JOIN node5_best ON node5_best.d = R6.d WHERE node5_best.tag = 1;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT node1_R1__R4__R5.a as a, node1_R1__R4__R5.b as b, node1_R1__R4__R5.d as d, 0 as tag, cnt_R2.cnt as cnt FROM node1_R1__R4__R5, cnt_R2 WHERE cnt_R2.b = node1_R1__R4__R5.b),
  cnt_R3__R6 as (SELECT a,d, COUNT(*) as cnt FROM node3_R3__R6 GROUP BY a,d),
  best_R3__R6 as (SELECT best_R2.a as a, best_R2.b as b, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R3__R6.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3__R6.cnt THEN best_R2.cnt ELSE cnt_R3__R6.cnt END as cnt FROM best_R2, cnt_R3__R6 WHERE cnt_R3__R6.a = best_R2.a AND cnt_R3__R6.d = best_R2.d)
SELECT a, b, d, tag FROM best_R3__R6;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM node5_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R5, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = R5.b),
  cnt_R4__R6 as (SELECT d, COUNT(*) as cnt FROM node7_R4__R6 GROUP BY d),
  best_R4__R6 as (SELECT best_R1__R2__R3.b as b, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R4__R6.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R4__R6.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R4__R6.cnt END as cnt FROM best_R1__R2__R3, cnt_R4__R6 WHERE cnt_R4__R6.d = best_R1__R2__R3.d)
SELECT b, d, tag FROM best_R4__R6;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_R1__R4__R5.a as a, node1_R1__R4__R5.b as b, node1_best.d as d, node1_best.c as c FROM node1_R1__R4__R5 JOIN node1_best ON node1_best.d = node1_R1__R4__R5.d SEMI JOIN R3 ON R3.a = node1_R1__R4__R5.a AND R3.c = node1_best.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, node5_R1__R2__R3.b as b, node5_R1__R2__R3.c as c, node5_best.d as d FROM node5_R1__R2__R3 JOIN node5_best ON node5_best.a = node5_R1__R2__R3.a SEMI JOIN R6 ON R6.c = node5_R1__R2__R3.c AND R6.d = node5_best.d WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node3_best.d as d, R2.c as c FROM R2 JOIN node3_best ON node3_best.b = R2.b WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node3_best.d as d, node3_R3__R6.c as c FROM node3_R3__R6 JOIN node3_best ON node3_best.a = node3_R3__R6.a AND node3_best.d = node3_R3__R6.d SEMI JOIN R2 ON R2.b = node3_best.b AND R2.c = node3_R3__R6.c WHERE node3_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_R1__R2__R3.a as a, node7_best.b as b, node5_R1__R2__R3.c as c, node7_best.d as d FROM node5_R1__R2__R3 JOIN node7_best ON node7_best.b = node5_R1__R2__R3.b WHERE node7_best.tag = 0) node8_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node7_R4__R6.a as a, node7_best.b as b, node7_R4__R6.c as c, node7_best.d as d FROM node7_R4__R6 JOIN node7_best ON node7_best.d = node7_R4__R6.d SEMI JOIN node5_R1__R2__R3 ON node5_R1__R2__R3.a = node7_R4__R6.a AND node5_R1__R2__R3.b = node7_best.b AND node5_R1__R2__R3.c = node7_R4__R6.c WHERE node7_best.tag = 1) node8_R1__R2__R3__R4__R5__R6
);
