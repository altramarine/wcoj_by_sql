CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R6 [R2, R4]
-- [1] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R1 [R4, R2__R5__R6]
-- | [2] R1__R4(a, b, d), R2__R5__R6(b, c, d), R3(a, c)
-- | | R2__R5__R6 [R3, R1__R4]
-- | | [3] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- [5] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R2 [R5, R3__R4__R6]
-- | [6] R1(a, b), R2__R5(b, c, d), R3__R4__R6(a, c, d)
-- | | R1 [R3__R4__R6, R2__R5]
-- | | [7] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [8] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT R6.c as c, R6.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R6, cnt_R2 WHERE cnt_R2.c = R6.c),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R2, cnt_R4 WHERE cnt_R4.d = best_R2.d)
SELECT c, d, tag FROM best_R4;
CREATE TEMP TABLE node1_R2__R5__R6 AS SELECT R2.b as b, node0_best.c as c, node0_best.d as d FROM R2 JOIN node0_best ON node0_best.c = R2.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R3__R4__R6 AS SELECT R4.a as a, node0_best.c as c, node0_best.d as d FROM R4 JOIN node0_best ON node0_best.d = R4.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R4.cnt as cnt FROM R1, cnt_R4 WHERE cnt_R4.a = R1.a),
  cnt_R2__R5__R6 as (SELECT b, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY b),
  best_R2__R5__R6 as (SELECT best_R4.a as a, best_R4.b as b, CASE WHEN best_R4.cnt < cnt_R2__R5__R6.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R2__R5__R6.cnt THEN best_R4.cnt ELSE cnt_R2__R5__R6.cnt END as cnt FROM best_R4, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.b = best_R4.b)
SELECT a, b, tag FROM best_R2__R5__R6;
CREATE TEMP TABLE node2_R1__R4 AS SELECT node1_best.a as a, node1_best.b as b, R4.d as d FROM R4 JOIN node1_best ON node1_best.a = R4.a WHERE node1_best.tag = 0;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R5.cnt as cnt FROM R2, cnt_R5 WHERE cnt_R5.b = R2.b),
  cnt_R3__R4__R6 as (SELECT c, COUNT(*) as cnt FROM node5_R3__R4__R6 GROUP BY c),
  best_R3__R4__R6 as (SELECT best_R5.b as b, best_R5.c as c, CASE WHEN best_R5.cnt < cnt_R3__R4__R6.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R3__R4__R6.cnt THEN best_R5.cnt ELSE cnt_R3__R4__R6.cnt END as cnt FROM best_R5, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.c = best_R5.c)
SELECT b, c, tag FROM best_R3__R4__R6;
CREATE TEMP TABLE node6_R2__R5 AS SELECT node5_best.b as b, node5_best.c as c, R5.d as d FROM R5 JOIN node5_best ON node5_best.b = R5.b WHERE node5_best.tag = 0;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT node1_R2__R5__R6.b as b, node1_R2__R5__R6.c as c, node1_R2__R5__R6.d as d, 0 as tag, cnt_R3.cnt as cnt FROM node1_R2__R5__R6, cnt_R3 WHERE cnt_R3.c = node1_R2__R5__R6.c),
  cnt_R1__R4 as (SELECT b,d, COUNT(*) as cnt FROM node2_R1__R4 GROUP BY b,d),
  best_R1__R4 as (SELECT best_R3.b as b, best_R3.c as c, best_R3.d as d, CASE WHEN best_R3.cnt < cnt_R1__R4.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1__R4.cnt THEN best_R3.cnt ELSE cnt_R1__R4.cnt END as cnt FROM best_R3, cnt_R1__R4 WHERE cnt_R1__R4.b = best_R3.b AND cnt_R1__R4.d = best_R3.d)
SELECT b, c, d, tag FROM best_R1__R4;
CREATE TEMP TABLE node6_best AS WITH
  cnt_R3__R4__R6 as (SELECT a, COUNT(*) as cnt FROM node5_R3__R4__R6 GROUP BY a),
  best_R3__R4__R6 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R3__R4__R6.cnt as cnt FROM R1, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.a = R1.a),
  cnt_R2__R5 as (SELECT b, COUNT(*) as cnt FROM node6_R2__R5 GROUP BY b),
  best_R2__R5 as (SELECT best_R3__R4__R6.a as a, best_R3__R4__R6.b as b, CASE WHEN best_R3__R4__R6.cnt < cnt_R2__R5.cnt THEN best_R3__R4__R6.tag ELSE 1 END as tag,CASE WHEN best_R3__R4__R6.cnt < cnt_R2__R5.cnt THEN best_R3__R4__R6.cnt ELSE cnt_R2__R5.cnt END as cnt FROM best_R3__R4__R6, cnt_R2__R5 WHERE cnt_R2__R5.b = best_R3__R4__R6.b)
SELECT a, b, tag FROM best_R2__R5;
SELECT COUNT(*) FROM (
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_R2__R5__R6.c as c, node1_R2__R5__R6.d as d FROM node1_R2__R5__R6 JOIN node1_best ON node1_best.b = node1_R2__R5__R6.b SEMI JOIN R4 ON R4.a = node1_best.a AND R4.d = node1_R2__R5__R6.d WHERE node1_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_R3__R4__R6.a as a, node5_best.b as b, node5_best.c as c, node5_R3__R4__R6.d as d FROM node5_R3__R4__R6 JOIN node5_best ON node5_best.c = node5_R3__R4__R6.c SEMI JOIN R5 ON R5.b = node5_best.b AND R5.d = node5_R3__R4__R6.d WHERE node5_best.tag = 1) node8_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT R3.a as a, node2_best.b as b, node2_best.d as d, node2_best.c as c FROM R3 JOIN node2_best ON node2_best.c = R3.c WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_R1__R4.a as a, node2_best.b as b, node2_best.d as d, node2_best.c as c FROM node2_R1__R4 JOIN node2_best ON node2_best.b = node2_R1__R4.b AND node2_best.d = node2_R1__R4.d SEMI JOIN R3 ON R3.a = node2_R1__R4.a AND R3.c = node2_best.c WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, node6_best.b as b, node5_R3__R4__R6.c as c, node5_R3__R4__R6.d as d FROM node5_R3__R4__R6 JOIN node6_best ON node6_best.a = node5_R3__R4__R6.a WHERE node6_best.tag = 0) node7_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, node6_best.b as b, node6_R2__R5.c as c, node6_R2__R5.d as d FROM node6_R2__R5 JOIN node6_best ON node6_best.b = node6_R2__R5.b WHERE node6_best.tag = 1) node7_R1__R2__R3__R4__R5__R6
);
