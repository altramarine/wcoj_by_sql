CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R6 [R5, R4]
-- [1] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R2__R5__R6 [R1, R4]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- [3] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R2 [R1, R5]
-- | [4] R1__R2(a, b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | | R5 [R1__R2, R3__R4__R6]
-- | | [5] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [6] R1(a, b), R2__R5(b, c, d), R3__R4__R6(a, c, d)
-- | | R3__R4__R6 [R1, R2__R5]
-- | | [7] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT R6.c as c, R6.d as d, 0 as tag, cnt_R5.cnt as cnt FROM R6, cnt_R5 WHERE cnt_R5.d = R6.d),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R5.c as c, best_R5.d as d, CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.cnt ELSE cnt_R4.cnt END as cnt FROM best_R5, cnt_R4 WHERE cnt_R4.d = best_R5.d)
SELECT c, d, tag FROM best_R4;
CREATE TEMP TABLE node1_R2__R5__R6 AS SELECT R5.b as b, node0_best.c as c, node0_best.d as d FROM R5 JOIN node0_best ON node0_best.d = R5.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R3__R4__R6 AS SELECT R4.a as a, node0_best.c as c, node0_best.d as d FROM R4 JOIN node0_best ON node0_best.d = R4.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT node1_R2__R5__R6.b as b, node1_R2__R5__R6.c as c, node1_R2__R5__R6.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node1_R2__R5__R6, cnt_R1 WHERE cnt_R1.b = node1_R2__R5__R6.b),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R1.b as b, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R4.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R4.cnt THEN best_R1.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1, cnt_R4 WHERE cnt_R4.d = best_R1.d)
SELECT b, c, d, tag FROM best_R4;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R5.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R5.cnt THEN best_R1.cnt ELSE cnt_R5.cnt END as cnt FROM best_R1, cnt_R5 WHERE cnt_R5.b = best_R1.b)
SELECT b, c, tag FROM best_R5;
CREATE TEMP TABLE node4_R1__R2 AS SELECT R1.a as a, node3_best.b as b, node3_best.c as c FROM R1 JOIN node3_best ON node3_best.b = R1.b WHERE node3_best.tag = 0;
CREATE TEMP TABLE node6_R2__R5 AS SELECT node3_best.b as b, node3_best.c as c, R5.d as d FROM R5 JOIN node3_best ON node3_best.b = R5.b WHERE node3_best.tag = 1;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1__R2 as (SELECT b, COUNT(*) as cnt FROM node4_R1__R2 GROUP BY b),
  best_R1__R2 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R1__R2.cnt as cnt FROM R5, cnt_R1__R2 WHERE cnt_R1__R2.b = R5.b),
  cnt_R3__R4__R6 as (SELECT d, COUNT(*) as cnt FROM node3_R3__R4__R6 GROUP BY d),
  best_R3__R4__R6 as (SELECT best_R1__R2.b as b, best_R1__R2.d as d, CASE WHEN best_R1__R2.cnt < cnt_R3__R4__R6.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R3__R4__R6.cnt THEN best_R1__R2.cnt ELSE cnt_R3__R4__R6.cnt END as cnt FROM best_R1__R2, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.d = best_R1__R2.d)
SELECT b, d, tag FROM best_R3__R4__R6;
CREATE TEMP TABLE node6_best AS WITH
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT node3_R3__R4__R6.a as a, node3_R3__R4__R6.c as c, node3_R3__R4__R6.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node3_R3__R4__R6, cnt_R1 WHERE cnt_R1.a = node3_R3__R4__R6.a),
  cnt_R2__R5 as (SELECT c,d, COUNT(*) as cnt FROM node6_R2__R5 GROUP BY c,d),
  best_R2__R5 as (SELECT best_R1.a as a, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R2__R5.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2__R5.cnt THEN best_R1.cnt ELSE cnt_R2__R5.cnt END as cnt FROM best_R1, cnt_R2__R5 WHERE cnt_R2__R5.c = best_R1.c AND cnt_R2__R5.d = best_R1.d)
SELECT a, c, d, tag FROM best_R2__R5;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT R1.a as a, node1_best.b as b, node1_best.c as c, node1_best.d as d FROM R1 JOIN node1_best ON node1_best.b = R1.b SEMI JOIN R4 ON R4.a = R1.a AND R4.d = node1_best.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT R4.a as a, node1_best.b as b, node1_best.c as c, node1_best.d as d FROM R4 JOIN node1_best ON node1_best.d = R4.d SEMI JOIN R1 ON R1.a = R4.a AND R1.b = node1_best.b WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node4_R1__R2.a as a, node4_best.b as b, node4_R1__R2.c as c, node4_best.d as d FROM node4_R1__R2 JOIN node4_best ON node4_best.b = node4_R1__R2.b WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_R3__R4__R6.a as a, node4_best.b as b, node3_R3__R4__R6.c as c, node4_best.d as d FROM node3_R3__R4__R6 JOIN node4_best ON node4_best.d = node3_R3__R4__R6.d SEMI JOIN node4_R1__R2 ON node4_R1__R2.a = node3_R3__R4__R6.a AND node4_R1__R2.b = node4_best.b AND node4_R1__R2.c = node3_R3__R4__R6.c WHERE node4_best.tag = 1) node5_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, R1.b as b, node6_best.c as c, node6_best.d as d FROM R1 JOIN node6_best ON node6_best.a = R1.a WHERE node6_best.tag = 0) node7_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, node6_R2__R5.b as b, node6_best.c as c, node6_best.d as d FROM node6_R2__R5 JOIN node6_best ON node6_best.c = node6_R2__R5.c AND node6_best.d = node6_R2__R5.d SEMI JOIN R1 ON R1.a = node6_best.a AND R1.b = node6_R2__R5.b WHERE node6_best.tag = 1) node7_R1__R2__R3__R4__R5__R6
);
