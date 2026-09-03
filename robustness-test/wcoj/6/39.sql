CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R2 [R5, R3]
-- [1] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R1 [R2__R5__R6, R4]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [3] R1__R4(a, b, d), R2__R5__R6(b, c, d), R3(a, c)
-- | | R2__R5__R6 [R1__R4, R3]
-- | | [4] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- [5] R1__R2__R3(a, b, c), R4(a, d), R5(b, d), R6(c, d)
-- | R5 [R6, R1__R2__R3]
-- | [6] R1__R2__R3(a, b, c), R4(a, d), R5__R6(b, d, c)
-- | | R1__R2__R3 [R5__R6, R4]
-- | | [7] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [8] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R5.cnt as cnt FROM R2, cnt_R5 WHERE cnt_R5.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R5.b as b, best_R5.c as c, CASE WHEN best_R5.cnt < cnt_R3.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R3.cnt THEN best_R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R5, cnt_R3 WHERE cnt_R3.c = best_R5.c)
SELECT b, c, tag FROM best_R3;
CREATE TEMP TABLE node1_R2__R5__R6 AS SELECT node0_best.b as b, node0_best.c as c, R5.d as d FROM R5 JOIN node0_best ON node0_best.b = R5.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R1__R2__R3 AS SELECT R3.a as a, node0_best.b as b, node0_best.c as c FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2__R5__R6 as (SELECT b, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY b),
  best_R2__R5__R6 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R1, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.b = R1.b),
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.b as b, CASE WHEN best_R2__R5__R6.cnt < cnt_R4.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R4.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R4.cnt END as cnt FROM best_R2__R5__R6, cnt_R4 WHERE cnt_R4.a = best_R2__R5__R6.a)
SELECT a, b, tag FROM best_R4;
CREATE TEMP TABLE node3_R1__R4 AS SELECT node1_best.a as a, node1_best.b as b, R4.d as d FROM R4 JOIN node1_best ON node1_best.a = R4.a WHERE node1_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R6.cnt as cnt FROM R5, cnt_R6 WHERE cnt_R6.d = R5.d),
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM node5_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT best_R6.b as b, best_R6.d as d, CASE WHEN best_R6.cnt < cnt_R1__R2__R3.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R1__R2__R3.cnt THEN best_R6.cnt ELSE cnt_R1__R2__R3.cnt END as cnt FROM best_R6, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = best_R6.b)
SELECT b, d, tag FROM best_R1__R2__R3;
CREATE TEMP TABLE node6_R5__R6 AS SELECT node5_best.b as b, node5_best.d as d, R6.c as c FROM R6 JOIN node5_best ON node5_best.d = R6.d WHERE node5_best.tag = 0;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R1__R4 as (SELECT b,d, COUNT(*) as cnt FROM node3_R1__R4 GROUP BY b,d),
  best_R1__R4 as (SELECT node1_R2__R5__R6.b as b, node1_R2__R5__R6.c as c, node1_R2__R5__R6.d as d, 0 as tag, cnt_R1__R4.cnt as cnt FROM node1_R2__R5__R6, cnt_R1__R4 WHERE cnt_R1__R4.b = node1_R2__R5__R6.b AND cnt_R1__R4.d = node1_R2__R5__R6.d),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1__R4.b as b, best_R1__R4.c as c, best_R1__R4.d as d, CASE WHEN best_R1__R4.cnt < cnt_R3.cnt THEN best_R1__R4.tag ELSE 1 END as tag,CASE WHEN best_R1__R4.cnt < cnt_R3.cnt THEN best_R1__R4.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1__R4, cnt_R3 WHERE cnt_R3.c = best_R1__R4.c)
SELECT b, c, d, tag FROM best_R3;
CREATE TEMP TABLE node6_best AS WITH
  cnt_R5__R6 as (SELECT b,c, COUNT(*) as cnt FROM node6_R5__R6 GROUP BY b,c),
  best_R5__R6 as (SELECT node5_R1__R2__R3.a as a, node5_R1__R2__R3.b as b, node5_R1__R2__R3.c as c, 0 as tag, cnt_R5__R6.cnt as cnt FROM node5_R1__R2__R3, cnt_R5__R6 WHERE cnt_R5__R6.b = node5_R1__R2__R3.b AND cnt_R5__R6.c = node5_R1__R2__R3.c),
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT best_R5__R6.a as a, best_R5__R6.b as b, best_R5__R6.c as c, CASE WHEN best_R5__R6.cnt < cnt_R4.cnt THEN best_R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R5__R6.cnt < cnt_R4.cnt THEN best_R5__R6.cnt ELSE cnt_R4.cnt END as cnt FROM best_R5__R6, cnt_R4 WHERE cnt_R4.a = best_R5__R6.a)
SELECT a, b, c, tag FROM best_R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_R2__R5__R6.c as c, node1_R2__R5__R6.d as d FROM node1_R2__R5__R6 JOIN node1_best ON node1_best.b = node1_R2__R5__R6.b SEMI JOIN R4 ON R4.a = node1_best.a AND R4.d = node1_R2__R5__R6.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_R1__R2__R3.a as a, node5_best.b as b, node5_R1__R2__R3.c as c, node5_best.d as d FROM node5_R1__R2__R3 JOIN node5_best ON node5_best.b = node5_R1__R2__R3.b SEMI JOIN R6 ON R6.c = node5_R1__R2__R3.c AND R6.d = node5_best.d WHERE node5_best.tag = 1) node8_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_R1__R4.a as a, node3_best.b as b, node3_best.d as d, node3_best.c as c FROM node3_R1__R4 JOIN node3_best ON node3_best.b = node3_R1__R4.b AND node3_best.d = node3_R1__R4.d SEMI JOIN R3 ON R3.a = node3_R1__R4.a AND R3.c = node3_best.c WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT R3.a as a, node3_best.b as b, node3_best.d as d, node3_best.c as c FROM R3 JOIN node3_best ON node3_best.c = R3.c WHERE node3_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, node6_best.b as b, node6_best.c as c, node6_R5__R6.d as d FROM node6_R5__R6 JOIN node6_best ON node6_best.b = node6_R5__R6.b AND node6_best.c = node6_R5__R6.c SEMI JOIN R4 ON R4.a = node6_best.a AND R4.d = node6_R5__R6.d WHERE node6_best.tag = 0) node7_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node7_R1__R2__R3__R4__R5__R6.a as a, node7_R1__R2__R3__R4__R5__R6.b as b, node7_R1__R2__R3__R4__R5__R6.c as c, node7_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node6_best.a as a, node6_best.b as b, node6_best.c as c, R4.d as d FROM R4 JOIN node6_best ON node6_best.a = R4.a SEMI JOIN node6_R5__R6 ON node6_R5__R6.b = node6_best.b AND node6_R5__R6.d = R4.d AND node6_R5__R6.c = node6_best.c WHERE node6_best.tag = 1) node7_R1__R2__R3__R4__R5__R6
);
