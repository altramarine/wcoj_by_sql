CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R4 [R2, R1]
-- [1] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R2__R4__R5, R3]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [3] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
-- [4] R1__R4(a, b, d), R2(b, c), R3(a, c), R5(c, d)
-- | R5 [R1__R4, R2]
-- | [5] R1__R2__R3__R4__R5(a, b, d, c) [acyclic]
-- | [6] R1__R4(a, b, d), R2__R5(b, c, d), R3(a, c)
-- | | R2__R5 [R3, R1__R4]
-- | | [7] R1__R2__R3__R4__R5(a, b, d, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R4, cnt_R2 WHERE cnt_R2.b = R4.b),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R2.b as b, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R1.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1.cnt THEN best_R2.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2, cnt_R1 WHERE cnt_R1.b = best_R2.b)
SELECT b, d, tag FROM best_R1;
CREATE TEMP TABLE node1_R2__R4__R5 AS SELECT node0_best.b as b, R2.c as c, node0_best.d as d FROM R2 JOIN node0_best ON node0_best.b = R2.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R1__R4 AS SELECT R1.a as a, node0_best.b as b, node0_best.d as d FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2__R4__R5 as (SELECT b, COUNT(*) as cnt FROM node1_R2__R4__R5 GROUP BY b),
  best_R2__R4__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R4__R5.cnt as cnt FROM R1, cnt_R2__R4__R5 WHERE cnt_R2__R4__R5.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2__R4__R5.a as a, best_R2__R4__R5.b as b, CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2__R4__R5, cnt_R3 WHERE cnt_R3.a = best_R2__R4__R5.a)
SELECT a, b, tag FROM best_R3;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1__R4 as (SELECT d, COUNT(*) as cnt FROM node4_R1__R4 GROUP BY d),
  best_R1__R4 as (SELECT R5.c as c, R5.d as d, 0 as tag, cnt_R1__R4.cnt as cnt FROM R5, cnt_R1__R4 WHERE cnt_R1__R4.d = R5.d),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R1__R4.c as c, best_R1__R4.d as d, CASE WHEN best_R1__R4.cnt < cnt_R2.cnt THEN best_R1__R4.tag ELSE 1 END as tag,CASE WHEN best_R1__R4.cnt < cnt_R2.cnt THEN best_R1__R4.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1__R4, cnt_R2 WHERE cnt_R2.c = best_R1__R4.c)
SELECT c, d, tag FROM best_R2;
CREATE TEMP TABLE node6_R2__R5 AS SELECT R2.b as b, node4_best.c as c, node4_best.d as d FROM R2 JOIN node4_best ON node4_best.c = R2.c WHERE node4_best.tag = 1;
CREATE TEMP TABLE node6_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT node6_R2__R5.b as b, node6_R2__R5.c as c, node6_R2__R5.d as d, 0 as tag, cnt_R3.cnt as cnt FROM node6_R2__R5, cnt_R3 WHERE cnt_R3.c = node6_R2__R5.c),
  cnt_R1__R4 as (SELECT b,d, COUNT(*) as cnt FROM node4_R1__R4 GROUP BY b,d),
  best_R1__R4 as (SELECT best_R3.b as b, best_R3.c as c, best_R3.d as d, CASE WHEN best_R3.cnt < cnt_R1__R4.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1__R4.cnt THEN best_R3.cnt ELSE cnt_R1__R4.cnt END as cnt FROM best_R3, cnt_R1__R4 WHERE cnt_R1__R4.b = best_R3.b AND cnt_R1__R4.d = best_R3.d)
SELECT b, c, d, tag FROM best_R1__R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_R2__R4__R5.c as c, node1_R2__R4__R5.d as d FROM node1_R2__R4__R5 JOIN node1_best ON node1_best.b = node1_R2__R4__R5.b SEMI JOIN R3 ON R3.a = node1_best.a AND R3.c = node1_R2__R4__R5.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node3_R1__R3.a as a, node3_R1__R3.b as b, node3_R1__R3.c as c, node1_R2__R4__R5.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, R3.c as c FROM R3 JOIN node1_best ON node1_best.a = R3.a WHERE node1_best.tag = 1) node3_R1__R3 JOIN node1_R2__R4__R5 ON node1_R2__R4__R5.b = node3_R1__R3.b AND node1_R2__R4__R5.c = node3_R1__R3.c
UNION ALL
SELECT node5_R1__R2__R3__R4__R5.a as a, node5_R1__R2__R3__R4__R5.b as b, node5_R1__R2__R3__R4__R5.c as c, node5_R1__R2__R3__R4__R5.d as d FROM (SELECT node4_R1__R4.a as a, node4_R1__R4.b as b, node4_best.d as d, node4_best.c as c FROM node4_R1__R4 JOIN node4_best ON node4_best.d = node4_R1__R4.d SEMI JOIN R2 ON R2.b = node4_R1__R4.b AND R2.c = node4_best.c WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5
UNION ALL
SELECT node7_R1__R2__R3__R4__R5.a as a, node7_R1__R2__R3__R4__R5.b as b, node7_R1__R2__R3__R4__R5.c as c, node7_R1__R2__R3__R4__R5.d as d FROM (SELECT R3.a as a, node6_best.b as b, node6_best.d as d, node6_best.c as c FROM R3 JOIN node6_best ON node6_best.c = R3.c WHERE node6_best.tag = 0) node7_R1__R2__R3__R4__R5
UNION ALL
SELECT node7_R1__R2__R3__R4__R5.a as a, node7_R1__R2__R3__R4__R5.b as b, node7_R1__R2__R3__R4__R5.c as c, node7_R1__R2__R3__R4__R5.d as d FROM (SELECT node4_R1__R4.a as a, node6_best.b as b, node6_best.d as d, node6_best.c as c FROM node4_R1__R4 JOIN node6_best ON node6_best.b = node4_R1__R4.b AND node6_best.d = node4_R1__R4.d SEMI JOIN R3 ON R3.a = node4_R1__R4.a AND R3.c = node6_best.c WHERE node6_best.tag = 1) node7_R1__R2__R3__R4__R5
);
