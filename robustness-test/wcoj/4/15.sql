CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R5 [R3, R4]
-- [1] R1(a, b), R2(b, c), R3__R5(a, c, d), R4(b, d)
-- | R1 [R2, R3__R5]
-- | [2] R1__R2(a, b, c), R3__R5(a, c, d), R4(b, d)
-- | | R3__R5 [R1__R2, R4]
-- | | [3] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [4] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- [5] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R3, R2__R4__R5]
-- | [6] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
-- | [7] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT R5.c as c, R5.d as d, 0 as tag, cnt_R3.cnt as cnt FROM R5, cnt_R3 WHERE cnt_R3.c = R5.c),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R3.c as c, best_R3.d as d, CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.cnt ELSE cnt_R4.cnt END as cnt FROM best_R3, cnt_R4 WHERE cnt_R4.d = best_R3.d)
SELECT c, d, tag FROM best_R4;
CREATE TEMP TABLE node1_R3__R5 AS SELECT R3.a as a, node0_best.c as c, node0_best.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R2__R4__R5 AS SELECT R4.b as b, node0_best.c as c, node0_best.d as d FROM R4 JOIN node0_best ON node0_best.d = R4.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2.cnt as cnt FROM R1, cnt_R2 WHERE cnt_R2.b = R1.b),
  cnt_R3__R5 as (SELECT a, COUNT(*) as cnt FROM node1_R3__R5 GROUP BY a),
  best_R3__R5 as (SELECT best_R2.a as a, best_R2.b as b, CASE WHEN best_R2.cnt < cnt_R3__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3__R5.cnt THEN best_R2.cnt ELSE cnt_R3__R5.cnt END as cnt FROM best_R2, cnt_R3__R5 WHERE cnt_R3__R5.a = best_R2.a)
SELECT a, b, tag FROM best_R3__R5;
CREATE TEMP TABLE node2_R1__R2 AS SELECT node1_best.a as a, node1_best.b as b, R2.c as c FROM R2 JOIN node1_best ON node1_best.b = R2.b WHERE node1_best.tag = 0;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R3.cnt as cnt FROM R1, cnt_R3 WHERE cnt_R3.a = R1.a),
  cnt_R2__R4__R5 as (SELECT b, COUNT(*) as cnt FROM node5_R2__R4__R5 GROUP BY b),
  best_R2__R4__R5 as (SELECT best_R3.a as a, best_R3.b as b, CASE WHEN best_R3.cnt < cnt_R2__R4__R5.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R2__R4__R5.cnt THEN best_R3.cnt ELSE cnt_R2__R4__R5.cnt END as cnt FROM best_R3, cnt_R2__R4__R5 WHERE cnt_R2__R4__R5.b = best_R3.b)
SELECT a, b, tag FROM best_R2__R4__R5;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R1__R2 as (SELECT a,c, COUNT(*) as cnt FROM node2_R1__R2 GROUP BY a,c),
  best_R1__R2 as (SELECT node1_R3__R5.a as a, node1_R3__R5.c as c, node1_R3__R5.d as d, 0 as tag, cnt_R1__R2.cnt as cnt FROM node1_R3__R5, cnt_R1__R2 WHERE cnt_R1__R2.a = node1_R3__R5.a AND cnt_R1__R2.c = node1_R3__R5.c),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R1__R2.a as a, best_R1__R2.c as c, best_R1__R2.d as d, CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1__R2, cnt_R4 WHERE cnt_R4.d = best_R1__R2.d)
SELECT a, c, d, tag FROM best_R4;
SELECT COUNT(*) FROM (
SELECT node4_R1__R2__R3__R4__R5.a as a, node4_R1__R2__R3__R4__R5.b as b, node4_R1__R2__R3__R4__R5.c as c, node4_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_R3__R5.c as c, node1_R3__R5.d as d FROM node1_R3__R5 JOIN node1_best ON node1_best.a = node1_R3__R5.a SEMI JOIN R2 ON R2.b = node1_best.b AND R2.c = node1_R3__R5.c WHERE node1_best.tag = 1) node4_R1__R2__R3__R4__R5
UNION ALL
SELECT node6_R1__R3.a as a, node6_R1__R3.b as b, node6_R1__R3.c as c, node5_R2__R4__R5.d as d FROM (SELECT node5_best.a as a, node5_best.b as b, R3.c as c FROM R3 JOIN node5_best ON node5_best.a = R3.a WHERE node5_best.tag = 0) node6_R1__R3 JOIN node5_R2__R4__R5 ON node5_R2__R4__R5.b = node6_R1__R3.b AND node5_R2__R4__R5.c = node6_R1__R3.c
UNION ALL
SELECT node7_R1__R2__R3__R4__R5.a as a, node7_R1__R2__R3__R4__R5.b as b, node7_R1__R2__R3__R4__R5.c as c, node7_R1__R2__R3__R4__R5.d as d FROM (SELECT node5_best.a as a, node5_best.b as b, node5_R2__R4__R5.c as c, node5_R2__R4__R5.d as d FROM node5_R2__R4__R5 JOIN node5_best ON node5_best.b = node5_R2__R4__R5.b SEMI JOIN R3 ON R3.a = node5_best.a AND R3.c = node5_R2__R4__R5.c WHERE node5_best.tag = 1) node7_R1__R2__R3__R4__R5
UNION ALL
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d FROM (SELECT node2_best.a as a, node2_R1__R2.b as b, node2_best.c as c, node2_best.d as d FROM node2_R1__R2 JOIN node2_best ON node2_best.a = node2_R1__R2.a AND node2_best.c = node2_R1__R2.c SEMI JOIN R4 ON R4.b = node2_R1__R2.b AND R4.d = node2_best.d WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d FROM (SELECT node2_best.a as a, R4.b as b, node2_best.c as c, node2_best.d as d FROM R4 JOIN node2_best ON node2_best.d = R4.d SEMI JOIN node2_R1__R2 ON node2_R1__R2.a = node2_best.a AND node2_R1__R2.b = R4.b AND node2_R1__R2.c = node2_best.c WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5
);
