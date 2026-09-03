CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R5 [R1, R2]
-- [1] R1__R4__R5(a, b, d), R2(b, c), R3(a, c), R6(c, d)
-- | R3 [R1__R4__R5, R2]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [3] R1__R4__R5(a, b, d), R2__R3(b, c, a), R6(c, d)
-- | | R2__R3 [R6, R1__R4__R5]
-- | | [4] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- [5] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R1 [R2__R5__R6, R3]
-- | [6] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [7] R1__R3(a, b, c), R2__R5__R6(b, c, d), R4(a, d)
-- | | R4 [R1__R3, R2__R5__R6]
-- | | [8] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R1.cnt as cnt FROM R5, cnt_R1 WHERE cnt_R1.b = R5.b),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R1.b as b, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1, cnt_R2 WHERE cnt_R2.b = best_R1.b)
SELECT b, d, tag FROM best_R2;
CREATE TEMP TABLE node1_R1__R4__R5 AS SELECT R1.a as a, node0_best.b as b, node0_best.d as d FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R2__R5__R6 AS SELECT node0_best.b as b, R2.c as c, node0_best.d as d FROM R2 JOIN node0_best ON node0_best.b = R2.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R4__R5 as (SELECT a, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY a),
  best_R1__R4__R5 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R1__R4__R5.cnt as cnt FROM R3, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.a = R3.a),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R1__R4__R5.a as a, best_R1__R4__R5.c as c, CASE WHEN best_R1__R4__R5.cnt < cnt_R2.cnt THEN best_R1__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R4__R5.cnt < cnt_R2.cnt THEN best_R1__R4__R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1__R4__R5, cnt_R2 WHERE cnt_R2.c = best_R1__R4__R5.c)
SELECT a, c, tag FROM best_R2;
CREATE TEMP TABLE node3_R2__R3 AS SELECT R2.b as b, node1_best.c as c, node1_best.a as a FROM R2 JOIN node1_best ON node1_best.c = R2.c WHERE node1_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R2__R5__R6 as (SELECT b, COUNT(*) as cnt FROM node5_R2__R5__R6 GROUP BY b),
  best_R2__R5__R6 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R1, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.b as b, CASE WHEN best_R2__R5__R6.cnt < cnt_R3.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R3.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2__R5__R6, cnt_R3 WHERE cnt_R3.a = best_R2__R5__R6.a)
SELECT a, b, tag FROM best_R3;
CREATE TEMP TABLE node7_R1__R3 AS SELECT node5_best.a as a, node5_best.b as b, R3.c as c FROM R3 JOIN node5_best ON node5_best.a = R3.a WHERE node5_best.tag = 1;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT node3_R2__R3.b as b, node3_R2__R3.c as c, node3_R2__R3.a as a, 0 as tag, cnt_R6.cnt as cnt FROM node3_R2__R3, cnt_R6 WHERE cnt_R6.c = node3_R2__R3.c),
  cnt_R1__R4__R5 as (SELECT a,b, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY a,b),
  best_R1__R4__R5 as (SELECT best_R6.b as b, best_R6.c as c, best_R6.a as a, CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.cnt ELSE cnt_R1__R4__R5.cnt END as cnt FROM best_R6, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.a = best_R6.a AND cnt_R1__R4__R5.b = best_R6.b)
SELECT b, c, a, tag FROM best_R1__R4__R5;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R1__R3 as (SELECT a, COUNT(*) as cnt FROM node7_R1__R3 GROUP BY a),
  best_R1__R3 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R1__R3.cnt as cnt FROM R4, cnt_R1__R3 WHERE cnt_R1__R3.a = R4.a),
  cnt_R2__R5__R6 as (SELECT d, COUNT(*) as cnt FROM node5_R2__R5__R6 GROUP BY d),
  best_R2__R5__R6 as (SELECT best_R1__R3.a as a, best_R1__R3.d as d, CASE WHEN best_R1__R3.cnt < cnt_R2__R5__R6.cnt THEN best_R1__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R3.cnt < cnt_R2__R5__R6.cnt THEN best_R1__R3.cnt ELSE cnt_R2__R5__R6.cnt END as cnt FROM best_R1__R3, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.d = best_R1__R3.d)
SELECT a, d, tag FROM best_R2__R5__R6;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_R1__R4__R5.b as b, node1_R1__R4__R5.d as d, node1_best.c as c FROM node1_R1__R4__R5 JOIN node1_best ON node1_best.a = node1_R1__R4__R5.a SEMI JOIN R2 ON R2.b = node1_R1__R4__R5.b AND R2.c = node1_best.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, node5_best.b as b, node5_R2__R5__R6.c as c, node5_R2__R5__R6.d as d FROM node5_R2__R5__R6 JOIN node5_best ON node5_best.b = node5_R2__R5__R6.b SEMI JOIN R3 ON R3.a = node5_best.a AND R3.c = node5_R2__R5__R6.c WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, R6.d as d, node3_best.c as c FROM R6 JOIN node3_best ON node3_best.c = R6.c WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node1_R1__R4__R5.d as d, node3_best.c as c FROM node1_R1__R4__R5 JOIN node3_best ON node3_best.b = node1_R1__R4__R5.b AND node3_best.a = node1_R1__R4__R5.a SEMI JOIN R6 ON R6.c = node3_best.c AND R6.d = node1_R1__R4__R5.d WHERE node3_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node7_best.a as a, node7_R1__R3.b as b, node7_R1__R3.c as c, node7_best.d as d FROM node7_R1__R3 JOIN node7_best ON node7_best.a = node7_R1__R3.a WHERE node7_best.tag = 0) node8_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node8_R1__R2__R3__R4__R5__R6.a as a, node8_R1__R2__R3__R4__R5__R6.b as b, node8_R1__R2__R3__R4__R5__R6.c as c, node8_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node7_best.a as a, node5_R2__R5__R6.b as b, node5_R2__R5__R6.c as c, node7_best.d as d FROM node5_R2__R5__R6 JOIN node7_best ON node7_best.d = node5_R2__R5__R6.d WHERE node7_best.tag = 1) node8_R1__R2__R3__R4__R5__R6
);
