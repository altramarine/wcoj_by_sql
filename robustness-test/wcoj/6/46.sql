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
-- | R2 [R6, R1__R4__R5]
-- | [2] R1__R4__R5(a, b, d), R2__R6(b, c, d), R3(a, c)
-- | | R3 [R1__R4__R5, R2__R6]
-- | | [3] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [4] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- [5] R1__R2__R3(a, b, c), R4(a, d), R5(b, d), R6(c, d)
-- | R1__R2__R3 [R5, R4]
-- | [6] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R5.cnt as cnt FROM R1, cnt_R5 WHERE cnt_R5.b = R1.b),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R5.a as a, best_R5.b as b, CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R5, cnt_R2 WHERE cnt_R2.b = best_R5.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node1_R1__R4__R5 AS SELECT node0_best.a as a, node0_best.b as b, R5.d as d FROM R5 JOIN node0_best ON node0_best.b = R5.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R1__R2__R3 AS SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM R2 JOIN node0_best ON node0_best.b = R2.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R6.cnt as cnt FROM R2, cnt_R6 WHERE cnt_R6.c = R2.c),
  cnt_R1__R4__R5 as (SELECT b, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY b),
  best_R1__R4__R5 as (SELECT best_R6.b as b, best_R6.c as c, CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.cnt ELSE cnt_R1__R4__R5.cnt END as cnt FROM best_R6, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.b = best_R6.b)
SELECT b, c, tag FROM best_R1__R4__R5;
CREATE TEMP TABLE node2_R2__R6 AS SELECT node1_best.b as b, node1_best.c as c, R6.d as d FROM R6 JOIN node1_best ON node1_best.c = R6.c WHERE node1_best.tag = 0;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT node5_R1__R2__R3.a as a, node5_R1__R2__R3.b as b, node5_R1__R2__R3.c as c, 0 as tag, cnt_R5.cnt as cnt FROM node5_R1__R2__R3, cnt_R5 WHERE cnt_R5.b = node5_R1__R2__R3.b),
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT best_R5.a as a, best_R5.b as b, best_R5.c as c, CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.cnt ELSE cnt_R4.cnt END as cnt FROM best_R5, cnt_R4 WHERE cnt_R4.a = best_R5.a)
SELECT a, b, c, tag FROM best_R4;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R1__R4__R5 as (SELECT a, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY a),
  best_R1__R4__R5 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R1__R4__R5.cnt as cnt FROM R3, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.a = R3.a),
  cnt_R2__R6 as (SELECT c, COUNT(*) as cnt FROM node2_R2__R6 GROUP BY c),
  best_R2__R6 as (SELECT best_R1__R4__R5.a as a, best_R1__R4__R5.c as c, CASE WHEN best_R1__R4__R5.cnt < cnt_R2__R6.cnt THEN best_R1__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R4__R5.cnt < cnt_R2__R6.cnt THEN best_R1__R4__R5.cnt ELSE cnt_R2__R6.cnt END as cnt FROM best_R1__R4__R5, cnt_R2__R6 WHERE cnt_R2__R6.c = best_R1__R4__R5.c)
SELECT a, c, tag FROM best_R2__R6;
SELECT COUNT(*) FROM (
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_R1__R4__R5.a as a, node1_best.b as b, node1_R1__R4__R5.d as d, node1_best.c as c FROM node1_R1__R4__R5 JOIN node1_best ON node1_best.b = node1_R1__R4__R5.b SEMI JOIN R6 ON R6.c = node1_best.c AND R6.d = node1_R1__R4__R5.d WHERE node1_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, node5_best.b as b, node5_best.c as c, R5.d as d FROM R5 JOIN node5_best ON node5_best.b = R5.b SEMI JOIN R4 ON R4.a = node5_best.a AND R4.d = R5.d WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, node5_best.b as b, node5_best.c as c, R4.d as d FROM R4 JOIN node5_best ON node5_best.a = R4.a SEMI JOIN R5 ON R5.b = node5_best.b AND R5.d = R4.d WHERE node5_best.tag = 1) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, node1_R1__R4__R5.b as b, node1_R1__R4__R5.d as d, node2_best.c as c FROM node1_R1__R4__R5 JOIN node2_best ON node2_best.a = node1_R1__R4__R5.a WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, node2_R2__R6.b as b, node2_R2__R6.d as d, node2_best.c as c FROM node2_R2__R6 JOIN node2_best ON node2_best.c = node2_R2__R6.c WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5__R6
);
