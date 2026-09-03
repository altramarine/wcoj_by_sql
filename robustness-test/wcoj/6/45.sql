CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R3 [R6, R2]
-- [1] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R3__R4__R6 [R1, R2]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- [3] R1__R2__R3(a, b, c), R4(a, d), R5(b, d), R6(c, d)
-- | R5 [R1__R2__R3, R6]
-- | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [5] R1__R2__R3(a, b, c), R4(a, d), R5__R6(b, d, c)
-- | | R4 [R1__R2__R3, R5__R6]
-- | | [6] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R6.cnt as cnt FROM R3, cnt_R6 WHERE cnt_R6.c = R3.c),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R6.a as a, best_R6.c as c, CASE WHEN best_R6.cnt < cnt_R2.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R2.cnt THEN best_R6.cnt ELSE cnt_R2.cnt END as cnt FROM best_R6, cnt_R2 WHERE cnt_R2.c = best_R6.c)
SELECT a, c, tag FROM best_R2;
CREATE TEMP TABLE node1_R3__R4__R6 AS SELECT node0_best.a as a, node0_best.c as c, R6.d as d FROM R6 JOIN node0_best ON node0_best.c = R6.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R1__R2__R3 AS SELECT node0_best.a as a, R2.b as b, node0_best.c as c FROM R2 JOIN node0_best ON node0_best.c = R2.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT node1_R3__R4__R6.a as a, node1_R3__R4__R6.c as c, node1_R3__R4__R6.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node1_R3__R4__R6, cnt_R1 WHERE cnt_R1.a = node1_R3__R4__R6.a),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R1.a as a, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1, cnt_R2 WHERE cnt_R2.c = best_R1.c)
SELECT a, c, d, tag FROM best_R2;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM node3_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R5, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = R5.b),
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT best_R1__R2__R3.b as b, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R6.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R6.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R6.cnt END as cnt FROM best_R1__R2__R3, cnt_R6 WHERE cnt_R6.d = best_R1__R2__R3.d)
SELECT b, d, tag FROM best_R6;
CREATE TEMP TABLE node5_R5__R6 AS SELECT node3_best.b as b, node3_best.d as d, R6.c as c FROM R6 JOIN node3_best ON node3_best.d = R6.d WHERE node3_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R1__R2__R3 as (SELECT a, COUNT(*) as cnt FROM node3_R1__R2__R3 GROUP BY a),
  best_R1__R2__R3 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.a = R4.a),
  cnt_R5__R6 as (SELECT d, COUNT(*) as cnt FROM node5_R5__R6 GROUP BY d),
  best_R5__R6 as (SELECT best_R1__R2__R3.a as a, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R5__R6.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R5__R6.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R5__R6.cnt END as cnt FROM best_R1__R2__R3, cnt_R5__R6 WHERE cnt_R5__R6.d = best_R1__R2__R3.d)
SELECT a, d, tag FROM best_R5__R6;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, R1.b as b, node1_best.c as c, node1_best.d as d FROM R1 JOIN node1_best ON node1_best.a = R1.a SEMI JOIN R2 ON R2.b = R1.b AND R2.c = node1_best.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, R2.b as b, node1_best.c as c, node1_best.d as d FROM R2 JOIN node1_best ON node1_best.c = R2.c SEMI JOIN R1 ON R1.a = node1_best.a AND R1.b = R2.b WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_R1__R2__R3.a as a, node3_best.b as b, node3_R1__R2__R3.c as c, node3_best.d as d FROM node3_R1__R2__R3 JOIN node3_best ON node3_best.b = node3_R1__R2__R3.b SEMI JOIN R6 ON R6.c = node3_R1__R2__R3.c AND R6.d = node3_best.d WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, node3_R1__R2__R3.b as b, node3_R1__R2__R3.c as c, node5_best.d as d FROM node3_R1__R2__R3 JOIN node5_best ON node5_best.a = node3_R1__R2__R3.a SEMI JOIN node5_R5__R6 ON node5_R5__R6.b = node3_R1__R2__R3.b AND node5_R5__R6.d = node5_best.d AND node5_R5__R6.c = node3_R1__R2__R3.c WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, node5_R5__R6.b as b, node5_R5__R6.c as c, node5_best.d as d FROM node5_R5__R6 JOIN node5_best ON node5_best.d = node5_R5__R6.d SEMI JOIN node3_R1__R2__R3 ON node3_R1__R2__R3.a = node5_best.a AND node3_R1__R2__R3.b = node5_R5__R6.b AND node3_R1__R2__R3.c = node5_R5__R6.c WHERE node5_best.tag = 1) node6_R1__R2__R3__R4__R5__R6
);
