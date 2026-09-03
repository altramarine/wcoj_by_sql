CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R3 [R4, R6]
-- [1] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R2 [R5, R1]
-- | [2] R1(a, b), R2__R5(b, c, d), R3__R4__R6(a, c, d)
-- | | R3__R4__R6 [R1, R2__R5]
-- | | [3] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [4] R1__R2(a, b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | | R5 [R1__R2, R3__R4__R6]
-- | | [5] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R4.cnt as cnt FROM R3, cnt_R4 WHERE cnt_R4.a = R3.a),
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT best_R4.a as a, best_R4.c as c, CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.cnt ELSE cnt_R6.cnt END as cnt FROM best_R4, cnt_R6 WHERE cnt_R6.c = best_R4.c)
SELECT a, c, tag FROM best_R6;
CREATE TEMP TABLE node1_R3__R4__R6 AS SELECT node0_best.a as a, node0_best.c as c, R4.d as d FROM R4 JOIN node0_best ON node0_best.a = R4.a SEMI JOIN R6 ON R6.c = node0_best.c AND R6.d = R4.d WHERE node0_best.tag = 0
UNION ALL
SELECT node0_best.a as a, node0_best.c as c, R6.d as d FROM R6 JOIN node0_best ON node0_best.c = R6.c SEMI JOIN R4 ON R4.a = node0_best.a AND R4.d = R6.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R5.cnt as cnt FROM R2, cnt_R5 WHERE cnt_R5.b = R2.b),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R5.b as b, best_R5.c as c, CASE WHEN best_R5.cnt < cnt_R1.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R1.cnt THEN best_R5.cnt ELSE cnt_R1.cnt END as cnt FROM best_R5, cnt_R1 WHERE cnt_R1.b = best_R5.b)
SELECT b, c, tag FROM best_R1;
CREATE TEMP TABLE node2_R2__R5 AS SELECT node1_best.b as b, node1_best.c as c, R5.d as d FROM R5 JOIN node1_best ON node1_best.b = R5.b WHERE node1_best.tag = 0;
CREATE TEMP TABLE node4_R1__R2 AS SELECT R1.a as a, node1_best.b as b, node1_best.c as c FROM R1 JOIN node1_best ON node1_best.b = R1.b WHERE node1_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT node1_R3__R4__R6.a as a, node1_R3__R4__R6.c as c, node1_R3__R4__R6.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node1_R3__R4__R6, cnt_R1 WHERE cnt_R1.a = node1_R3__R4__R6.a),
  cnt_R2__R5 as (SELECT c,d, COUNT(*) as cnt FROM node2_R2__R5 GROUP BY c,d),
  best_R2__R5 as (SELECT best_R1.a as a, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R2__R5.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2__R5.cnt THEN best_R1.cnt ELSE cnt_R2__R5.cnt END as cnt FROM best_R1, cnt_R2__R5 WHERE cnt_R2__R5.c = best_R1.c AND cnt_R2__R5.d = best_R1.d)
SELECT a, c, d, tag FROM best_R2__R5;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1__R2 as (SELECT b, COUNT(*) as cnt FROM node4_R1__R2 GROUP BY b),
  best_R1__R2 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R1__R2.cnt as cnt FROM R5, cnt_R1__R2 WHERE cnt_R1__R2.b = R5.b),
  cnt_R3__R4__R6 as (SELECT d, COUNT(*) as cnt FROM node1_R3__R4__R6 GROUP BY d),
  best_R3__R4__R6 as (SELECT best_R1__R2.b as b, best_R1__R2.d as d, CASE WHEN best_R1__R2.cnt < cnt_R3__R4__R6.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R3__R4__R6.cnt THEN best_R1__R2.cnt ELSE cnt_R3__R4__R6.cnt END as cnt FROM best_R1__R2, cnt_R3__R4__R6 WHERE cnt_R3__R4__R6.d = best_R1__R2.d)
SELECT b, d, tag FROM best_R3__R4__R6;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, R1.b as b, node2_best.c as c, node2_best.d as d FROM R1 JOIN node2_best ON node2_best.a = R1.a WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, node2_R2__R5.b as b, node2_best.c as c, node2_best.d as d FROM node2_R2__R5 JOIN node2_best ON node2_best.c = node2_R2__R5.c AND node2_best.d = node2_R2__R5.d SEMI JOIN R1 ON R1.a = node2_best.a AND R1.b = node2_R2__R5.b WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node4_R1__R2.a as a, node4_best.b as b, node4_R1__R2.c as c, node4_best.d as d FROM node4_R1__R2 JOIN node4_best ON node4_best.b = node4_R1__R2.b WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node5_R1__R2__R3__R4__R5__R6.a as a, node5_R1__R2__R3__R4__R5__R6.b as b, node5_R1__R2__R3__R4__R5__R6.c as c, node5_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_R3__R4__R6.a as a, node4_best.b as b, node1_R3__R4__R6.c as c, node4_best.d as d FROM node1_R3__R4__R6 JOIN node4_best ON node4_best.d = node1_R3__R4__R6.d SEMI JOIN node4_R1__R2 ON node4_R1__R2.a = node1_R3__R4__R6.a AND node4_R1__R2.b = node4_best.b AND node4_R1__R2.c = node1_R3__R4__R6.c WHERE node4_best.tag = 1) node5_R1__R2__R3__R4__R5__R6
);
