CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R5 [R4, R1]
-- [1] R1__R4__R5(a, b, d), R2(b, c), R3(a, c), R6(c, d)
-- | R3 [R6, R1__R4__R5]
-- | [2] R1__R4__R5(a, b, d), R2(b, c), R3__R6(a, c, d)
-- | | R3__R6 [R2, R1__R4__R5]
-- | | [3] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- | [4] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R4.cnt as cnt FROM R5, cnt_R4 WHERE cnt_R4.d = R5.d),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R4.b as b, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.cnt ELSE cnt_R1.cnt END as cnt FROM best_R4, cnt_R1 WHERE cnt_R1.b = best_R4.b)
SELECT b, d, tag FROM best_R1;
CREATE TEMP TABLE node1_R1__R4__R5 AS SELECT R4.a as a, node0_best.b as b, node0_best.d as d FROM R4 JOIN node0_best ON node0_best.d = R4.d SEMI JOIN R1 ON R1.a = R4.a AND R1.b = node0_best.b WHERE node0_best.tag = 0
UNION ALL
SELECT R1.a as a, node0_best.b as b, node0_best.d as d FROM R1 JOIN node0_best ON node0_best.b = R1.b SEMI JOIN R4 ON R4.a = R1.a AND R4.d = node0_best.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R6.cnt as cnt FROM R3, cnt_R6 WHERE cnt_R6.c = R3.c),
  cnt_R1__R4__R5 as (SELECT a, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY a),
  best_R1__R4__R5 as (SELECT best_R6.a as a, best_R6.c as c, CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R1__R4__R5.cnt THEN best_R6.cnt ELSE cnt_R1__R4__R5.cnt END as cnt FROM best_R6, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.a = best_R6.a)
SELECT a, c, tag FROM best_R1__R4__R5;
CREATE TEMP TABLE node2_R3__R6 AS SELECT node1_best.a as a, node1_best.c as c, R6.d as d FROM R6 JOIN node1_best ON node1_best.c = R6.c WHERE node1_best.tag = 0;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT node2_R3__R6.a as a, node2_R3__R6.c as c, node2_R3__R6.d as d, 0 as tag, cnt_R2.cnt as cnt FROM node2_R3__R6, cnt_R2 WHERE cnt_R2.c = node2_R3__R6.c),
  cnt_R1__R4__R5 as (SELECT a,d, COUNT(*) as cnt FROM node1_R1__R4__R5 GROUP BY a,d),
  best_R1__R4__R5 as (SELECT best_R2.a as a, best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R1__R4__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1__R4__R5.cnt THEN best_R2.cnt ELSE cnt_R1__R4__R5.cnt END as cnt FROM best_R2, cnt_R1__R4__R5 WHERE cnt_R1__R4__R5.a = best_R2.a AND cnt_R1__R4__R5.d = best_R2.d)
SELECT a, c, d, tag FROM best_R1__R4__R5;
SELECT COUNT(*) FROM (
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_R1__R4__R5.b as b, node1_R1__R4__R5.d as d, node1_best.c as c FROM node1_R1__R4__R5 JOIN node1_best ON node1_best.a = node1_R1__R4__R5.a SEMI JOIN R6 ON R6.c = node1_best.c AND R6.d = node1_R1__R4__R5.d WHERE node1_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, R2.b as b, node2_best.d as d, node2_best.c as c FROM R2 JOIN node2_best ON node2_best.c = R2.c WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_best.a as a, node1_R1__R4__R5.b as b, node2_best.d as d, node2_best.c as c FROM node1_R1__R4__R5 JOIN node2_best ON node2_best.a = node1_R1__R4__R5.a AND node2_best.d = node1_R1__R4__R5.d SEMI JOIN R2 ON R2.b = node1_R1__R4__R5.b AND R2.c = node2_best.c WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5__R6
);
