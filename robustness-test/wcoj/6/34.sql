CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R5 [R4, R6]
-- [1] R1__R4__R5(a, b, d), R2(b, c), R3(a, c), R6(c, d)
-- | R1__R4__R5 [R2, R6]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- [3] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R4 [R2__R5__R6, R1]
-- | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [5] R1__R4(a, b, d), R2__R5__R6(b, c, d), R3(a, c)
-- | | R2__R5__R6 [R3, R1__R4]
-- | | [6] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R4.cnt as cnt FROM R5, cnt_R4 WHERE cnt_R4.d = R5.d),
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT best_R4.b as b, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R6.cnt THEN best_R4.cnt ELSE cnt_R6.cnt END as cnt FROM best_R4, cnt_R6 WHERE cnt_R6.d = best_R4.d)
SELECT b, d, tag FROM best_R6;
CREATE TEMP TABLE node1_R1__R4__R5 AS SELECT R4.a as a, node0_best.b as b, node0_best.d as d FROM R4 JOIN node0_best ON node0_best.d = R4.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R2__R5__R6 AS SELECT node0_best.b as b, R6.c as c, node0_best.d as d FROM R6 JOIN node0_best ON node0_best.d = R6.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT node1_R1__R4__R5.a as a, node1_R1__R4__R5.b as b, node1_R1__R4__R5.d as d, 0 as tag, cnt_R2.cnt as cnt FROM node1_R1__R4__R5, cnt_R2 WHERE cnt_R2.b = node1_R1__R4__R5.b),
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT best_R2.a as a, best_R2.b as b, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R6.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R6.cnt THEN best_R2.cnt ELSE cnt_R6.cnt END as cnt FROM best_R2, cnt_R6 WHERE cnt_R6.d = best_R2.d)
SELECT a, b, d, tag FROM best_R6;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R2__R5__R6 as (SELECT d, COUNT(*) as cnt FROM node3_R2__R5__R6 GROUP BY d),
  best_R2__R5__R6 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R4, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.d = R4.d),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.d as d, CASE WHEN best_R2__R5__R6.cnt < cnt_R1.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R1.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2__R5__R6, cnt_R1 WHERE cnt_R1.a = best_R2__R5__R6.a)
SELECT a, d, tag FROM best_R1;
CREATE TEMP TABLE node5_R1__R4 AS SELECT node3_best.a as a, R1.b as b, node3_best.d as d FROM R1 JOIN node3_best ON node3_best.a = R1.a WHERE node3_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT node3_R2__R5__R6.b as b, node3_R2__R5__R6.c as c, node3_R2__R5__R6.d as d, 0 as tag, cnt_R3.cnt as cnt FROM node3_R2__R5__R6, cnt_R3 WHERE cnt_R3.c = node3_R2__R5__R6.c),
  cnt_R1__R4 as (SELECT b,d, COUNT(*) as cnt FROM node5_R1__R4 GROUP BY b,d),
  best_R1__R4 as (SELECT best_R3.b as b, best_R3.c as c, best_R3.d as d, CASE WHEN best_R3.cnt < cnt_R1__R4.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1__R4.cnt THEN best_R3.cnt ELSE cnt_R1__R4.cnt END as cnt FROM best_R3, cnt_R1__R4 WHERE cnt_R1__R4.b = best_R3.b AND cnt_R1__R4.d = best_R3.d)
SELECT b, c, d, tag FROM best_R1__R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.d as d, R2.c as c FROM R2 JOIN node1_best ON node1_best.b = R2.b SEMI JOIN R6 ON R6.c = R2.c AND R6.d = node1_best.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.d as d, R6.c as c FROM R6 JOIN node1_best ON node1_best.d = R6.d SEMI JOIN R2 ON R2.b = node1_best.b AND R2.c = R6.c WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_R2__R5__R6.b as b, node3_R2__R5__R6.c as c, node3_best.d as d FROM node3_R2__R5__R6 JOIN node3_best ON node3_best.d = node3_R2__R5__R6.d SEMI JOIN R1 ON R1.a = node3_best.a AND R1.b = node3_R2__R5__R6.b WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT R3.a as a, node5_best.b as b, node5_best.d as d, node5_best.c as c FROM R3 JOIN node5_best ON node5_best.c = R3.c WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_R1__R4.a as a, node5_best.b as b, node5_best.d as d, node5_best.c as c FROM node5_R1__R4 JOIN node5_best ON node5_best.b = node5_R1__R4.b AND node5_best.d = node5_R1__R4.d SEMI JOIN R3 ON R3.a = node5_R1__R4.a AND R3.c = node5_best.c WHERE node5_best.tag = 1) node6_R1__R2__R3__R4__R5__R6
);
