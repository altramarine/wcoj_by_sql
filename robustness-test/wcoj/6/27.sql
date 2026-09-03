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
-- | R3 [R2__R5__R6, R1]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [3] R1__R3(a, b, c), R2__R5__R6(b, c, d), R4(a, d)
-- | | R1__R3 [R2__R5__R6, R4]
-- | | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- [5] R1(a, b), R2(b, c), R3__R4__R6(a, c, d), R5(b, d)
-- | R3__R4__R6 [R2, R5]
-- | [6] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT R6.c as c, R6.d as d, 0 as tag, cnt_R5.cnt as cnt FROM R6, cnt_R5 WHERE cnt_R5.d = R6.d),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R5.c as c, best_R5.d as d, CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.cnt ELSE cnt_R4.cnt END as cnt FROM best_R5, cnt_R4 WHERE cnt_R4.d = best_R5.d)
SELECT c, d, tag FROM best_R4;
CREATE TEMP TABLE node1_R2__R5__R6 AS SELECT R5.b as b, node0_best.c as c, node0_best.d as d FROM R5 JOIN node0_best ON node0_best.d = R5.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node5_R3__R4__R6 AS SELECT R4.a as a, node0_best.c as c, node0_best.d as d FROM R4 JOIN node0_best ON node0_best.d = R4.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2__R5__R6 as (SELECT c, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY c),
  best_R2__R5__R6 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R3, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.c = R3.c),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.c as c, CASE WHEN best_R2__R5__R6.cnt < cnt_R1.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R1.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2__R5__R6, cnt_R1 WHERE cnt_R1.a = best_R2__R5__R6.a)
SELECT a, c, tag FROM best_R1;
CREATE TEMP TABLE node3_R1__R3 AS SELECT node1_best.a as a, R1.b as b, node1_best.c as c FROM R1 JOIN node1_best ON node1_best.a = R1.a WHERE node1_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT node5_R3__R4__R6.a as a, node5_R3__R4__R6.c as c, node5_R3__R4__R6.d as d, 0 as tag, cnt_R2.cnt as cnt FROM node5_R3__R4__R6, cnt_R2 WHERE cnt_R2.c = node5_R3__R4__R6.c),
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT best_R2.a as a, best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.cnt ELSE cnt_R5.cnt END as cnt FROM best_R2, cnt_R5 WHERE cnt_R5.d = best_R2.d)
SELECT a, c, d, tag FROM best_R5;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R2__R5__R6 as (SELECT b,c, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY b,c),
  best_R2__R5__R6 as (SELECT node3_R1__R3.a as a, node3_R1__R3.b as b, node3_R1__R3.c as c, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM node3_R1__R3, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.b = node3_R1__R3.b AND cnt_R2__R5__R6.c = node3_R1__R3.c),
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.b as b, best_R2__R5__R6.c as c, CASE WHEN best_R2__R5__R6.cnt < cnt_R4.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R4.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R4.cnt END as cnt FROM best_R2__R5__R6, cnt_R4 WHERE cnt_R4.a = best_R2__R5__R6.a)
SELECT a, b, c, tag FROM best_R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_R2__R5__R6.b as b, node1_best.c as c, node1_R2__R5__R6.d as d FROM node1_R2__R5__R6 JOIN node1_best ON node1_best.c = node1_R2__R5__R6.c SEMI JOIN R1 ON R1.a = node1_best.a AND R1.b = node1_R2__R5__R6.b WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, R2.b as b, node5_best.c as c, node5_best.d as d FROM R2 JOIN node5_best ON node5_best.c = R2.c SEMI JOIN R5 ON R5.b = R2.b AND R5.d = node5_best.d WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node6_R1__R2__R3__R4__R5__R6.a as a, node6_R1__R2__R3__R4__R5__R6.b as b, node6_R1__R2__R3__R4__R5__R6.c as c, node6_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node5_best.a as a, R5.b as b, node5_best.c as c, node5_best.d as d FROM R5 JOIN node5_best ON node5_best.d = R5.d SEMI JOIN R2 ON R2.b = R5.b AND R2.c = node5_best.c WHERE node5_best.tag = 1) node6_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node3_best.c as c, node1_R2__R5__R6.d as d FROM node1_R2__R5__R6 JOIN node3_best ON node3_best.b = node1_R2__R5__R6.b AND node3_best.c = node1_R2__R5__R6.c SEMI JOIN R4 ON R4.a = node3_best.a AND R4.d = node1_R2__R5__R6.d WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node3_best.c as c, R4.d as d FROM R4 JOIN node3_best ON node3_best.a = R4.a WHERE node3_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
);
