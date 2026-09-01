CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R3 [R2, R5]
-- [1] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R1__R2__R3 [R4, R5]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- [3] R1(a, b), R2(b, c), R3__R5(a, c, d), R4(b, d)
-- | R1 [R3__R5, R4]
-- | [4] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [5] R1__R4(a, b, d), R2(b, c), R3__R5(a, c, d)
-- | | R2 [R3__R5, R1__R4]
-- | | [6] R1__R2__R3__R4__R5(a, b, d, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R2.cnt as cnt FROM R3, cnt_R2 WHERE cnt_R2.c = R3.c),
  cnt_R5 as (SELECT c, COUNT(*) as cnt FROM R5 GROUP BY c),
  best_R5 as (SELECT best_R2.a as a, best_R2.c as c, CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.cnt ELSE cnt_R5.cnt END as cnt FROM best_R2, cnt_R5 WHERE cnt_R5.c = best_R2.c)
SELECT a, c, tag FROM best_R5;
CREATE TEMP TABLE node1_R1__R2__R3 AS SELECT node0_best.a as a, R2.b as b, node0_best.c as c FROM R2 JOIN node0_best ON node0_best.c = R2.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R3__R5 AS SELECT node0_best.a as a, node0_best.c as c, R5.d as d FROM R5 JOIN node0_best ON node0_best.c = R5.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT node1_R1__R2__R3.a as a, node1_R1__R2__R3.b as b, node1_R1__R2__R3.c as c, 0 as tag, cnt_R4.cnt as cnt FROM node1_R1__R2__R3, cnt_R4 WHERE cnt_R4.b = node1_R1__R2__R3.b),
  cnt_R5 as (SELECT c, COUNT(*) as cnt FROM R5 GROUP BY c),
  best_R5 as (SELECT best_R4.a as a, best_R4.b as b, best_R4.c as c, CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.cnt ELSE cnt_R5.cnt END as cnt FROM best_R4, cnt_R5 WHERE cnt_R5.c = best_R4.c)
SELECT a, b, c, tag FROM best_R5;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R3__R5 as (SELECT a, COUNT(*) as cnt FROM node3_R3__R5 GROUP BY a),
  best_R3__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R3__R5.cnt as cnt FROM R1, cnt_R3__R5 WHERE cnt_R3__R5.a = R1.a),
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT best_R3__R5.a as a, best_R3__R5.b as b, CASE WHEN best_R3__R5.cnt < cnt_R4.cnt THEN best_R3__R5.tag ELSE 1 END as tag,CASE WHEN best_R3__R5.cnt < cnt_R4.cnt THEN best_R3__R5.cnt ELSE cnt_R4.cnt END as cnt FROM best_R3__R5, cnt_R4 WHERE cnt_R4.b = best_R3__R5.b)
SELECT a, b, tag FROM best_R4;
CREATE TEMP TABLE node5_R1__R4 AS SELECT node3_best.a as a, node3_best.b as b, R4.d as d FROM R4 JOIN node3_best ON node3_best.b = R4.b WHERE node3_best.tag = 1;
CREATE TEMP TABLE node5_best AS WITH
  cnt_R3__R5 as (SELECT c, COUNT(*) as cnt FROM node3_R3__R5 GROUP BY c),
  best_R3__R5 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R3__R5.cnt as cnt FROM R2, cnt_R3__R5 WHERE cnt_R3__R5.c = R2.c),
  cnt_R1__R4 as (SELECT b, COUNT(*) as cnt FROM node5_R1__R4 GROUP BY b),
  best_R1__R4 as (SELECT best_R3__R5.b as b, best_R3__R5.c as c, CASE WHEN best_R3__R5.cnt < cnt_R1__R4.cnt THEN best_R3__R5.tag ELSE 1 END as tag,CASE WHEN best_R3__R5.cnt < cnt_R1__R4.cnt THEN best_R3__R5.cnt ELSE cnt_R1__R4.cnt END as cnt FROM best_R3__R5, cnt_R1__R4 WHERE cnt_R1__R4.b = best_R3__R5.b)
SELECT b, c, tag FROM best_R1__R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R4.d as d FROM R4 JOIN node1_best ON node1_best.b = R4.b SEMI JOIN R5 ON R5.c = node1_best.c AND R5.d = R4.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R5.d as d FROM R5 JOIN node1_best ON node1_best.c = R5.c SEMI JOIN R4 ON R4.b = node1_best.b AND R4.d = R5.d WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node4_R1__R2__R3__R4__R5.a as a, node4_R1__R2__R3__R4__R5.b as b, node4_R1__R2__R3__R4__R5.c as c, node4_R1__R2__R3__R4__R5.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node3_R3__R5.c as c, node3_R3__R5.d as d FROM node3_R3__R5 JOIN node3_best ON node3_best.a = node3_R3__R5.a SEMI JOIN R4 ON R4.b = node3_best.b AND R4.d = node3_R3__R5.d WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5
UNION ALL
SELECT node6_R1__R2__R3__R4__R5.a as a, node6_R1__R2__R3__R4__R5.b as b, node6_R1__R2__R3__R4__R5.c as c, node6_R1__R2__R3__R4__R5.d as d FROM (SELECT node3_R3__R5.a as a, node5_best.b as b, node3_R3__R5.d as d, node5_best.c as c FROM node3_R3__R5 JOIN node5_best ON node5_best.c = node3_R3__R5.c WHERE node5_best.tag = 0) node6_R1__R2__R3__R4__R5
UNION ALL
SELECT node6_R1__R2__R3__R4__R5.a as a, node6_R1__R2__R3__R4__R5.b as b, node6_R1__R2__R3__R4__R5.c as c, node6_R1__R2__R3__R4__R5.d as d FROM (SELECT node5_R1__R4.a as a, node5_best.b as b, node5_R1__R4.d as d, node5_best.c as c FROM node5_R1__R4 JOIN node5_best ON node5_best.b = node5_R1__R4.b WHERE node5_best.tag = 1) node6_R1__R2__R3__R4__R5
);
