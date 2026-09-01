CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R5 [R2, R3]
-- [1] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R2__R4__R5 [R1, R3]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- [3] R1(a, b), R2(b, c), R3__R5(a, c, d), R4(b, d)
-- | R4 [R2, R3__R5]
-- | [4] R1(a, b), R2__R4(b, c, d), R3__R5(a, c, d)
-- | | R2__R4 [R1, R3__R5]
-- | | [5] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [6] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT R5.c as c, R5.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R5, cnt_R2 WHERE cnt_R2.c = R5.c),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2, cnt_R3 WHERE cnt_R3.c = best_R2.c)
SELECT c, d, tag FROM best_R3;
CREATE TEMP TABLE node1_R2__R4__R5 AS SELECT R2.b as b, node0_best.c as c, node0_best.d as d FROM R2 JOIN node0_best ON node0_best.c = R2.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R3__R5 AS SELECT R3.a as a, node0_best.c as c, node0_best.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT node1_R2__R4__R5.b as b, node1_R2__R4__R5.c as c, node1_R2__R4__R5.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node1_R2__R4__R5, cnt_R1 WHERE cnt_R1.b = node1_R2__R4__R5.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1.b as b, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.c = best_R1.c)
SELECT b, c, d, tag FROM best_R3;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R4, cnt_R2 WHERE cnt_R2.b = R4.b),
  cnt_R3__R5 as (SELECT d, COUNT(*) as cnt FROM node3_R3__R5 GROUP BY d),
  best_R3__R5 as (SELECT best_R2.b as b, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R3__R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3__R5.cnt THEN best_R2.cnt ELSE cnt_R3__R5.cnt END as cnt FROM best_R2, cnt_R3__R5 WHERE cnt_R3__R5.d = best_R2.d)
SELECT b, d, tag FROM best_R3__R5;
CREATE TEMP TABLE node4_R2__R4 AS SELECT node3_best.b as b, R2.c as c, node3_best.d as d FROM R2 JOIN node3_best ON node3_best.b = R2.b WHERE node3_best.tag = 0;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT node4_R2__R4.b as b, node4_R2__R4.c as c, node4_R2__R4.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node4_R2__R4, cnt_R1 WHERE cnt_R1.b = node4_R2__R4.b),
  cnt_R3__R5 as (SELECT c,d, COUNT(*) as cnt FROM node3_R3__R5 GROUP BY c,d),
  best_R3__R5 as (SELECT best_R1.b as b, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R3__R5.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3__R5.cnt THEN best_R1.cnt ELSE cnt_R3__R5.cnt END as cnt FROM best_R1, cnt_R3__R5 WHERE cnt_R3__R5.c = best_R1.c AND cnt_R3__R5.d = best_R1.d)
SELECT b, c, d, tag FROM best_R3__R5;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT R1.a as a, node1_best.b as b, node1_best.c as c, node1_best.d as d FROM R1 JOIN node1_best ON node1_best.b = R1.b SEMI JOIN R3 ON R3.a = R1.a AND R3.c = node1_best.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT R3.a as a, node1_best.b as b, node1_best.c as c, node1_best.d as d FROM R3 JOIN node1_best ON node1_best.c = R3.c SEMI JOIN R1 ON R1.a = R3.a AND R1.b = node1_best.b WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node6_R1__R2__R3__R4__R5.a as a, node6_R1__R2__R3__R4__R5.b as b, node6_R1__R2__R3__R4__R5.c as c, node6_R1__R2__R3__R4__R5.d as d FROM (SELECT node3_R3__R5.a as a, node3_best.b as b, node3_R3__R5.c as c, node3_best.d as d FROM node3_R3__R5 JOIN node3_best ON node3_best.d = node3_R3__R5.d SEMI JOIN R2 ON R2.b = node3_best.b AND R2.c = node3_R3__R5.c WHERE node3_best.tag = 1) node6_R1__R2__R3__R4__R5
UNION ALL
SELECT node5_R1__R2__R3__R4__R5.a as a, node5_R1__R2__R3__R4__R5.b as b, node5_R1__R2__R3__R4__R5.c as c, node5_R1__R2__R3__R4__R5.d as d FROM (SELECT R1.a as a, node4_best.b as b, node4_best.c as c, node4_best.d as d FROM R1 JOIN node4_best ON node4_best.b = R1.b WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5
UNION ALL
SELECT node5_R1__R2__R3__R4__R5.a as a, node5_R1__R2__R3__R4__R5.b as b, node5_R1__R2__R3__R4__R5.c as c, node5_R1__R2__R3__R4__R5.d as d FROM (SELECT node3_R3__R5.a as a, node4_best.b as b, node4_best.c as c, node4_best.d as d FROM node3_R3__R5 JOIN node4_best ON node4_best.c = node3_R3__R5.c AND node4_best.d = node3_R3__R5.d SEMI JOIN R1 ON R1.a = node3_R3__R5.a AND R1.b = node4_best.b WHERE node4_best.tag = 1) node5_R1__R2__R3__R4__R5
);
