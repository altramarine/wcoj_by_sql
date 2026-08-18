CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, a)
-- R2 [R3, R1]
-- [1] R1(a, b), R2__R3(b, c, d), R4(d, a)
-- | R2__R3 [R4, R1]
-- | [2] R1__R2__R3__R4(a, b, c, d) [acyclic]
-- [3] R1__R2(a, b, c), R3(c, d), R4(d, a)
-- | R3 [R1__R2, R4]
-- | [4] R1__R2__R3__R4(a, b, c, d) [acyclic]
-- | [5] R1__R2(a, b, c), R3__R4(c, d, a) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R3.cnt as cnt FROM R2, cnt_R3 WHERE cnt_R3.c = R2.c),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R3.b as b, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.cnt ELSE cnt_R1.cnt END as cnt FROM best_R3, cnt_R1 WHERE cnt_R1.b = best_R3.b)
SELECT b, c, tag FROM best_R1;
CREATE TEMP TABLE node1_R2__R3 AS SELECT node0_best.b as b, node0_best.c as c, R3.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R1__R2 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT node1_R2__R3.b as b, node1_R2__R3.c as c, node1_R2__R3.d as d, 0 as tag, cnt_R4.cnt as cnt FROM node1_R2__R3, cnt_R4 WHERE cnt_R4.d = node1_R2__R3.d),
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT best_R4.b as b, best_R4.c as c, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.cnt ELSE cnt_R1.cnt END as cnt FROM best_R4, cnt_R1 WHERE cnt_R1.b = best_R4.b)
SELECT b, c, d, tag FROM best_R1;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R1__R2 as (SELECT c, COUNT(*) as cnt FROM node3_R1__R2 GROUP BY c),
  best_R1__R2 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R1__R2.cnt as cnt FROM R3, cnt_R1__R2 WHERE cnt_R1__R2.c = R3.c),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R1__R2.c as c, best_R1__R2.d as d, CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1__R2, cnt_R4 WHERE cnt_R4.d = best_R1__R2.d)
SELECT c, d, tag FROM best_R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT R4.a as a, node1_best.b as b, node1_best.c as c, node1_best.d as d FROM R4 JOIN node1_best ON node1_best.d = R4.d SEMI JOIN R1 ON R1.a = R4.a AND R1.b = node1_best.b WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT R1.a as a, node1_best.b as b, node1_best.c as c, node1_best.d as d FROM R1 JOIN node1_best ON node1_best.b = R1.b SEMI JOIN R4 ON R4.d = node1_best.d AND R4.a = R1.a WHERE node1_best.tag = 1) node2_R1__R2__R3__R4
UNION ALL
SELECT node4_R1__R2__R3__R4.a as a, node4_R1__R2__R3__R4.b as b, node4_R1__R2__R3__R4.c as c, node4_R1__R2__R3__R4.d as d FROM (SELECT node3_R1__R2.a as a, node3_R1__R2.b as b, node3_best.c as c, node3_best.d as d FROM node3_R1__R2 JOIN node3_best ON node3_best.c = node3_R1__R2.c SEMI JOIN R4 ON R4.d = node3_best.d AND R4.a = node3_R1__R2.a WHERE node3_best.tag = 0) node4_R1__R2__R3__R4
UNION ALL
SELECT node5_R3__R4.a as a, node3_R1__R2.b as b, node5_R3__R4.c as c, node5_R3__R4.d as d FROM (SELECT node3_best.c as c, node3_best.d as d, R4.a as a FROM R4 JOIN node3_best ON node3_best.d = R4.d WHERE node3_best.tag = 1) node5_R3__R4 JOIN node3_R1__R2 ON node3_R1__R2.a = node5_R3__R4.a AND node3_R1__R2.c = node5_R3__R4.c
);
