CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, a)
-- R2 [R1, R3]
-- [1] R1__R2(a, b, c), R3(c, d), R4(d, a)
-- | R3 [R4, R1__R2]
-- | [2] R1__R2(a, b, c), R3__R4(c, d, a) [acyclic]
-- | [3] R1__R2__R3__R4(a, b, c, d) [acyclic]
-- [4] R1(a, b), R2__R3(b, c, d), R4(d, a)
-- | R2__R3 [R1, R4]
-- | [5] R1__R2__R3__R4(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.c = best_R1.c)
SELECT b, c, tag FROM best_R3;
CREATE TEMP TABLE node1_R1__R2 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R2__R3 AS SELECT node0_best.b as b, node0_best.c as c, R3.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R4.cnt as cnt FROM R3, cnt_R4 WHERE cnt_R4.d = R3.d),
  cnt_R1__R2 as (SELECT c, COUNT(*) as cnt FROM node1_R1__R2 GROUP BY c),
  best_R1__R2 as (SELECT best_R4.c as c, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R1__R2.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1__R2.cnt THEN best_R4.cnt ELSE cnt_R1__R2.cnt END as cnt FROM best_R4, cnt_R1__R2 WHERE cnt_R1__R2.c = best_R4.c)
SELECT c, d, tag FROM best_R1__R2;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT node4_R2__R3.b as b, node4_R2__R3.c as c, node4_R2__R3.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node4_R2__R3, cnt_R1 WHERE cnt_R1.b = node4_R2__R3.b),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R1.b as b, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R4.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R4.cnt THEN best_R1.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1, cnt_R4 WHERE cnt_R4.d = best_R1.d)
SELECT b, c, d, tag FROM best_R4;
SELECT COUNT(*) FROM (
SELECT node2_R3__R4.a as a, node1_R1__R2.b as b, node2_R3__R4.c as c, node2_R3__R4.d as d FROM (SELECT node1_best.c as c, node1_best.d as d, R4.a as a FROM R4 JOIN node1_best ON node1_best.d = R4.d WHERE node1_best.tag = 0) node2_R3__R4 JOIN node1_R1__R2 ON node1_R1__R2.a = node2_R3__R4.a AND node1_R1__R2.c = node2_R3__R4.c
UNION ALL
SELECT node3_R1__R2__R3__R4.a as a, node3_R1__R2__R3__R4.b as b, node3_R1__R2__R3__R4.c as c, node3_R1__R2__R3__R4.d as d FROM (SELECT node1_R1__R2.a as a, node1_R1__R2.b as b, node1_best.c as c, node1_best.d as d FROM node1_R1__R2 JOIN node1_best ON node1_best.c = node1_R1__R2.c SEMI JOIN R4 ON R4.d = node1_best.d AND R4.a = node1_R1__R2.a WHERE node1_best.tag = 1) node3_R1__R2__R3__R4
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT R1.a as a, node4_best.b as b, node4_best.c as c, node4_best.d as d FROM R1 JOIN node4_best ON node4_best.b = R1.b SEMI JOIN R4 ON R4.d = node4_best.d AND R4.a = R1.a WHERE node4_best.tag = 0) node5_R1__R2__R3__R4
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT R4.a as a, node4_best.b as b, node4_best.c as c, node4_best.d as d FROM R4 JOIN node4_best ON node4_best.d = R4.d SEMI JOIN R1 ON R1.a = R4.a AND R1.b = node4_best.b WHERE node4_best.tag = 1) node5_R1__R2__R3__R4
);
