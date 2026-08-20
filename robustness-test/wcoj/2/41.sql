CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, d), R4(d, c)
-- R1 [R3, R2]
-- [1] R1__R3(a, b, d), R2(b, c), R4(d, c)
-- | R1__R3 [R4, R2]
-- | [2] R1__R2__R3__R4(a, b, d, c) [acyclic]
-- [3] R1__R2(a, b, c), R3(a, d), R4(d, c)
-- | R4 [R3, R1__R2]
-- | [4] R1__R2(a, b, c), R3__R4(a, d, c) [acyclic]
-- | [5] R1__R2__R3__R4(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R3.cnt as cnt FROM R1, cnt_R3 WHERE cnt_R3.a = R1.a),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R3.a as a, best_R3.b as b, CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.cnt ELSE cnt_R2.cnt END as cnt FROM best_R3, cnt_R2 WHERE cnt_R2.b = best_R3.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node1_R1__R3 AS SELECT node0_best.a as a, node0_best.b as b, R3.d as d FROM R3 JOIN node0_best ON node0_best.a = R3.a WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R1__R2 AS SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM R2 JOIN node0_best ON node0_best.b = R2.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT node1_R1__R3.a as a, node1_R1__R3.b as b, node1_R1__R3.d as d, 0 as tag, cnt_R4.cnt as cnt FROM node1_R1__R3, cnt_R4 WHERE cnt_R4.d = node1_R1__R3.d),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R4.a as a, best_R4.b as b, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R2.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R2.cnt THEN best_R4.cnt ELSE cnt_R2.cnt END as cnt FROM best_R4, cnt_R2 WHERE cnt_R2.b = best_R4.b)
SELECT a, b, d, tag FROM best_R2;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT R4.d as d, R4.c as c, 0 as tag, cnt_R3.cnt as cnt FROM R4, cnt_R3 WHERE cnt_R3.d = R4.d),
  cnt_R1__R2 as (SELECT c, COUNT(*) as cnt FROM node3_R1__R2 GROUP BY c),
  best_R1__R2 as (SELECT best_R3.d as d, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R1__R2.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1__R2.cnt THEN best_R3.cnt ELSE cnt_R1__R2.cnt END as cnt FROM best_R3, cnt_R1__R2 WHERE cnt_R1__R2.c = best_R3.c)
SELECT d, c, tag FROM best_R1__R2;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.d as d, R4.c as c FROM R4 JOIN node1_best ON node1_best.d = R4.d SEMI JOIN R2 ON R2.b = node1_best.b AND R2.c = R4.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.d as d, R2.c as c FROM R2 JOIN node1_best ON node1_best.b = R2.b SEMI JOIN R4 ON R4.d = node1_best.d AND R4.c = R2.c WHERE node1_best.tag = 1) node2_R1__R2__R3__R4
UNION ALL
SELECT node4_R3__R4.a as a, node3_R1__R2.b as b, node4_R3__R4.c as c, node4_R3__R4.d as d FROM (SELECT R3.a as a, node3_best.d as d, node3_best.c as c FROM R3 JOIN node3_best ON node3_best.d = R3.d WHERE node3_best.tag = 0) node4_R3__R4 JOIN node3_R1__R2 ON node3_R1__R2.a = node4_R3__R4.a AND node3_R1__R2.c = node4_R3__R4.c
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT node3_R1__R2.a as a, node3_R1__R2.b as b, node3_best.c as c, node3_best.d as d FROM node3_R1__R2 JOIN node3_best ON node3_best.c = node3_R1__R2.c SEMI JOIN R3 ON R3.a = node3_R1__R2.a AND R3.d = node3_best.d WHERE node3_best.tag = 1) node5_R1__R2__R3__R4
);
