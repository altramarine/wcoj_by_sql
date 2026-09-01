CREATE VIEW R1 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R2 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(c, d), R2(a, d), R3(a, b), R4(b, c)
-- R2 [R1, R3]
-- [1] R1__R2(c, d, a), R3(a, b), R4(b, c)
-- | R3 [R1__R2, R4]
-- | [2] R1__R2__R3__R4(c, d, a, b) [acyclic]
-- | [3] R1__R2(c, d, a), R3__R4(a, b, c) [acyclic]
-- [4] R1(c, d), R2__R3(a, d, b), R4(b, c)
-- | R2__R3 [R4, R1]
-- | [5] R1__R2__R3__R4(c, d, a, b) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT d, COUNT(*) as cnt FROM R1 GROUP BY d),
  best_R1 as (SELECT R2.a as a, R2.d as d, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.d = R2.d),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R1.a as a, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.a = best_R1.a)
SELECT a, d, tag FROM best_R3;
CREATE TEMP TABLE node1_R1__R2 AS SELECT R1.c as c, node0_best.d as d, node0_best.a as a FROM R1 JOIN node0_best ON node0_best.d = R1.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R2__R3 AS SELECT node0_best.a as a, node0_best.d as d, R3.b as b FROM R3 JOIN node0_best ON node0_best.a = R3.a WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R2 as (SELECT a, COUNT(*) as cnt FROM node1_R1__R2 GROUP BY a),
  best_R1__R2 as (SELECT R3.a as a, R3.b as b, 0 as tag, cnt_R1__R2.cnt as cnt FROM R3, cnt_R1__R2 WHERE cnt_R1__R2.a = R3.a),
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT best_R1__R2.a as a, best_R1__R2.b as b, CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1__R2, cnt_R4 WHERE cnt_R4.b = best_R1__R2.b)
SELECT a, b, tag FROM best_R4;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT node4_R2__R3.a as a, node4_R2__R3.d as d, node4_R2__R3.b as b, 0 as tag, cnt_R4.cnt as cnt FROM node4_R2__R3, cnt_R4 WHERE cnt_R4.b = node4_R2__R3.b),
  cnt_R1 as (SELECT d, COUNT(*) as cnt FROM R1 GROUP BY d),
  best_R1 as (SELECT best_R4.a as a, best_R4.d as d, best_R4.b as b, CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.cnt ELSE cnt_R1.cnt END as cnt FROM best_R4, cnt_R1 WHERE cnt_R1.d = best_R4.d)
SELECT a, d, b, tag FROM best_R1;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_R1__R2.c as c, node1_R1__R2.d as d, node1_best.a as a, node1_best.b as b FROM node1_R1__R2 JOIN node1_best ON node1_best.a = node1_R1__R2.a SEMI JOIN R4 ON R4.b = node1_best.b AND R4.c = node1_R1__R2.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node3_R3__R4.a as a, node3_R3__R4.b as b, node3_R3__R4.c as c, node1_R1__R2.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, R4.c as c FROM R4 JOIN node1_best ON node1_best.b = R4.b WHERE node1_best.tag = 1) node3_R3__R4 JOIN node1_R1__R2 ON node1_R1__R2.c = node3_R3__R4.c AND node1_R1__R2.a = node3_R3__R4.a
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT R4.c as c, node4_best.d as d, node4_best.a as a, node4_best.b as b FROM R4 JOIN node4_best ON node4_best.b = R4.b SEMI JOIN R1 ON R1.c = R4.c AND R1.d = node4_best.d WHERE node4_best.tag = 0) node5_R1__R2__R3__R4
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT R1.c as c, node4_best.d as d, node4_best.a as a, node4_best.b as b FROM R1 JOIN node4_best ON node4_best.d = R1.d SEMI JOIN R4 ON R4.b = node4_best.b AND R4.c = R1.c WHERE node4_best.tag = 1) node5_R1__R2__R3__R4
);
