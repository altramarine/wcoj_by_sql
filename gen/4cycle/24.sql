CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, a)
-- R4 [R3, R1]
-- [1] R1(a, b), R2(b, c), R3__R4(c, d, a)
-- | R1 [R3__R4, R2]
-- | [2] R1__R2__R3__R4(a, b, c, d) [acyclic]
-- | [3] R1__R2(a, b, c), R3__R4(c, d, a) [acyclic]
-- [4] R1__R4(a, b, d), R2(b, c), R3(c, d)
-- | R3 [R2, R1__R4]
-- | [5] R1__R4(a, b, d), R2__R3(b, c, d) [acyclic]
-- | [6] R1__R2__R3__R4(a, b, d, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT R4.d as d, R4.a as a, 0 as tag, cnt_R3.cnt as cnt FROM R4, cnt_R3 WHERE cnt_R3.d = R4.d),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R3.d as d, best_R3.a as a, CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R1.cnt THEN best_R3.cnt ELSE cnt_R1.cnt END as cnt FROM best_R3, cnt_R1 WHERE cnt_R1.a = best_R3.a)
SELECT d, a, tag FROM best_R1;
CREATE TEMP TABLE node1_R3__R4 AS SELECT R3.c as c, node0_best.d as d, node0_best.a as a FROM R3 JOIN node0_best ON node0_best.d = R3.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R1__R4 AS SELECT node0_best.a as a, R1.b as b, node0_best.d as d FROM R1 JOIN node0_best ON node0_best.a = R1.a WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R3__R4 as (SELECT a, COUNT(*) as cnt FROM node1_R3__R4 GROUP BY a),
  best_R3__R4 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R3__R4.cnt as cnt FROM R1, cnt_R3__R4 WHERE cnt_R3__R4.a = R1.a),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R3__R4.a as a, best_R3__R4.b as b, CASE WHEN best_R3__R4.cnt < cnt_R2.cnt THEN best_R3__R4.tag ELSE 1 END as tag,CASE WHEN best_R3__R4.cnt < cnt_R2.cnt THEN best_R3__R4.cnt ELSE cnt_R2.cnt END as cnt FROM best_R3__R4, cnt_R2 WHERE cnt_R2.b = best_R3__R4.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R3, cnt_R2 WHERE cnt_R2.c = R3.c),
  cnt_R1__R4 as (SELECT d, COUNT(*) as cnt FROM node4_R1__R4 GROUP BY d),
  best_R1__R4 as (SELECT best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R1__R4.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1__R4.cnt THEN best_R2.cnt ELSE cnt_R1__R4.cnt END as cnt FROM best_R2, cnt_R1__R4 WHERE cnt_R1__R4.d = best_R2.d)
SELECT c, d, tag FROM best_R1__R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_R3__R4.c as c, node1_R3__R4.d as d FROM node1_R3__R4 JOIN node1_best ON node1_best.a = node1_R3__R4.a SEMI JOIN R2 ON R2.b = node1_best.b AND R2.c = node1_R3__R4.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node3_R1__R2.a as a, node3_R1__R2.b as b, node3_R1__R2.c as c, node1_R3__R4.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, R2.c as c FROM R2 JOIN node1_best ON node1_best.b = R2.b WHERE node1_best.tag = 1) node3_R1__R2 JOIN node1_R3__R4 ON node1_R3__R4.c = node3_R1__R2.c AND node1_R3__R4.a = node3_R1__R2.a
UNION ALL
SELECT node4_R1__R4.a as a, node5_R2__R3.b as b, node5_R2__R3.c as c, node5_R2__R3.d as d FROM (SELECT R2.b as b, node4_best.c as c, node4_best.d as d FROM R2 JOIN node4_best ON node4_best.c = R2.c WHERE node4_best.tag = 0) node5_R2__R3 JOIN node4_R1__R4 ON node4_R1__R4.b = node5_R2__R3.b AND node4_R1__R4.d = node5_R2__R3.d
UNION ALL
SELECT node6_R1__R2__R3__R4.a as a, node6_R1__R2__R3__R4.b as b, node6_R1__R2__R3__R4.c as c, node6_R1__R2__R3__R4.d as d FROM (SELECT node4_R1__R4.a as a, node4_R1__R4.b as b, node4_best.d as d, node4_best.c as c FROM node4_R1__R4 JOIN node4_best ON node4_best.d = node4_R1__R4.d SEMI JOIN R2 ON R2.b = node4_R1__R4.b AND R2.c = node4_best.c WHERE node4_best.tag = 1) node6_R1__R2__R3__R4
);
