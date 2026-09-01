CREATE VIEW R1 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R2 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(c, d), R2(a, d), R3(a, b), R4(b, c)
-- R2 [R1, R3]
-- [1] R1__R2(c, d, a), R3(a, b), R4(b, c)
-- | R1__R2 [R4, R3]
-- | [2] R1__R2__R3__R4(c, d, a, b) [acyclic]
-- [3] R1(c, d), R2__R3(a, d, b), R4(b, c)
-- | R1 [R4, R2__R3]
-- | [4] R1__R4(c, d, b), R2__R3(a, d, b) [acyclic]
-- | [5] R1__R2__R3__R4(c, d, a, b) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT d, COUNT(*) as cnt FROM R1 GROUP BY d),
  best_R1 as (SELECT R2.a as a, R2.d as d, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.d = R2.d),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R1.a as a, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.a = best_R1.a)
SELECT a, d, tag FROM best_R3;
CREATE TEMP TABLE node1_R1__R2 AS SELECT R1.c as c, node0_best.d as d, node0_best.a as a FROM R1 JOIN node0_best ON node0_best.d = R1.d WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R2__R3 AS SELECT node0_best.a as a, node0_best.d as d, R3.b as b FROM R3 JOIN node0_best ON node0_best.a = R3.a WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT c, COUNT(*) as cnt FROM R4 GROUP BY c),
  best_R4 as (SELECT node1_R1__R2.c as c, node1_R1__R2.d as d, node1_R1__R2.a as a, 0 as tag, cnt_R4.cnt as cnt FROM node1_R1__R2, cnt_R4 WHERE cnt_R4.c = node1_R1__R2.c),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R4.c as c, best_R4.d as d, best_R4.a as a, CASE WHEN best_R4.cnt < cnt_R3.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R3.cnt THEN best_R4.cnt ELSE cnt_R3.cnt END as cnt FROM best_R4, cnt_R3 WHERE cnt_R3.a = best_R4.a)
SELECT c, d, a, tag FROM best_R3;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R4 as (SELECT c, COUNT(*) as cnt FROM R4 GROUP BY c),
  best_R4 as (SELECT R1.c as c, R1.d as d, 0 as tag, cnt_R4.cnt as cnt FROM R1, cnt_R4 WHERE cnt_R4.c = R1.c),
  cnt_R2__R3 as (SELECT d, COUNT(*) as cnt FROM node3_R2__R3 GROUP BY d),
  best_R2__R3 as (SELECT best_R4.c as c, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R2__R3.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R2__R3.cnt THEN best_R4.cnt ELSE cnt_R2__R3.cnt END as cnt FROM best_R4, cnt_R2__R3 WHERE cnt_R2__R3.d = best_R4.d)
SELECT c, d, tag FROM best_R2__R3;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_best.c as c, node1_best.d as d, node1_best.a as a, R4.b as b FROM R4 JOIN node1_best ON node1_best.c = R4.c SEMI JOIN R3 ON R3.a = node1_best.a AND R3.b = R4.b WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_best.c as c, node1_best.d as d, node1_best.a as a, R3.b as b FROM R3 JOIN node1_best ON node1_best.a = R3.a SEMI JOIN R4 ON R4.b = R3.b AND R4.c = node1_best.c WHERE node1_best.tag = 1) node2_R1__R2__R3__R4
UNION ALL
SELECT node3_R2__R3.a as a, node4_R1__R4.b as b, node4_R1__R4.c as c, node4_R1__R4.d as d FROM (SELECT node3_best.c as c, node3_best.d as d, R4.b as b FROM R4 JOIN node3_best ON node3_best.c = R4.c WHERE node3_best.tag = 0) node4_R1__R4 JOIN node3_R2__R3 ON node3_R2__R3.d = node4_R1__R4.d AND node3_R2__R3.b = node4_R1__R4.b
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT node3_best.c as c, node3_best.d as d, node3_R2__R3.a as a, node3_R2__R3.b as b FROM node3_R2__R3 JOIN node3_best ON node3_best.d = node3_R2__R3.d SEMI JOIN R4 ON R4.b = node3_R2__R3.b AND R4.c = node3_best.c WHERE node3_best.tag = 1) node5_R1__R2__R3__R4
);
