CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, d), R4(d, c)
-- R4 [R2, R3]
-- [1] R1(a, b), R2__R4(b, c, d), R3(a, d)
-- | R3 [R2__R4, R1]
-- | [2] R1__R2__R3__R4(a, b, c, d) [acyclic]
-- | [3] R1__R3(a, b, d), R2__R4(b, c, d) [acyclic]
-- [4] R1(a, b), R2(b, c), R3__R4(a, d, c)
-- | R3__R4 [R1, R2]
-- | [5] R1__R2__R3__R4(a, b, d, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT R4.d as d, R4.c as c, 0 as tag, cnt_R2.cnt as cnt FROM R4, cnt_R2 WHERE cnt_R2.c = R4.c),
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT best_R2.d as d, best_R2.c as c, CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2, cnt_R3 WHERE cnt_R3.d = best_R2.d)
SELECT d, c, tag FROM best_R3;
CREATE TEMP TABLE node1_R2__R4 AS SELECT R2.b as b, node0_best.c as c, node0_best.d as d FROM R2 JOIN node0_best ON node0_best.c = R2.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R3__R4 AS SELECT R3.a as a, node0_best.d as d, node0_best.c as c FROM R3 JOIN node0_best ON node0_best.d = R3.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2__R4 as (SELECT d, COUNT(*) as cnt FROM node1_R2__R4 GROUP BY d),
  best_R2__R4 as (SELECT R3.a as a, R3.d as d, 0 as tag, cnt_R2__R4.cnt as cnt FROM R3, cnt_R2__R4 WHERE cnt_R2__R4.d = R3.d),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R2__R4.a as a, best_R2__R4.d as d, CASE WHEN best_R2__R4.cnt < cnt_R1.cnt THEN best_R2__R4.tag ELSE 1 END as tag,CASE WHEN best_R2__R4.cnt < cnt_R1.cnt THEN best_R2__R4.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2__R4, cnt_R1 WHERE cnt_R1.a = best_R2__R4.a)
SELECT a, d, tag FROM best_R1;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT node4_R3__R4.a as a, node4_R3__R4.d as d, node4_R3__R4.c as c, 0 as tag, cnt_R1.cnt as cnt FROM node4_R3__R4, cnt_R1 WHERE cnt_R1.a = node4_R3__R4.a),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R1.a as a, best_R1.d as d, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R2.cnt THEN best_R1.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1, cnt_R2 WHERE cnt_R2.c = best_R1.c)
SELECT a, d, c, tag FROM best_R2;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_best.a as a, node1_R2__R4.b as b, node1_R2__R4.c as c, node1_best.d as d FROM node1_R2__R4 JOIN node1_best ON node1_best.d = node1_R2__R4.d SEMI JOIN R1 ON R1.a = node1_best.a AND R1.b = node1_R2__R4.b WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node3_R1__R3.a as a, node3_R1__R3.b as b, node1_R2__R4.c as c, node3_R1__R3.d as d FROM (SELECT node1_best.a as a, R1.b as b, node1_best.d as d FROM R1 JOIN node1_best ON node1_best.a = R1.a WHERE node1_best.tag = 1) node3_R1__R3 JOIN node1_R2__R4 ON node1_R2__R4.b = node3_R1__R3.b AND node1_R2__R4.d = node3_R1__R3.d
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT node4_best.a as a, R1.b as b, node4_best.d as d, node4_best.c as c FROM R1 JOIN node4_best ON node4_best.a = R1.a SEMI JOIN R2 ON R2.b = R1.b AND R2.c = node4_best.c WHERE node4_best.tag = 0) node5_R1__R2__R3__R4
UNION ALL
SELECT node5_R1__R2__R3__R4.a as a, node5_R1__R2__R3__R4.b as b, node5_R1__R2__R3__R4.c as c, node5_R1__R2__R3__R4.d as d FROM (SELECT node4_best.a as a, R2.b as b, node4_best.d as d, node4_best.c as c FROM R2 JOIN node4_best ON node4_best.c = R2.c SEMI JOIN R1 ON R1.a = node4_best.a AND R1.b = R2.b WHERE node4_best.tag = 1) node5_R1__R2__R3__R4
);
