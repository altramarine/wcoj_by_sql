CREATE VIEW R1 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R2 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(c, d), R2(a, d), R3(a, b), R4(b, c)
-- R3 [R2, R4]
-- [1] R1(c, d), R2__R3(a, d, b), R4(b, c)
-- | R2__R3 [R4, R1]
-- | [2] R1__R2__R3__R4(c, d, a, b) [acyclic]
-- [3] R1(c, d), R2(a, d), R3__R4(a, b, c)
-- | R3__R4 [R2, R1]
-- | [4] R1__R2__R3__R4(c, d, a, b) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT a, COUNT(*) as cnt FROM R2 GROUP BY a),
  best_R2 as (SELECT R3.a as a, R3.b as b, 0 as tag, cnt_R2.cnt as cnt FROM R3, cnt_R2 WHERE cnt_R2.a = R3.a),
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT best_R2.a as a, best_R2.b as b, CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R2, cnt_R4 WHERE cnt_R4.b = best_R2.b)
SELECT a, b, tag FROM best_R4;
CREATE TEMP TABLE node1_R2__R3 AS SELECT node0_best.a as a, R2.d as d, node0_best.b as b FROM R2 JOIN node0_best ON node0_best.a = R2.a WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R3__R4 AS SELECT node0_best.a as a, node0_best.b as b, R4.c as c FROM R4 JOIN node0_best ON node0_best.b = R4.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT node1_R2__R3.a as a, node1_R2__R3.d as d, node1_R2__R3.b as b, 0 as tag, cnt_R4.cnt as cnt FROM node1_R2__R3, cnt_R4 WHERE cnt_R4.b = node1_R2__R3.b),
  cnt_R1 as (SELECT d, COUNT(*) as cnt FROM R1 GROUP BY d),
  best_R1 as (SELECT best_R4.a as a, best_R4.d as d, best_R4.b as b, CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1.cnt THEN best_R4.cnt ELSE cnt_R1.cnt END as cnt FROM best_R4, cnt_R1 WHERE cnt_R1.d = best_R4.d)
SELECT a, d, b, tag FROM best_R1;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R2 as (SELECT a, COUNT(*) as cnt FROM R2 GROUP BY a),
  best_R2 as (SELECT node3_R3__R4.a as a, node3_R3__R4.b as b, node3_R3__R4.c as c, 0 as tag, cnt_R2.cnt as cnt FROM node3_R3__R4, cnt_R2 WHERE cnt_R2.a = node3_R3__R4.a),
  cnt_R1 as (SELECT c, COUNT(*) as cnt FROM R1 GROUP BY c),
  best_R1 as (SELECT best_R2.a as a, best_R2.b as b, best_R2.c as c, CASE WHEN best_R2.cnt < cnt_R1.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1.cnt THEN best_R2.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2, cnt_R1 WHERE cnt_R1.c = best_R2.c)
SELECT a, b, c, tag FROM best_R1;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT R4.c as c, node1_best.d as d, node1_best.a as a, node1_best.b as b FROM R4 JOIN node1_best ON node1_best.b = R4.b SEMI JOIN R1 ON R1.c = R4.c AND R1.d = node1_best.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT R1.c as c, node1_best.d as d, node1_best.a as a, node1_best.b as b FROM R1 JOIN node1_best ON node1_best.d = R1.d SEMI JOIN R4 ON R4.b = node1_best.b AND R4.c = R1.c WHERE node1_best.tag = 1) node2_R1__R2__R3__R4
UNION ALL
SELECT node4_R1__R2__R3__R4.a as a, node4_R1__R2__R3__R4.b as b, node4_R1__R2__R3__R4.c as c, node4_R1__R2__R3__R4.d as d FROM (SELECT node3_best.c as c, R2.d as d, node3_best.a as a, node3_best.b as b FROM R2 JOIN node3_best ON node3_best.a = R2.a SEMI JOIN R1 ON R1.c = node3_best.c AND R1.d = R2.d WHERE node3_best.tag = 0) node4_R1__R2__R3__R4
UNION ALL
SELECT node4_R1__R2__R3__R4.a as a, node4_R1__R2__R3__R4.b as b, node4_R1__R2__R3__R4.c as c, node4_R1__R2__R3__R4.d as d FROM (SELECT node3_best.c as c, R1.d as d, node3_best.a as a, node3_best.b as b FROM R1 JOIN node3_best ON node3_best.c = R1.c SEMI JOIN R2 ON R2.a = node3_best.a AND R2.d = R1.d WHERE node3_best.tag = 1) node4_R1__R2__R3__R4
);
