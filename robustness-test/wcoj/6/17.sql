CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R5 [R6, R2]
-- [1] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R3 [R2__R5__R6, R1]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [3] R1__R3(a, b, c), R2__R5__R6(b, c, d), R4(a, d)
-- | | R4 [R2__R5__R6, R1__R3]
-- | | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R6 as (SELECT d, COUNT(*) as cnt FROM R6 GROUP BY d),
  best_R6 as (SELECT R5.b as b, R5.d as d, 0 as tag, cnt_R6.cnt as cnt FROM R5, cnt_R6 WHERE cnt_R6.d = R5.d),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R6.b as b, best_R6.d as d, CASE WHEN best_R6.cnt < cnt_R2.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R2.cnt THEN best_R6.cnt ELSE cnt_R2.cnt END as cnt FROM best_R6, cnt_R2 WHERE cnt_R2.b = best_R6.b)
SELECT b, d, tag FROM best_R2;
CREATE TEMP TABLE node1_R2__R5__R6 AS SELECT node0_best.b as b, R6.c as c, node0_best.d as d FROM R6 JOIN node0_best ON node0_best.d = R6.d SEMI JOIN R2 ON R2.b = node0_best.b AND R2.c = R6.c WHERE node0_best.tag = 0
UNION ALL
SELECT node0_best.b as b, R2.c as c, node0_best.d as d FROM R2 JOIN node0_best ON node0_best.b = R2.b SEMI JOIN R6 ON R6.c = R2.c AND R6.d = node0_best.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R2__R5__R6 as (SELECT c, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY c),
  best_R2__R5__R6 as (SELECT R3.a as a, R3.c as c, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R3, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.c = R3.c),
  cnt_R1 as (SELECT a, COUNT(*) as cnt FROM R1 GROUP BY a),
  best_R1 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.c as c, CASE WHEN best_R2__R5__R6.cnt < cnt_R1.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R1.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2__R5__R6, cnt_R1 WHERE cnt_R1.a = best_R2__R5__R6.a)
SELECT a, c, tag FROM best_R1;
CREATE TEMP TABLE node3_R1__R3 AS SELECT node1_best.a as a, R1.b as b, node1_best.c as c FROM R1 JOIN node1_best ON node1_best.a = R1.a WHERE node1_best.tag = 1;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R2__R5__R6 as (SELECT d, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY d),
  best_R2__R5__R6 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R2__R5__R6.cnt as cnt FROM R4, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.d = R4.d),
  cnt_R1__R3 as (SELECT a, COUNT(*) as cnt FROM node3_R1__R3 GROUP BY a),
  best_R1__R3 as (SELECT best_R2__R5__R6.a as a, best_R2__R5__R6.d as d, CASE WHEN best_R2__R5__R6.cnt < cnt_R1__R3.cnt THEN best_R2__R5__R6.tag ELSE 1 END as tag,CASE WHEN best_R2__R5__R6.cnt < cnt_R1__R3.cnt THEN best_R2__R5__R6.cnt ELSE cnt_R1__R3.cnt END as cnt FROM best_R2__R5__R6, cnt_R1__R3 WHERE cnt_R1__R3.a = best_R2__R5__R6.a)
SELECT a, d, tag FROM best_R1__R3;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_R2__R5__R6.b as b, node1_best.c as c, node1_R2__R5__R6.d as d FROM node1_R2__R5__R6 JOIN node1_best ON node1_best.c = node1_R2__R5__R6.c SEMI JOIN R1 ON R1.a = node1_best.a AND R1.b = node1_R2__R5__R6.b WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node1_R2__R5__R6.b as b, node1_R2__R5__R6.c as c, node3_best.d as d FROM node1_R2__R5__R6 JOIN node3_best ON node3_best.d = node1_R2__R5__R6.d WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_R1__R3.b as b, node3_R1__R3.c as c, node3_best.d as d FROM node3_R1__R3 JOIN node3_best ON node3_best.a = node3_R1__R3.a WHERE node3_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
);
