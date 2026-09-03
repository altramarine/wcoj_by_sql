CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R2 [R6, R5]
-- [1] R1(a, b), R2__R5__R6(b, c, d), R3(a, c), R4(a, d)
-- | R4 [R3, R2__R5__R6]
-- | [2] R1(a, b), R2__R5__R6(b, c, d), R3__R4(a, c, d)
-- | | R2__R5__R6 [R1, R3__R4]
-- | | [3] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
-- | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R6.cnt as cnt FROM R2, cnt_R6 WHERE cnt_R6.c = R2.c),
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT best_R6.b as b, best_R6.c as c, CASE WHEN best_R6.cnt < cnt_R5.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R5.cnt THEN best_R6.cnt ELSE cnt_R5.cnt END as cnt FROM best_R6, cnt_R5 WHERE cnt_R5.b = best_R6.b)
SELECT b, c, tag FROM best_R5;
CREATE TEMP TABLE node1_R2__R5__R6 AS SELECT node0_best.b as b, node0_best.c as c, R6.d as d FROM R6 JOIN node0_best ON node0_best.c = R6.c SEMI JOIN R5 ON R5.b = node0_best.b AND R5.d = R6.d WHERE node0_best.tag = 0
UNION ALL
SELECT node0_best.b as b, node0_best.c as c, R5.d as d FROM R5 JOIN node0_best ON node0_best.b = R5.b SEMI JOIN R6 ON R6.c = node0_best.c AND R6.d = R5.d WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT R4.a as a, R4.d as d, 0 as tag, cnt_R3.cnt as cnt FROM R4, cnt_R3 WHERE cnt_R3.a = R4.a),
  cnt_R2__R5__R6 as (SELECT d, COUNT(*) as cnt FROM node1_R2__R5__R6 GROUP BY d),
  best_R2__R5__R6 as (SELECT best_R3.a as a, best_R3.d as d, CASE WHEN best_R3.cnt < cnt_R2__R5__R6.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R2__R5__R6.cnt THEN best_R3.cnt ELSE cnt_R2__R5__R6.cnt END as cnt FROM best_R3, cnt_R2__R5__R6 WHERE cnt_R2__R5__R6.d = best_R3.d)
SELECT a, d, tag FROM best_R2__R5__R6;
CREATE TEMP TABLE node2_R3__R4 AS SELECT node1_best.a as a, R3.c as c, node1_best.d as d FROM R3 JOIN node1_best ON node1_best.a = R3.a WHERE node1_best.tag = 0;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT node1_R2__R5__R6.b as b, node1_R2__R5__R6.c as c, node1_R2__R5__R6.d as d, 0 as tag, cnt_R1.cnt as cnt FROM node1_R2__R5__R6, cnt_R1 WHERE cnt_R1.b = node1_R2__R5__R6.b),
  cnt_R3__R4 as (SELECT c,d, COUNT(*) as cnt FROM node2_R3__R4 GROUP BY c,d),
  best_R3__R4 as (SELECT best_R1.b as b, best_R1.c as c, best_R1.d as d, CASE WHEN best_R1.cnt < cnt_R3__R4.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3__R4.cnt THEN best_R1.cnt ELSE cnt_R3__R4.cnt END as cnt FROM best_R1, cnt_R3__R4 WHERE cnt_R3__R4.c = best_R1.c AND cnt_R3__R4.d = best_R1.d)
SELECT b, c, d, tag FROM best_R3__R4;
SELECT COUNT(*) FROM (
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_R2__R5__R6.b as b, node1_R2__R5__R6.c as c, node1_best.d as d FROM node1_R2__R5__R6 JOIN node1_best ON node1_best.d = node1_R2__R5__R6.d SEMI JOIN R3 ON R3.a = node1_best.a AND R3.c = node1_R2__R5__R6.c WHERE node1_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT R1.a as a, node2_best.b as b, node2_best.c as c, node2_best.d as d FROM R1 JOIN node2_best ON node2_best.b = R1.b SEMI JOIN node2_R3__R4 ON node2_R3__R4.a = R1.a AND node2_R3__R4.c = node2_best.c AND node2_R3__R4.d = node2_best.d WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node3_R1__R2__R3__R4__R5__R6.a as a, node3_R1__R2__R3__R4__R5__R6.b as b, node3_R1__R2__R3__R4__R5__R6.c as c, node3_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node2_R3__R4.a as a, node2_best.b as b, node2_best.c as c, node2_best.d as d FROM node2_R3__R4 JOIN node2_best ON node2_best.c = node2_R3__R4.c AND node2_best.d = node2_R3__R4.d SEMI JOIN R1 ON R1.a = node2_R3__R4.a AND R1.b = node2_best.b WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5__R6
);
