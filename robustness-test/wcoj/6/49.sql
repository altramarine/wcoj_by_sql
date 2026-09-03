CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R1 [R4, R3]
-- [1] R1__R4__R5(a, b, d), R2(b, c), R3(a, c), R6(c, d)
-- | R1__R4__R5 [R3, R2]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, d, c) [acyclic]
-- [3] R1__R2__R3(a, b, c), R4(a, d), R5(b, d), R6(c, d)
-- | R1__R2__R3 [R6, R5]
-- | [4] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R4.cnt as cnt FROM R1, cnt_R4 WHERE cnt_R4.a = R1.a),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R4.a as a, best_R4.b as b, CASE WHEN best_R4.cnt < cnt_R3.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R3.cnt THEN best_R4.cnt ELSE cnt_R3.cnt END as cnt FROM best_R4, cnt_R3 WHERE cnt_R3.a = best_R4.a)
SELECT a, b, tag FROM best_R3;
CREATE TEMP TABLE node1_R1__R4__R5 AS SELECT node0_best.a as a, node0_best.b as b, R4.d as d FROM R4 JOIN node0_best ON node0_best.a = R4.a WHERE node0_best.tag = 0;
CREATE TEMP TABLE node3_R1__R2__R3 AS SELECT node0_best.a as a, node0_best.b as b, R3.c as c FROM R3 JOIN node0_best ON node0_best.a = R3.a WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT node1_R1__R4__R5.a as a, node1_R1__R4__R5.b as b, node1_R1__R4__R5.d as d, 0 as tag, cnt_R3.cnt as cnt FROM node1_R1__R4__R5, cnt_R3 WHERE cnt_R3.a = node1_R1__R4__R5.a),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R3.a as a, best_R3.b as b, best_R3.d as d, CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.cnt ELSE cnt_R2.cnt END as cnt FROM best_R3, cnt_R2 WHERE cnt_R2.b = best_R3.b)
SELECT a, b, d, tag FROM best_R2;
CREATE TEMP TABLE node3_best AS WITH
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT node3_R1__R2__R3.a as a, node3_R1__R2__R3.b as b, node3_R1__R2__R3.c as c, 0 as tag, cnt_R6.cnt as cnt FROM node3_R1__R2__R3, cnt_R6 WHERE cnt_R6.c = node3_R1__R2__R3.c),
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT best_R6.a as a, best_R6.b as b, best_R6.c as c, CASE WHEN best_R6.cnt < cnt_R5.cnt THEN best_R6.tag ELSE 1 END as tag,CASE WHEN best_R6.cnt < cnt_R5.cnt THEN best_R6.cnt ELSE cnt_R5.cnt END as cnt FROM best_R6, cnt_R5 WHERE cnt_R5.b = best_R6.b)
SELECT a, b, c, tag FROM best_R5;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.d as d, R3.c as c FROM R3 JOIN node1_best ON node1_best.a = R3.a SEMI JOIN R2 ON R2.b = node1_best.b AND R2.c = R3.c WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.d as d, R2.c as c FROM R2 JOIN node1_best ON node1_best.b = R2.b SEMI JOIN R3 ON R3.a = node1_best.a AND R3.c = R2.c WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node3_best.c as c, R6.d as d FROM R6 JOIN node3_best ON node3_best.c = R6.c SEMI JOIN R5 ON R5.b = node3_best.b AND R5.d = R6.d WHERE node3_best.tag = 0) node4_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node4_R1__R2__R3__R4__R5__R6.a as a, node4_R1__R2__R3__R4__R5__R6.b as b, node4_R1__R2__R3__R4__R5__R6.c as c, node4_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node3_best.a as a, node3_best.b as b, node3_best.c as c, R5.d as d FROM R5 JOIN node3_best ON node3_best.b = R5.b SEMI JOIN R6 ON R6.c = node3_best.c AND R6.d = R5.d WHERE node3_best.tag = 1) node4_R1__R2__R3__R4__R5__R6
);
