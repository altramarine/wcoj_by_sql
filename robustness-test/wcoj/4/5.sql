CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R2 [R1, R5]
-- [1] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R1__R2__R3, R5]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [3] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- [4] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R3, R2__R4__R5]
-- | [5] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
-- | [6] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R5 as (SELECT c, COUNT(*) as cnt FROM R5 GROUP BY c),
  best_R5 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R5.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R5.cnt THEN best_R1.cnt ELSE cnt_R5.cnt END as cnt FROM best_R1, cnt_R5 WHERE cnt_R5.c = best_R1.c)
SELECT b, c, tag FROM best_R5;
CREATE TEMP TABLE node1_R1__R2__R3 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R2__R4__R5 AS SELECT node0_best.b as b, node0_best.c as c, R5.d as d FROM R5 JOIN node0_best ON node0_best.c = R5.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM node1_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = R4.b),
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT best_R1__R2__R3.b as b, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R1__R2__R3, cnt_R5 WHERE cnt_R5.d = best_R1__R2__R3.d)
SELECT b, d, tag FROM best_R5;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R3.cnt as cnt FROM R1, cnt_R3 WHERE cnt_R3.a = R1.a),
  cnt_R2__R4__R5 as (SELECT b, COUNT(*) as cnt FROM node4_R2__R4__R5 GROUP BY b),
  best_R2__R4__R5 as (SELECT best_R3.a as a, best_R3.b as b, CASE WHEN best_R3.cnt < cnt_R2__R4__R5.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R2__R4__R5.cnt THEN best_R3.cnt ELSE cnt_R2__R4__R5.cnt END as cnt FROM best_R3, cnt_R2__R4__R5 WHERE cnt_R2__R4__R5.b = best_R3.b)
SELECT a, b, tag FROM best_R2__R4__R5;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_R1__R2__R3.a as a, node1_best.b as b, node1_R1__R2__R3.c as c, node1_best.d as d FROM node1_R1__R2__R3 JOIN node1_best ON node1_best.b = node1_R1__R2__R3.b SEMI JOIN R5 ON R5.c = node1_R1__R2__R3.c AND R5.d = node1_best.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node1_R1__R2__R3.a as a, node3_R4__R5.b as b, node3_R4__R5.c as c, node3_R4__R5.d as d FROM (SELECT node1_best.b as b, node1_best.d as d, R5.c as c FROM R5 JOIN node1_best ON node1_best.d = R5.d WHERE node1_best.tag = 1) node3_R4__R5 JOIN node1_R1__R2__R3 ON node1_R1__R2__R3.b = node3_R4__R5.b AND node1_R1__R2__R3.c = node3_R4__R5.c
UNION ALL
SELECT node5_R1__R3.a as a, node5_R1__R3.b as b, node5_R1__R3.c as c, node4_R2__R4__R5.d as d FROM (SELECT node4_best.a as a, node4_best.b as b, R3.c as c FROM R3 JOIN node4_best ON node4_best.a = R3.a WHERE node4_best.tag = 0) node5_R1__R3 JOIN node4_R2__R4__R5 ON node4_R2__R4__R5.b = node5_R1__R3.b AND node4_R2__R4__R5.c = node5_R1__R3.c
UNION ALL
SELECT node6_R1__R2__R3__R4__R5.a as a, node6_R1__R2__R3__R4__R5.b as b, node6_R1__R2__R3__R4__R5.c as c, node6_R1__R2__R3__R4__R5.d as d FROM (SELECT node4_best.a as a, node4_best.b as b, node4_R2__R4__R5.c as c, node4_R2__R4__R5.d as d FROM node4_R2__R4__R5 JOIN node4_best ON node4_best.b = node4_R2__R4__R5.b SEMI JOIN R3 ON R3.a = node4_best.a AND R3.c = node4_R2__R4__R5.c WHERE node4_best.tag = 1) node6_R1__R2__R3__R4__R5
);
