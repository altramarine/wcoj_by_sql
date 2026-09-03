CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(a, d), R5(b, d), R6(c, d)
-- R2 [R1, R3]
-- [1] R1__R2__R3(a, b, c), R4(a, d), R5(b, d), R6(c, d)
-- | R1__R2__R3 [R5, R6]
-- | [2] R1__R2__R3__R4__R5__R6(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.c = best_R1.c)
SELECT b, c, tag FROM best_R3;
CREATE TEMP TABLE node1_R1__R2__R3 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b SEMI JOIN R3 ON R3.a = R1.a AND R3.c = node0_best.c WHERE node0_best.tag = 0
UNION ALL
SELECT R3.a as a, node0_best.b as b, node0_best.c as c FROM R3 JOIN node0_best ON node0_best.c = R3.c SEMI JOIN R1 ON R1.a = R3.a AND R1.b = node0_best.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R5 as (SELECT b, COUNT(*) as cnt FROM R5 GROUP BY b),
  best_R5 as (SELECT node1_R1__R2__R3.a as a, node1_R1__R2__R3.b as b, node1_R1__R2__R3.c as c, 0 as tag, cnt_R5.cnt as cnt FROM node1_R1__R2__R3, cnt_R5 WHERE cnt_R5.b = node1_R1__R2__R3.b),
  cnt_R6 as (SELECT c, COUNT(*) as cnt FROM R6 GROUP BY c),
  best_R6 as (SELECT best_R5.a as a, best_R5.b as b, best_R5.c as c, CASE WHEN best_R5.cnt < cnt_R6.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R6.cnt THEN best_R5.cnt ELSE cnt_R6.cnt END as cnt FROM best_R5, cnt_R6 WHERE cnt_R6.c = best_R5.c)
SELECT a, b, c, tag FROM best_R6;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R5.d as d FROM R5 JOIN node1_best ON node1_best.b = R5.b SEMI JOIN R6 ON R6.c = node1_best.c AND R6.d = R5.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5__R6
UNION ALL
SELECT node2_R1__R2__R3__R4__R5__R6.a as a, node2_R1__R2__R3__R4__R5__R6.b as b, node2_R1__R2__R3__R4__R5__R6.c as c, node2_R1__R2__R3__R4__R5__R6.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R6.d as d FROM R6 JOIN node1_best ON node1_best.c = R6.c SEMI JOIN R5 ON R5.b = node1_best.b AND R5.d = R6.d WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5__R6
);
