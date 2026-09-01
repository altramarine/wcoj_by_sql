CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R1 [R3, R2]
-- [1] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R1__R2__R3 [R5, R4]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R3.cnt as cnt FROM R1, cnt_R3 WHERE cnt_R3.a = R1.a),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R3.a as a, best_R3.b as b, CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.cnt ELSE cnt_R2.cnt END as cnt FROM best_R3, cnt_R2 WHERE cnt_R2.b = best_R3.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node1_R1__R2__R3 AS SELECT node0_best.a as a, node0_best.b as b, R3.c as c FROM R3 JOIN node0_best ON node0_best.a = R3.a SEMI JOIN R2 ON R2.b = node0_best.b AND R2.c = R3.c WHERE node0_best.tag = 0
UNION ALL
SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM R2 JOIN node0_best ON node0_best.b = R2.b SEMI JOIN R3 ON R3.a = node0_best.a AND R3.c = R2.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R5 as (SELECT c, COUNT(*) as cnt FROM R5 GROUP BY c),
  best_R5 as (SELECT node1_R1__R2__R3.a as a, node1_R1__R2__R3.b as b, node1_R1__R2__R3.c as c, 0 as tag, cnt_R5.cnt as cnt FROM node1_R1__R2__R3, cnt_R5 WHERE cnt_R5.c = node1_R1__R2__R3.c),
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT best_R5.a as a, best_R5.b as b, best_R5.c as c, CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.cnt ELSE cnt_R4.cnt END as cnt FROM best_R5, cnt_R4 WHERE cnt_R4.b = best_R5.b)
SELECT a, b, c, tag FROM best_R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R5.d as d FROM R5 JOIN node1_best ON node1_best.c = R5.c SEMI JOIN R4 ON R4.b = node1_best.b AND R4.d = R5.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R4.d as d FROM R4 JOIN node1_best ON node1_best.b = R4.b SEMI JOIN R5 ON R5.c = node1_best.c AND R5.d = R4.d WHERE node1_best.tag = 1) node2_R1__R2__R3__R4__R5
);
