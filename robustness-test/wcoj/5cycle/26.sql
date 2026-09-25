CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, e), R5(e, a)
-- R2 [R1, R3]
-- [1] R1__R2(a, b, c), R3(c, d), R4(d, e), R5(e, a)
-- | R1__R2 [R3, R5]
-- | [2] R1__R2__R3(a, b, c, d), R4(d, e), R5(e, a)
-- | | R1__R2__R3 [R4, R5]
-- | | [3] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | [4] R1__R2__R5(a, b, c, e), R3(c, d), R4(d, e)
-- | | R1__R2__R5 [R4, R3]
-- | | [5] R1__R2__R3__R4__R5(a, b, c, e, d) [acyclic]
-- [6] R1(a, b), R2__R3(b, c, d), R4(d, e), R5(e, a)
-- | R1 [R2__R3, R5]
-- | [7] R1__R2__R3(a, b, c, d), R4(d, e), R5(e, a)
-- | | R1__R2__R3 [R4, R5]
-- | | [8] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | [9] R1__R5(a, b, e), R2__R3(b, c, d), R4(d, e)
-- | | R2__R3 [R4, R1__R5]
-- | | [10] R1__R5(a, b, e), R2__R3__R4(b, c, d, e) [acyclic]
-- | | [11] R1__R2__R3__R4__R5(a, b, e, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.c = best_R1.c)
SELECT b, c, tag FROM best_R3;
CREATE TEMP TABLE node1_R1__R2 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node6_R2__R3 AS SELECT node0_best.b as b, node0_best.c as c, R3.d as d FROM R3 JOIN node0_best ON node0_best.c = R3.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT node1_R1__R2.a as a, node1_R1__R2.b as b, node1_R1__R2.c as c, 0 as tag, cnt_R3.cnt as cnt FROM node1_R1__R2, cnt_R3 WHERE cnt_R3.c = node1_R1__R2.c),
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT best_R3.a as a, best_R3.b as b, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R3, cnt_R5 WHERE cnt_R5.a = best_R3.a)
SELECT a, b, c, tag FROM best_R5;
CREATE TEMP TABLE node2_R1__R2__R3 AS SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R3.d as d FROM R3 JOIN node1_best ON node1_best.c = R3.c WHERE node1_best.tag = 0;
CREATE TEMP TABLE node4_R1__R2__R5 AS SELECT node1_best.a as a, node1_best.b as b, node1_best.c as c, R5.e as e FROM R5 JOIN node1_best ON node1_best.a = R5.a WHERE node1_best.tag = 1;
CREATE TEMP TABLE node6_best AS WITH
  cnt_R2__R3 as (SELECT b, COUNT(*) as cnt FROM node6_R2__R3 GROUP BY b),
  best_R2__R3 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R3.cnt as cnt FROM R1, cnt_R2__R3 WHERE cnt_R2__R3.b = R1.b),
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT best_R2__R3.a as a, best_R2__R3.b as b, CASE WHEN best_R2__R3.cnt < cnt_R5.cnt THEN best_R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R2__R3.cnt < cnt_R5.cnt THEN best_R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R2__R3, cnt_R5 WHERE cnt_R5.a = best_R2__R3.a)
SELECT a, b, tag FROM best_R5;
CREATE TEMP TABLE node7_R1__R2__R3 AS SELECT node6_best.a as a, node6_best.b as b, node6_R2__R3.c as c, node6_R2__R3.d as d FROM node6_R2__R3 JOIN node6_best ON node6_best.b = node6_R2__R3.b WHERE node6_best.tag = 0;
CREATE TEMP TABLE node9_R1__R5 AS SELECT node6_best.a as a, node6_best.b as b, R5.e as e FROM R5 JOIN node6_best ON node6_best.a = R5.a WHERE node6_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT node2_R1__R2__R3.a as a, node2_R1__R2__R3.b as b, node2_R1__R2__R3.c as c, node2_R1__R2__R3.d as d, 0 as tag, cnt_R4.cnt as cnt FROM node2_R1__R2__R3, cnt_R4 WHERE cnt_R4.d = node2_R1__R2__R3.d),
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT best_R4.a as a, best_R4.b as b, best_R4.c as c, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.cnt ELSE cnt_R5.cnt END as cnt FROM best_R4, cnt_R5 WHERE cnt_R5.a = best_R4.a)
SELECT a, b, c, d, tag FROM best_R5;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R4 as (SELECT e, COUNT(*) as cnt FROM R4 GROUP BY e),
  best_R4 as (SELECT node4_R1__R2__R5.a as a, node4_R1__R2__R5.b as b, node4_R1__R2__R5.c as c, node4_R1__R2__R5.e as e, 0 as tag, cnt_R4.cnt as cnt FROM node4_R1__R2__R5, cnt_R4 WHERE cnt_R4.e = node4_R1__R2__R5.e),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R4.a as a, best_R4.b as b, best_R4.c as c, best_R4.e as e, CASE WHEN best_R4.cnt < cnt_R3.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R3.cnt THEN best_R4.cnt ELSE cnt_R3.cnt END as cnt FROM best_R4, cnt_R3 WHERE cnt_R3.c = best_R4.c)
SELECT a, b, c, e, tag FROM best_R3;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT node7_R1__R2__R3.a as a, node7_R1__R2__R3.b as b, node7_R1__R2__R3.c as c, node7_R1__R2__R3.d as d, 0 as tag, cnt_R4.cnt as cnt FROM node7_R1__R2__R3, cnt_R4 WHERE cnt_R4.d = node7_R1__R2__R3.d),
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT best_R4.a as a, best_R4.b as b, best_R4.c as c, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.cnt ELSE cnt_R5.cnt END as cnt FROM best_R4, cnt_R5 WHERE cnt_R5.a = best_R4.a)
SELECT a, b, c, d, tag FROM best_R5;
CREATE TEMP TABLE node9_best AS WITH
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT node6_R2__R3.b as b, node6_R2__R3.c as c, node6_R2__R3.d as d, 0 as tag, cnt_R4.cnt as cnt FROM node6_R2__R3, cnt_R4 WHERE cnt_R4.d = node6_R2__R3.d),
  cnt_R1__R5 as (SELECT b, COUNT(*) as cnt FROM node9_R1__R5 GROUP BY b),
  best_R1__R5 as (SELECT best_R4.b as b, best_R4.c as c, best_R4.d as d, CASE WHEN best_R4.cnt < cnt_R1__R5.cnt THEN best_R4.tag ELSE 1 END as tag,CASE WHEN best_R4.cnt < cnt_R1__R5.cnt THEN best_R4.cnt ELSE cnt_R1__R5.cnt END as cnt FROM best_R4, cnt_R1__R5 WHERE cnt_R1__R5.b = best_R4.b)
SELECT b, c, d, tag FROM best_R1__R5;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.c as c, node2_best.d as d, R4.e as e FROM R4 JOIN node2_best ON node2_best.d = R4.d SEMI JOIN R5 ON R5.e = R4.e AND R5.a = node2_best.a WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.c as c, node2_best.d as d, R5.e as e FROM R5 JOIN node2_best ON node2_best.a = R5.a SEMI JOIN R4 ON R4.d = node2_best.d AND R4.e = R5.e WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node5_R1__R2__R3__R4__R5.a as a, node5_R1__R2__R3__R4__R5.b as b, node5_R1__R2__R3__R4__R5.c as c, node5_R1__R2__R3__R4__R5.d as d, node5_R1__R2__R3__R4__R5.e as e FROM (SELECT node4_best.a as a, node4_best.b as b, node4_best.c as c, node4_best.e as e, R4.d as d FROM R4 JOIN node4_best ON node4_best.e = R4.e SEMI JOIN R3 ON R3.c = node4_best.c AND R3.d = R4.d WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5
UNION ALL
SELECT node5_R1__R2__R3__R4__R5.a as a, node5_R1__R2__R3__R4__R5.b as b, node5_R1__R2__R3__R4__R5.c as c, node5_R1__R2__R3__R4__R5.d as d, node5_R1__R2__R3__R4__R5.e as e FROM (SELECT node4_best.a as a, node4_best.b as b, node4_best.c as c, node4_best.e as e, R3.d as d FROM R3 JOIN node4_best ON node4_best.c = R3.c SEMI JOIN R4 ON R4.d = R3.d AND R4.e = node4_best.e WHERE node4_best.tag = 1) node5_R1__R2__R3__R4__R5
UNION ALL
SELECT node8_R1__R2__R3__R4__R5.a as a, node8_R1__R2__R3__R4__R5.b as b, node8_R1__R2__R3__R4__R5.c as c, node8_R1__R2__R3__R4__R5.d as d, node8_R1__R2__R3__R4__R5.e as e FROM (SELECT node7_best.a as a, node7_best.b as b, node7_best.c as c, node7_best.d as d, R4.e as e FROM R4 JOIN node7_best ON node7_best.d = R4.d SEMI JOIN R5 ON R5.e = R4.e AND R5.a = node7_best.a WHERE node7_best.tag = 0) node8_R1__R2__R3__R4__R5
UNION ALL
SELECT node8_R1__R2__R3__R4__R5.a as a, node8_R1__R2__R3__R4__R5.b as b, node8_R1__R2__R3__R4__R5.c as c, node8_R1__R2__R3__R4__R5.d as d, node8_R1__R2__R3__R4__R5.e as e FROM (SELECT node7_best.a as a, node7_best.b as b, node7_best.c as c, node7_best.d as d, R5.e as e FROM R5 JOIN node7_best ON node7_best.a = R5.a SEMI JOIN R4 ON R4.d = node7_best.d AND R4.e = R5.e WHERE node7_best.tag = 1) node8_R1__R2__R3__R4__R5
UNION ALL
SELECT node9_R1__R5.a as a, node10_R2__R3__R4.b as b, node10_R2__R3__R4.c as c, node10_R2__R3__R4.d as d, node10_R2__R3__R4.e as e FROM (SELECT node9_best.b as b, node9_best.c as c, node9_best.d as d, R4.e as e FROM R4 JOIN node9_best ON node9_best.d = R4.d WHERE node9_best.tag = 0) node10_R2__R3__R4 JOIN node9_R1__R5 ON node9_R1__R5.b = node10_R2__R3__R4.b AND node9_R1__R5.e = node10_R2__R3__R4.e
UNION ALL
SELECT node11_R1__R2__R3__R4__R5.a as a, node11_R1__R2__R3__R4__R5.b as b, node11_R1__R2__R3__R4__R5.c as c, node11_R1__R2__R3__R4__R5.d as d, node11_R1__R2__R3__R4__R5.e as e FROM (SELECT node9_R1__R5.a as a, node9_best.b as b, node9_R1__R5.e as e, node9_best.c as c, node9_best.d as d FROM node9_R1__R5 JOIN node9_best ON node9_best.b = node9_R1__R5.b SEMI JOIN R4 ON R4.d = node9_best.d AND R4.e = node9_R1__R5.e WHERE node9_best.tag = 1) node11_R1__R2__R3__R4__R5
);
