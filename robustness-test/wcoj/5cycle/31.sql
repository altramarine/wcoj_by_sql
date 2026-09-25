CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, e), R5(e, a)
-- R1 [R5, R2]
-- [1] R1__R5(a, b, e), R2(b, c), R3(c, d), R4(d, e)
-- | R4 [R1__R5, R3]
-- | [2] R1__R4__R5(a, b, e, d), R2(b, c), R3(c, d)
-- | | R1__R4__R5 [R2, R3]
-- | | [3] R1__R2__R3__R4__R5(a, b, e, d, c) [acyclic]
-- | [4] R1__R5(a, b, e), R2(b, c), R3__R4(c, d, e)
-- | | R3__R4 [R1__R5, R2]
-- | | [5] R1__R2__R3__R4__R5(a, b, e, c, d) [acyclic]
-- | | [6] R1__R5(a, b, e), R2__R3__R4(b, c, d, e) [acyclic]
-- [7] R1__R2(a, b, c), R3(c, d), R4(d, e), R5(e, a)
-- | R1__R2 [R3, R5]
-- | [8] R1__R2__R3(a, b, c, d), R4(d, e), R5(e, a)
-- | | R1__R2__R3 [R5, R4]
-- | | [9] R1__R2__R3__R4__R5(a, b, c, d, e) [acyclic]
-- | [10] R1__R2__R5(a, b, c, e), R3(c, d), R4(d, e)
-- | | R1__R2__R5 [R3, R4]
-- | | [11] R1__R2__R3__R4__R5(a, b, c, e, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R5.cnt as cnt FROM R1, cnt_R5 WHERE cnt_R5.a = R1.a),
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT best_R5.a as a, best_R5.b as b, CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R2.cnt THEN best_R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R5, cnt_R2 WHERE cnt_R2.b = best_R5.b)
SELECT a, b, tag FROM best_R2;
CREATE TEMP TABLE node1_R1__R5 AS SELECT node0_best.a as a, node0_best.b as b, R5.e as e FROM R5 JOIN node0_best ON node0_best.a = R5.a WHERE node0_best.tag = 0;
CREATE TEMP TABLE node7_R1__R2 AS SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM R2 JOIN node0_best ON node0_best.b = R2.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R5 as (SELECT e, COUNT(*) as cnt FROM node1_R1__R5 GROUP BY e),
  best_R1__R5 as (SELECT R4.d as d, R4.e as e, 0 as tag, cnt_R1__R5.cnt as cnt FROM R4, cnt_R1__R5 WHERE cnt_R1__R5.e = R4.e),
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT best_R1__R5.d as d, best_R1__R5.e as e, CASE WHEN best_R1__R5.cnt < cnt_R3.cnt THEN best_R1__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R5.cnt < cnt_R3.cnt THEN best_R1__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1__R5, cnt_R3 WHERE cnt_R3.d = best_R1__R5.d)
SELECT d, e, tag FROM best_R3;
CREATE TEMP TABLE node2_R1__R4__R5 AS SELECT node1_R1__R5.a as a, node1_R1__R5.b as b, node1_best.e as e, node1_best.d as d FROM node1_R1__R5 JOIN node1_best ON node1_best.e = node1_R1__R5.e WHERE node1_best.tag = 0;
CREATE TEMP TABLE node4_R3__R4 AS SELECT R3.c as c, node1_best.d as d, node1_best.e as e FROM R3 JOIN node1_best ON node1_best.d = R3.d WHERE node1_best.tag = 1;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT node7_R1__R2.a as a, node7_R1__R2.b as b, node7_R1__R2.c as c, 0 as tag, cnt_R3.cnt as cnt FROM node7_R1__R2, cnt_R3 WHERE cnt_R3.c = node7_R1__R2.c),
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT best_R3.a as a, best_R3.b as b, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R5.cnt THEN best_R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R3, cnt_R5 WHERE cnt_R5.a = best_R3.a)
SELECT a, b, c, tag FROM best_R5;
CREATE TEMP TABLE node8_R1__R2__R3 AS SELECT node7_best.a as a, node7_best.b as b, node7_best.c as c, R3.d as d FROM R3 JOIN node7_best ON node7_best.c = R3.c WHERE node7_best.tag = 0;
CREATE TEMP TABLE node10_R1__R2__R5 AS SELECT node7_best.a as a, node7_best.b as b, node7_best.c as c, R5.e as e FROM R5 JOIN node7_best ON node7_best.a = R5.a WHERE node7_best.tag = 1;
CREATE TEMP TABLE node2_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT node2_R1__R4__R5.a as a, node2_R1__R4__R5.b as b, node2_R1__R4__R5.e as e, node2_R1__R4__R5.d as d, 0 as tag, cnt_R2.cnt as cnt FROM node2_R1__R4__R5, cnt_R2 WHERE cnt_R2.b = node2_R1__R4__R5.b),
  cnt_R3 as (SELECT d, COUNT(*) as cnt FROM R3 GROUP BY d),
  best_R3 as (SELECT best_R2.a as a, best_R2.b as b, best_R2.e as e, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2, cnt_R3 WHERE cnt_R3.d = best_R2.d)
SELECT a, b, e, d, tag FROM best_R3;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1__R5 as (SELECT e, COUNT(*) as cnt FROM node1_R1__R5 GROUP BY e),
  best_R1__R5 as (SELECT node4_R3__R4.c as c, node4_R3__R4.d as d, node4_R3__R4.e as e, 0 as tag, cnt_R1__R5.cnt as cnt FROM node4_R3__R4, cnt_R1__R5 WHERE cnt_R1__R5.e = node4_R3__R4.e),
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT best_R1__R5.c as c, best_R1__R5.d as d, best_R1__R5.e as e, CASE WHEN best_R1__R5.cnt < cnt_R2.cnt THEN best_R1__R5.tag ELSE 1 END as tag,CASE WHEN best_R1__R5.cnt < cnt_R2.cnt THEN best_R1__R5.cnt ELSE cnt_R2.cnt END as cnt FROM best_R1__R5, cnt_R2 WHERE cnt_R2.c = best_R1__R5.c)
SELECT c, d, e, tag FROM best_R2;
CREATE TEMP TABLE node8_best AS WITH
  cnt_R5 as (SELECT a, COUNT(*) as cnt FROM R5 GROUP BY a),
  best_R5 as (SELECT node8_R1__R2__R3.a as a, node8_R1__R2__R3.b as b, node8_R1__R2__R3.c as c, node8_R1__R2__R3.d as d, 0 as tag, cnt_R5.cnt as cnt FROM node8_R1__R2__R3, cnt_R5 WHERE cnt_R5.a = node8_R1__R2__R3.a),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R5.a as a, best_R5.b as b, best_R5.c as c, best_R5.d as d, CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R4.cnt THEN best_R5.cnt ELSE cnt_R4.cnt END as cnt FROM best_R5, cnt_R4 WHERE cnt_R4.d = best_R5.d)
SELECT a, b, c, d, tag FROM best_R4;
CREATE TEMP TABLE node10_best AS WITH
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT node10_R1__R2__R5.a as a, node10_R1__R2__R5.b as b, node10_R1__R2__R5.c as c, node10_R1__R2__R5.e as e, 0 as tag, cnt_R3.cnt as cnt FROM node10_R1__R2__R5, cnt_R3 WHERE cnt_R3.c = node10_R1__R2__R5.c),
  cnt_R4 as (SELECT e, COUNT(*) as cnt FROM R4 GROUP BY e),
  best_R4 as (SELECT best_R3.a as a, best_R3.b as b, best_R3.c as c, best_R3.e as e, CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.cnt ELSE cnt_R4.cnt END as cnt FROM best_R3, cnt_R4 WHERE cnt_R4.e = best_R3.e)
SELECT a, b, c, e, tag FROM best_R4;
SELECT COUNT(*) FROM (
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.e as e, node2_best.d as d, R2.c as c FROM R2 JOIN node2_best ON node2_best.b = R2.b SEMI JOIN R3 ON R3.c = R2.c AND R3.d = node2_best.d WHERE node2_best.tag = 0) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node3_R1__R2__R3__R4__R5.a as a, node3_R1__R2__R3__R4__R5.b as b, node3_R1__R2__R3__R4__R5.c as c, node3_R1__R2__R3__R4__R5.d as d, node3_R1__R2__R3__R4__R5.e as e FROM (SELECT node2_best.a as a, node2_best.b as b, node2_best.e as e, node2_best.d as d, R3.c as c FROM R3 JOIN node2_best ON node2_best.d = R3.d SEMI JOIN R2 ON R2.b = node2_best.b AND R2.c = R3.c WHERE node2_best.tag = 1) node3_R1__R2__R3__R4__R5
UNION ALL
SELECT node5_R1__R2__R3__R4__R5.a as a, node5_R1__R2__R3__R4__R5.b as b, node5_R1__R2__R3__R4__R5.c as c, node5_R1__R2__R3__R4__R5.d as d, node5_R1__R2__R3__R4__R5.e as e FROM (SELECT node1_R1__R5.a as a, node1_R1__R5.b as b, node4_best.e as e, node4_best.c as c, node4_best.d as d FROM node1_R1__R5 JOIN node4_best ON node4_best.e = node1_R1__R5.e SEMI JOIN R2 ON R2.b = node1_R1__R5.b AND R2.c = node4_best.c WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5
UNION ALL
SELECT node1_R1__R5.a as a, node6_R2__R3__R4.b as b, node6_R2__R3__R4.c as c, node6_R2__R3__R4.d as d, node6_R2__R3__R4.e as e FROM (SELECT R2.b as b, node4_best.c as c, node4_best.d as d, node4_best.e as e FROM R2 JOIN node4_best ON node4_best.c = R2.c WHERE node4_best.tag = 1) node6_R2__R3__R4 JOIN node1_R1__R5 ON node1_R1__R5.b = node6_R2__R3__R4.b AND node1_R1__R5.e = node6_R2__R3__R4.e
UNION ALL
SELECT node9_R1__R2__R3__R4__R5.a as a, node9_R1__R2__R3__R4__R5.b as b, node9_R1__R2__R3__R4__R5.c as c, node9_R1__R2__R3__R4__R5.d as d, node9_R1__R2__R3__R4__R5.e as e FROM (SELECT node8_best.a as a, node8_best.b as b, node8_best.c as c, node8_best.d as d, R5.e as e FROM R5 JOIN node8_best ON node8_best.a = R5.a SEMI JOIN R4 ON R4.d = node8_best.d AND R4.e = R5.e WHERE node8_best.tag = 0) node9_R1__R2__R3__R4__R5
UNION ALL
SELECT node9_R1__R2__R3__R4__R5.a as a, node9_R1__R2__R3__R4__R5.b as b, node9_R1__R2__R3__R4__R5.c as c, node9_R1__R2__R3__R4__R5.d as d, node9_R1__R2__R3__R4__R5.e as e FROM (SELECT node8_best.a as a, node8_best.b as b, node8_best.c as c, node8_best.d as d, R4.e as e FROM R4 JOIN node8_best ON node8_best.d = R4.d SEMI JOIN R5 ON R5.e = R4.e AND R5.a = node8_best.a WHERE node8_best.tag = 1) node9_R1__R2__R3__R4__R5
UNION ALL
SELECT node11_R1__R2__R3__R4__R5.a as a, node11_R1__R2__R3__R4__R5.b as b, node11_R1__R2__R3__R4__R5.c as c, node11_R1__R2__R3__R4__R5.d as d, node11_R1__R2__R3__R4__R5.e as e FROM (SELECT node10_best.a as a, node10_best.b as b, node10_best.c as c, node10_best.e as e, R3.d as d FROM R3 JOIN node10_best ON node10_best.c = R3.c SEMI JOIN R4 ON R4.d = R3.d AND R4.e = node10_best.e WHERE node10_best.tag = 0) node11_R1__R2__R3__R4__R5
UNION ALL
SELECT node11_R1__R2__R3__R4__R5.a as a, node11_R1__R2__R3__R4__R5.b as b, node11_R1__R2__R3__R4__R5.c as c, node11_R1__R2__R3__R4__R5.d as d, node11_R1__R2__R3__R4__R5.e as e FROM (SELECT node10_best.a as a, node10_best.b as b, node10_best.c as c, node10_best.e as e, R4.d as d FROM R4 JOIN node10_best ON node10_best.e = R4.e SEMI JOIN R3 ON R3.c = node10_best.c AND R3.d = R4.d WHERE node10_best.tag = 1) node11_R1__R2__R3__R4__R5
);
