-- rename: R -> R1
-- rename: R -> R2
-- rename: R -> R3
-- rename: R -> R4
-- rename: R -> R5
-- relations: R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

-- [0] group: R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- [0] enter split (e.g. "R1 R2 R3"): R2 R1 R3 R4 R5
-- [0] start: R2  split: R1 vs R3 vs R4 vs R5
-- [0] R2 join w/ R1: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [1] group: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [1] enter split (e.g. "R1 R2 R3"): R4 R1__R2__R3 R5
-- [1] start: R4  split: R1__R2__R3 vs R5
-- [1] R4 join w/ R1__R2__R3: R1__R2__R3__R4__R5(a, b, c, d)
-- [1] R4 join w/ R5: R1__R2__R3(a, b, c), R4__R5(b, d, c)
-- [0] R2 join w/ R3: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [4] group: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [4] enter split (e.g. "R1 R2 R3"): R4 R1__R2__R3 R5
-- [4] start: R4  split: R1__R2__R3 vs R5
-- [4] R4 join w/ R1__R2__R3: R1__R2__R3__R4__R5(a, b, c, d)
-- [4] R4 join w/ R5: R1__R2__R3(a, b, c), R4__R5(b, d, c)
-- [0] R2 join w/ R4: R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- [7] group: R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- [7] enter split (e.g. "R1 R2 R3"): R1 R2__R4__R5 R3
-- [7] start: R1  split: R2__R4__R5 vs R3
-- [7] R1 join w/ R2__R4__R5: R1__R2__R3__R4__R5(a, b, c, d)
-- [7] R1 join w/ R3: R1__R3(a, b, c), R2__R4__R5(b, c, d)
-- [0] R2 join w/ R5: R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- [10] group: R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- [10] enter split (e.g. "R1 R2 R3"): R1 R2__R4__R5 R3
-- [10] start: R1  split: R2__R4__R5 vs R3
-- [10] R1 join w/ R2__R4__R5: R1__R2__R3__R4__R5(a, b, c, d)
-- [10] R1 join w/ R3: R1__R3(a, b, c), R2__R4__R5(b, c, d)
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R2 [R1, R3, R4, R5]
-- [1] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R1__R2__R3, R5]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [3] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- [4] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R1__R2__R3, R5]
-- | [5] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [6] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- [7] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R2__R4__R5, R3]
-- | [8] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [9] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
-- [10] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R2__R4__R5, R3]
-- | [11] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [12] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.c = best_R1.c),
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT best_R3.b as b, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.tag ELSE 2 END as tag,CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.cnt ELSE cnt_R4.cnt END as cnt FROM best_R3, cnt_R4 WHERE cnt_R4.b = best_R3.b),
  cnt_R5 as (SELECT c, COUNT(*) as cnt FROM R5 GROUP BY c),
  best_R5 as (SELECT best_R4.b as b, best_R4.c as c, CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.tag ELSE 3 END as tag,CASE WHEN best_R4.cnt < cnt_R5.cnt THEN best_R4.cnt ELSE cnt_R5.cnt END as cnt FROM best_R4, cnt_R5 WHERE cnt_R5.c = best_R4.c),
SELECT b, c, tag FROM best_R5;
CREATE TEMP TABLE node1_R1__R2__R3 AS SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM node0_best JOIN R1 ON node0_best.b = R1.b SEMI JOIN R3 ON R3.a = R1.a AND R3.c = node0_best.c WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R1__R2__R3 AS SELECT R3.a as a, node0_best.b as b, node0_best.c as c FROM node0_best JOIN R3 ON node0_best.c = R3.c SEMI JOIN R1 ON R1.a = R3.a AND R1.b = node0_best.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE node7_R2__R4__R5 AS SELECT node0_best.b as b, node0_best.c as c, R4.d as d FROM node0_best JOIN R4 ON node0_best.b = R4.b SEMI JOIN R5 ON R5.c = node0_best.c AND R5.d = R4.d WHERE node0_best.tag = 2;
CREATE TEMP TABLE node10_R2__R4__R5 AS SELECT node0_best.b as b, node0_best.c as c, R5.d as d FROM node0_best JOIN R5 ON node0_best.c = R5.c SEMI JOIN R4 ON R4.b = node0_best.b AND R4.d = R5.d WHERE node0_best.tag = 3;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM node1_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = R4.b),
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT best_R1__R2__R3.b as b, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R1__R2__R3, cnt_R5 WHERE cnt_R5.d = best_R1__R2__R3.d),
SELECT b, d, tag FROM best_R5;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM node4_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = R4.b),
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT best_R1__R2__R3.b as b, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R1__R2__R3, cnt_R5 WHERE cnt_R5.d = best_R1__R2__R3.d),
SELECT b, d, tag FROM best_R5;
CREATE TEMP TABLE node7_best AS WITH
  cnt_R2__R4__R5 as (SELECT b, COUNT(*) as cnt FROM node7_R2__R4__R5 GROUP BY b),
  best_R2__R4__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R4__R5.cnt as cnt FROM R1, cnt_R2__R4__R5 WHERE cnt_R2__R4__R5.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2__R4__R5.a as a, best_R2__R4__R5.b as b, CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2__R4__R5, cnt_R3 WHERE cnt_R3.a = best_R2__R4__R5.a),
SELECT a, b, tag FROM best_R3;
CREATE TEMP TABLE node10_best AS WITH
  cnt_R2__R4__R5 as (SELECT b, COUNT(*) as cnt FROM node10_R2__R4__R5 GROUP BY b),
  best_R2__R4__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R4__R5.cnt as cnt FROM R1, cnt_R2__R4__R5 WHERE cnt_R2__R4__R5.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2__R4__R5.a as a, best_R2__R4__R5.b as b, CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2__R4__R5, cnt_R3 WHERE cnt_R3.a = best_R2__R4__R5.a),
SELECT a, b, tag FROM best_R3;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4__R5.a as a, node2_R1__R2__R3__R4__R5.b as b, node2_R1__R2__R3__R4__R5.c as c, node2_R1__R2__R3__R4__R5.d as d FROM (SELECT node1_R1__R2__R3.a as a, node1_best.b as b, node1_R1__R2__R3.c as c, node1_best.d as d FROM node1_best JOIN node1_R1__R2__R3 ON node1_best.b = node1_R1__R2__R3.b SEMI JOIN R5 ON R5.c = node1_R1__R2__R3.c AND R5.d = node1_best.d WHERE node1_best.tag = 0) node2_R1__R2__R3__R4__R5
UNION ALL
SELECT node1_R1__R2__R3.a as a, node3_R4__R5.b as b, node3_R4__R5.c as c, node3_R4__R5.d as d FROM (SELECT node1_best.b as b, node1_best.d as d, R5.c as c FROM node1_best JOIN R5 ON node1_best.d = R5.d WHERE node1_best.tag = 1) node3_R4__R5 JOIN node1_R1__R2__R3 ON node1_R1__R2__R3.b = node3_R4__R5.b AND node1_R1__R2__R3.c = node3_R4__R5.c
UNION ALL
SELECT node5_R1__R2__R3__R4__R5.a as a, node5_R1__R2__R3__R4__R5.b as b, node5_R1__R2__R3__R4__R5.c as c, node5_R1__R2__R3__R4__R5.d as d FROM (SELECT node4_R1__R2__R3.a as a, node4_best.b as b, node4_R1__R2__R3.c as c, node4_best.d as d FROM node4_best JOIN node4_R1__R2__R3 ON node4_best.b = node4_R1__R2__R3.b SEMI JOIN R5 ON R5.c = node4_R1__R2__R3.c AND R5.d = node4_best.d WHERE node4_best.tag = 0) node5_R1__R2__R3__R4__R5
UNION ALL
SELECT node4_R1__R2__R3.a as a, node6_R4__R5.b as b, node6_R4__R5.c as c, node6_R4__R5.d as d FROM (SELECT node4_best.b as b, node4_best.d as d, R5.c as c FROM node4_best JOIN R5 ON node4_best.d = R5.d WHERE node4_best.tag = 1) node6_R4__R5 JOIN node4_R1__R2__R3 ON node4_R1__R2__R3.b = node6_R4__R5.b AND node4_R1__R2__R3.c = node6_R4__R5.c
UNION ALL
SELECT node8_R1__R2__R3__R4__R5.a as a, node8_R1__R2__R3__R4__R5.b as b, node8_R1__R2__R3__R4__R5.c as c, node8_R1__R2__R3__R4__R5.d as d FROM (SELECT node7_best.a as a, node7_best.b as b, node7_R2__R4__R5.c as c, node7_R2__R4__R5.d as d FROM node7_best JOIN node7_R2__R4__R5 ON node7_best.b = node7_R2__R4__R5.b SEMI JOIN R3 ON R3.a = node7_best.a AND R3.c = node7_R2__R4__R5.c WHERE node7_best.tag = 0) node8_R1__R2__R3__R4__R5
UNION ALL
SELECT node9_R1__R3.a as a, node9_R1__R3.b as b, node9_R1__R3.c as c, node7_R2__R4__R5.d as d FROM (SELECT node7_best.a as a, node7_best.b as b, R3.c as c FROM node7_best JOIN R3 ON node7_best.a = R3.a WHERE node7_best.tag = 1) node9_R1__R3 JOIN node7_R2__R4__R5 ON node7_R2__R4__R5.b = node9_R1__R3.b AND node7_R2__R4__R5.c = node9_R1__R3.c
UNION ALL
SELECT node11_R1__R2__R3__R4__R5.a as a, node11_R1__R2__R3__R4__R5.b as b, node11_R1__R2__R3__R4__R5.c as c, node11_R1__R2__R3__R4__R5.d as d FROM (SELECT node10_best.a as a, node10_best.b as b, node10_R2__R4__R5.c as c, node10_R2__R4__R5.d as d FROM node10_best JOIN node10_R2__R4__R5 ON node10_best.b = node10_R2__R4__R5.b SEMI JOIN R3 ON R3.a = node10_best.a AND R3.c = node10_R2__R4__R5.c WHERE node10_best.tag = 0) node11_R1__R2__R3__R4__R5
UNION ALL
SELECT node12_R1__R3.a as a, node12_R1__R3.b as b, node12_R1__R3.c as c, node10_R2__R4__R5.d as d FROM (SELECT node10_best.a as a, node10_best.b as b, R3.c as c FROM node10_best JOIN R3 ON node10_best.a = R3.a WHERE node10_best.tag = 1) node12_R1__R3 JOIN node10_R2__R4__R5 ON node10_R2__R4__R5.b = node12_R1__R3.b AND node10_R2__R4__R5.c = node12_R1__R3.c
);