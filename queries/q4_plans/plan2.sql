-- rename: R -> R1
-- rename: R -> R2
-- rename: R -> R3
-- relations: R1(a, b), R2(b, c), R3(a, c)
CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;

-- [0] group: R1(a, b), R2(b, c), R3(a, c)
-- [0] enter split (e.g. "R1 R2 R3"): R1 R2 R3
-- [0] start: R1  split: R2 vs R3
-- [0] R1 join w/ R2: R1__R2__R3(a, b, c)
-- [0] R1 join w/ R3: R1__R2__R3(a, b, c)
-- tree:
-- R1(a, b), R2(b, c), R3(a, c)
-- R1 [R2, R3]
-- [1] R1__R2__R3(a, b, c) [acyclic]
-- [2] R1__R2__R3(a, b, c) [acyclic]
CREATE TEMP TABLE node0a_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2.cnt as cnt FROM R1, cnt_R2 WHERE cnt_R2.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2.a as a, best_R2.b as b, CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2, cnt_R3 WHERE cnt_R3.a = best_R2.a),
SELECT a, b, tag FROM best_R3;

-- relations: R2(b, c), R4(b, d), R5(c, d)
-- CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

-- [0] group: R2(b, c), R4(b, d), R5(c, d)
-- [0] enter split (e.g. "R1 R2 R3"): R4 R2 R5
-- [0] start: R4  split: R2 vs R5
-- [0] R4 join w/ R2: R2__R4__R5(b, c, d)
-- [0] R4 join w/ R5: R2__R4__R5(b, c, d)
-- tree:
-- R2(b, c), R4(b, d), R5(c, d)
-- R4 [R2, R5]
-- [1] R2__R4__R5(b, c, d) [acyclic]
-- [2] R2__R4__R5(b, c, d) [acyclic]
CREATE TEMP TABLE node0b_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R4, cnt_R2 WHERE cnt_R2.b = R4.b),
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT best_R2.b as b, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R5.cnt THEN best_R2.cnt ELSE cnt_R5.cnt END as cnt FROM best_R2, cnt_R5 WHERE cnt_R5.d = best_R2.d),
SELECT b, d, tag FROM best_R5;

-- SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (
  SELECT node1_R1__R2__R3.a as a, node1_R1__R2__R3.b as b, node1_R1__R2__R3.c as c FROM (SELECT node0a_best.a as a, node0a_best.b as b, R2.c as c FROM node0a_best JOIN R2 ON node0a_best.b = R2.b SEMI JOIN R3 ON R3.a = node0a_best.a AND R3.c = R2.c WHERE node0a_best.tag = 0) node1_R1__R2__R3
  UNION ALL
  SELECT node2_R1__R2__R3.a as a, node2_R1__R2__R3.b as b, node2_R1__R2__R3.c as c FROM (SELECT node0a_best.a as a, node0a_best.b as b, R3.c as c FROM node0a_best JOIN R3 ON node0a_best.a = R3.a SEMI JOIN R2 ON R2.b = node0a_best.b AND R2.c = R3.c WHERE node0a_best.tag = 1) node2_R1__R2__R3
) abc
JOIN (
  SELECT node1_R2__R4__R5.b as b, node1_R2__R4__R5.c as c, node1_R2__R4__R5.d as d FROM (SELECT node0b_best.b as b, R2.c as c, node0b_best.d as d FROM node0b_best JOIN R2 ON node0b_best.b = R2.b SEMI JOIN R5 ON R5.c = R2.c AND R5.d = node0b_best.d WHERE node0b_best.tag = 0) node1_R2__R4__R5
  UNION ALL
  SELECT node2_R2__R4__R5.b as b, node2_R2__R4__R5.c as c, node2_R2__R4__R5.d as d FROM (SELECT node0b_best.b as b, R5.c as c, node0b_best.d as d FROM node0b_best JOIN R5 ON node0b_best.d = R5.d SEMI JOIN R2 ON R2.b = node0b_best.b AND R2.c = R5.c WHERE node0b_best.tag = 1) node2_R2__R4__R5
) bcd ON abc.b = bcd.b AND abc.c = bcd.c;