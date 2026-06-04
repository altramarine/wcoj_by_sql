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

SET disabled_optimizers = 'join_order, build_side_probe_side';
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
CREATE TEMP TABLE all_R1__R2__R3 AS
  SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b SEMI JOIN R3 ON R3.a = R1.a AND R3.c = node0_best.c WHERE node0_best.tag = 0
  UNION ALL
  SELECT R3.a as a, node0_best.b as b, node0_best.c as c FROM R3 JOIN node0_best ON node0_best.c = R3.c SEMI JOIN R1 ON R1.a = R3.a AND R1.b = node0_best.b WHERE node0_best.tag = 1;
CREATE TEMP TABLE all_R2__R4__R5 AS
  SELECT node0_best.b as b, node0_best.c as c, R4.d as d FROM R4 JOIN node0_best ON node0_best.b = R4.b SEMI JOIN R5 ON R5.c = node0_best.c AND R5.d = R4.d WHERE node0_best.tag = 2
  UNION ALL
  SELECT node0_best.b as b, node0_best.c as c, R5.d as d FROM R5 JOIN node0_best ON node0_best.c = R5.c SEMI JOIN R4 ON R4.b = node0_best.b AND R4.d = R5.d WHERE node0_best.tag = 3;
CREATE TEMP TABLE all_abc_best AS WITH
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM all_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = R4.b),
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT best_R1__R2__R3.b as b, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R5.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R5.cnt END as cnt FROM best_R1__R2__R3, cnt_R5 WHERE cnt_R5.d = best_R1__R2__R3.d),
SELECT b, d, tag FROM best_R5;
CREATE TEMP TABLE all_bcd_best AS WITH
  cnt_R2__R4__R5 as (SELECT b, COUNT(*) as cnt FROM all_R2__R4__R5 GROUP BY b),
  best_R2__R4__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R4__R5.cnt as cnt FROM R1, cnt_R2__R4__R5 WHERE cnt_R2__R4__R5.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2__R4__R5.a as a, best_R2__R4__R5.b as b, CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R2__R4__R5.cnt < cnt_R3.cnt THEN best_R2__R4__R5.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2__R4__R5, cnt_R3 WHERE cnt_R3.a = best_R2__R4__R5.a),
SELECT a, b, tag FROM best_R3;
SELECT COUNT(*) FROM (
SELECT res.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_R1__R2__R3.a as a, all_abc_best.b as b, all_R1__R2__R3.c as c, all_abc_best.d as d FROM all_R1__R2__R3 JOIN all_abc_best ON all_abc_best.b = all_R1__R2__R3.b SEMI JOIN R5 ON R5.c = all_R1__R2__R3.c AND R5.d = all_abc_best.d WHERE all_abc_best.tag = 0) res
UNION ALL
SELECT all_R1__R2__R3.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_abc_best.b as b, all_abc_best.d as d, R5.c as c FROM R5 JOIN all_abc_best ON all_abc_best.d = R5.d WHERE all_abc_best.tag = 1) res JOIN all_R1__R2__R3 ON all_R1__R2__R3.b = res.b AND all_R1__R2__R3.c = res.c
UNION ALL
SELECT res.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_bcd_best.a as a, all_bcd_best.b as b, all_R2__R4__R5.c as c, all_R2__R4__R5.d as d FROM all_R2__R4__R5 JOIN all_bcd_best ON all_bcd_best.b = all_R2__R4__R5.b SEMI JOIN R3 ON R3.a = all_bcd_best.a AND R3.c = all_R2__R4__R5.c WHERE all_bcd_best.tag = 0) res
UNION ALL
SELECT res.a as a, res.b as b, res.c as c, all_R2__R4__R5.d as d FROM (SELECT all_bcd_best.a as a, all_bcd_best.b as b, R3.c as c FROM R3 JOIN all_bcd_best ON all_bcd_best.a = R3.a WHERE all_bcd_best.tag = 1) res JOIN all_R2__R4__R5 ON all_R2__R4__R5.b = res.b AND all_R2__R4__R5.c = res.c
);