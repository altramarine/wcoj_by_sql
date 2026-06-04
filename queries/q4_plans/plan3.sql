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
-- [0] enter split (e.g. "R1 R2 R3"): R1 R2 R3 
-- [0] start: R1  split: R2 vs R3
-- [0] R1 join w/ R2: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [1] group: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [1] enter split (e.g. "R1 R2 R3"): R4 R5 R1__R2__R3
-- [1] start: R4  split: R5 vs R1__R2__R3
-- [1] R4 join w/ R5: R1__R2__R3(a, b, c), R4__R5(b, d, c)
-- [1] R4 join w/ R1__R2__R3: R1__R2__R3__R4__R5(a, b, c, d)
-- [0] R1 join w/ R3: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [4] group: R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- [4] enter split (e.g. "R1 R2 R3"): R4 R5 R1__R2__R3
-- [4] start: R4  split: R5 vs R1__R2__R3
-- [4] R4 join w/ R5: R1__R2__R3(a, b, c), R4__R5(b, d, c)
-- [4] R4 join w/ R1__R2__R3: R1__R2__R3__R4__R5(a, b, c, d)
-- tree:
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R1 [R2, R3]
-- [1] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R5, R1__R2__R3]
-- | [2] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- | [3] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- [4] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R5, R1__R2__R3]
-- | [5] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- | [6] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2.cnt as cnt FROM R1, cnt_R2 WHERE cnt_R2.b = R1.b),
  cnt_R3 as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  best_R3 as (SELECT best_R2.a as a, best_R2.b as b, CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R3.cnt THEN best_R2.cnt ELSE cnt_R3.cnt END as cnt FROM best_R2, cnt_R3 WHERE cnt_R3.a = best_R2.a),
SELECT a, b, tag FROM best_R3;

CREATE TEMP TABLE all_R1__R2__R3 AS
  SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM R2 JOIN node0_best ON node0_best.b = R2.b SEMI JOIN R3 ON R3.a = node0_best.a AND R3.c = R2.c WHERE node0_best.tag = 0
  UNION ALL
  SELECT node0_best.a as a, node0_best.b as b, R3.c as c FROM R3 JOIN node0_best ON node0_best.a = R3.a SEMI JOIN R2 ON R2.b = node0_best.b AND R2.c = R3.c WHERE node0_best.tag = 1;
CREATE TEMP TABLE all_best AS WITH
  cnt_R5 as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  best_R5 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R5.cnt as cnt FROM R4, cnt_R5 WHERE cnt_R5.d = R4.d),
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM all_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT best_R5.b as b, best_R5.d as d, CASE WHEN best_R5.cnt < cnt_R1__R2__R3.cnt THEN best_R5.tag ELSE 1 END as tag,CASE WHEN best_R5.cnt < cnt_R1__R2__R3.cnt THEN best_R5.cnt ELSE cnt_R1__R2__R3.cnt END as cnt FROM best_R5, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = best_R5.b),
SELECT b, d, tag FROM best_R1__R2__R3;
SELECT COUNT(*) FROM (
SELECT all_R1__R2__R3.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_best.b as b, all_best.d as d, R5.c as c FROM R5 JOIN all_best ON all_best.d = R5.d WHERE all_best.tag = 0) res JOIN all_R1__R2__R3 ON all_R1__R2__R3.b = res.b AND all_R1__R2__R3.c = res.c
UNION ALL
SELECT res.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_R1__R2__R3.a as a, all_best.b as b, all_R1__R2__R3.c as c, all_best.d as d FROM all_R1__R2__R3 JOIN all_best ON all_best.b = all_R1__R2__R3.b SEMI JOIN R5 ON R5.c = all_R1__R2__R3.c AND R5.d = all_best.d WHERE all_best.tag = 1) res
);