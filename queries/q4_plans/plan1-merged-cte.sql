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

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (
WITH
  cnt_R1 as (SELECT b, COUNT(*) as cnt FROM R1 GROUP BY b),
  best_R1 as (SELECT R2.b as b, R2.c as c, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.b = R2.b),
  cnt_R3 as (SELECT c, COUNT(*) as cnt FROM R3 GROUP BY c),
  best_R3 as (SELECT best_R1.b as b, best_R1.c as c, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.c = best_R1.c),
  cnt_R4 as (SELECT b, COUNT(*) as cnt FROM R4 GROUP BY b),
  best_R4 as (SELECT best_R3.b as b, best_R3.c as c, CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.tag ELSE 2 END as tag,CASE WHEN best_R3.cnt < cnt_R4.cnt THEN best_R3.cnt ELSE cnt_R4.cnt END as cnt FROM best_R3, cnt_R4 WHERE cnt_R4.b = best_R3.b),
  cnt_R5_node0 as (SELECT c, COUNT(*) as cnt FROM R5 GROUP BY c),
  node0_best AS MATERIALIZED (SELECT best_R4.b as b, best_R4.c as c, CASE WHEN best_R4.cnt < cnt_R5_node0.cnt THEN best_R4.tag ELSE 3 END as tag,CASE WHEN best_R4.cnt < cnt_R5_node0.cnt THEN best_R4.cnt ELSE cnt_R5_node0.cnt END as cnt FROM best_R4, cnt_R5_node0 WHERE cnt_R5_node0.c = best_R4.c),
  all_R1__R2__R3 AS MATERIALIZED (
    SELECT R1.a as a, node0_best.b as b, node0_best.c as c FROM R1 JOIN node0_best ON node0_best.b = R1.b SEMI JOIN R3 ON R3.a = R1.a AND R3.c = node0_best.c WHERE node0_best.tag = 0
    UNION ALL
    SELECT R3.a as a, node0_best.b as b, node0_best.c as c FROM R3 JOIN node0_best ON node0_best.c = R3.c SEMI JOIN R1 ON R1.a = R3.a AND R1.b = node0_best.b WHERE node0_best.tag = 1
  ),
  all_R2__R4__R5 AS MATERIALIZED (
    SELECT node0_best.b as b, node0_best.c as c, R4.d as d FROM R4 JOIN node0_best ON node0_best.b = R4.b SEMI JOIN R5 ON R5.c = node0_best.c AND R5.d = R4.d WHERE node0_best.tag = 2
    UNION ALL
    SELECT node0_best.b as b, node0_best.c as c, R5.d as d FROM R5 JOIN node0_best ON node0_best.c = R5.c SEMI JOIN R4 ON R4.b = node0_best.b AND R4.d = R5.d WHERE node0_best.tag = 3
  ),
  cnt_R1__R2__R3 as (SELECT b, COUNT(*) as cnt FROM all_R1__R2__R3 GROUP BY b),
  best_R1__R2__R3 as (SELECT R4.b as b, R4.d as d, 0 as tag, cnt_R1__R2__R3.cnt as cnt FROM R4, cnt_R1__R2__R3 WHERE cnt_R1__R2__R3.b = R4.b),
  cnt_R5_abc as (SELECT d, COUNT(*) as cnt FROM R5 GROUP BY d),
  all_abc_best AS MATERIALIZED (SELECT best_R1__R2__R3.b as b, best_R1__R2__R3.d as d, CASE WHEN best_R1__R2__R3.cnt < cnt_R5_abc.cnt THEN best_R1__R2__R3.tag ELSE 1 END as tag,CASE WHEN best_R1__R2__R3.cnt < cnt_R5_abc.cnt THEN best_R1__R2__R3.cnt ELSE cnt_R5_abc.cnt END as cnt FROM best_R1__R2__R3, cnt_R5_abc WHERE cnt_R5_abc.d = best_R1__R2__R3.d),
  cnt_R2__R4__R5 as (SELECT b, COUNT(*) as cnt FROM all_R2__R4__R5 GROUP BY b),
  best_R2__R4__R5 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2__R4__R5.cnt as cnt FROM R1, cnt_R2__R4__R5 WHERE cnt_R2__R4__R5.b = R1.b),
  cnt_R3_bcd as (SELECT a, COUNT(*) as cnt FROM R3 GROUP BY a),
  all_bcd_best AS MATERIALIZED (SELECT best_R2__R4__R5.a as a, best_R2__R4__R5.b as b, CASE WHEN best_R2__R4__R5.cnt < cnt_R3_bcd.cnt THEN best_R2__R4__R5.tag ELSE 1 END as tag,CASE WHEN best_R2__R4__R5.cnt < cnt_R3_bcd.cnt THEN best_R2__R4__R5.cnt ELSE cnt_R3_bcd.cnt END as cnt FROM best_R2__R4__R5, cnt_R3_bcd WHERE cnt_R3_bcd.a = best_R2__R4__R5.a)
SELECT res.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_R1__R2__R3.a as a, all_abc_best.b as b, all_R1__R2__R3.c as c, all_abc_best.d as d FROM all_R1__R2__R3 JOIN all_abc_best ON all_abc_best.b = all_R1__R2__R3.b SEMI JOIN R5 ON R5.c = all_R1__R2__R3.c AND R5.d = all_abc_best.d WHERE all_abc_best.tag = 0) res
UNION ALL
SELECT all_R1__R2__R3.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_abc_best.b as b, all_abc_best.d as d, R5.c as c FROM R5 JOIN all_abc_best ON all_abc_best.d = R5.d WHERE all_abc_best.tag = 1) res JOIN all_R1__R2__R3 ON all_R1__R2__R3.b = res.b AND all_R1__R2__R3.c = res.c
UNION ALL
SELECT res.a as a, res.b as b, res.c as c, res.d as d FROM (SELECT all_bcd_best.a as a, all_bcd_best.b as b, all_R2__R4__R5.c as c, all_R2__R4__R5.d as d FROM all_R2__R4__R5 JOIN all_bcd_best ON all_bcd_best.b = all_R2__R4__R5.b SEMI JOIN R3 ON R3.a = all_bcd_best.a AND R3.c = all_R2__R4__R5.c WHERE all_bcd_best.tag = 0) res
UNION ALL
SELECT res.a as a, res.b as b, res.c as c, all_R2__R4__R5.d as d FROM (SELECT all_bcd_best.a as a, all_bcd_best.b as b, R3.c as c FROM R3 JOIN all_bcd_best ON all_bcd_best.a = R3.a WHERE all_bcd_best.tag = 1) res JOIN all_R2__R4__R5 ON all_R2__R4__R5.b = res.b AND all_R2__R4__R5.c = res.c
);
