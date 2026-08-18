CREATE VIEW R1 AS SELECT col0 AS x, col1 AS y FROM R;
CREATE VIEW R2 AS SELECT col0 AS y, col1 AS z FROM R;
CREATE VIEW R3 AS SELECT col0 AS x, col1 AS z FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(x, y), R2(y, z), R3(x, z)
-- R1 [R3, R2]
-- [1] R1__R2__R3(x, y, z) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R3 as (SELECT x, COUNT(*) as cnt FROM R3 GROUP BY x),
  best_R3 as (SELECT R1.x as x, R1.y as y, 0 as tag, cnt_R3.cnt as cnt FROM R1, cnt_R3 WHERE cnt_R3.x = R1.x),
  cnt_R2 as (SELECT y, COUNT(*) as cnt FROM R2 GROUP BY y),
  best_R2 as (SELECT best_R3.x as x, best_R3.y as y, CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.tag ELSE 1 END as tag,CASE WHEN best_R3.cnt < cnt_R2.cnt THEN best_R3.cnt ELSE cnt_R2.cnt END as cnt FROM best_R3, cnt_R2 WHERE cnt_R2.y = best_R3.y)
SELECT x, y, tag FROM best_R2;
SELECT COUNT(*) FROM (
SELECT node1_R1__R2__R3.x as x, node1_R1__R2__R3.y as y, node1_R1__R2__R3.z as z FROM (SELECT node0_best.x as x, node0_best.y as y, R3.z as z FROM R3 JOIN node0_best ON node0_best.x = R3.x SEMI JOIN R2 ON R2.y = node0_best.y AND R2.z = R3.z WHERE node0_best.tag = 0) node1_R1__R2__R3
UNION ALL
SELECT node1_R1__R2__R3.x as x, node1_R1__R2__R3.y as y, node1_R1__R2__R3.z as z FROM (SELECT node0_best.x as x, node0_best.y as y, R2.z as z FROM R2 JOIN node0_best ON node0_best.y = R2.y SEMI JOIN R3 ON R3.x = node0_best.x AND R3.z = R2.z WHERE node0_best.tag = 1) node1_R1__R2__R3
);
