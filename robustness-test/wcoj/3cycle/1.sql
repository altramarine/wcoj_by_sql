CREATE VIEW R1 AS SELECT col0 AS x, col1 AS y FROM R;
CREATE VIEW R2 AS SELECT col0 AS y, col1 AS z FROM R;
CREATE VIEW R3 AS SELECT col0 AS z, col1 AS x FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(x, y), R2(y, z), R3(z, x)
-- R2 [R1, R3]
-- [1] R1__R2__R3(x, y, z) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R1 as (SELECT y, COUNT(*) as cnt FROM R1 GROUP BY y),
  best_R1 as (SELECT R2.y as y, R2.z as z, 0 as tag, cnt_R1.cnt as cnt FROM R2, cnt_R1 WHERE cnt_R1.y = R2.y),
  cnt_R3 as (SELECT z, COUNT(*) as cnt FROM R3 GROUP BY z),
  best_R3 as (SELECT best_R1.y as y, best_R1.z as z, CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.tag ELSE 1 END as tag,CASE WHEN best_R1.cnt < cnt_R3.cnt THEN best_R1.cnt ELSE cnt_R3.cnt END as cnt FROM best_R1, cnt_R3 WHERE cnt_R3.z = best_R1.z)
SELECT y, z, tag FROM best_R3;
SELECT COUNT(*) FROM (
SELECT node1_R1__R2__R3.x as x, node1_R1__R2__R3.y as y, node1_R1__R2__R3.z as z FROM (SELECT R1.x as x, node0_best.y as y, node0_best.z as z FROM R1 JOIN node0_best ON node0_best.y = R1.y SEMI JOIN R3 ON R3.z = node0_best.z AND R3.x = R1.x WHERE node0_best.tag = 0) node1_R1__R2__R3
UNION ALL
SELECT node1_R1__R2__R3.x as x, node1_R1__R2__R3.y as y, node1_R1__R2__R3.z as z FROM (SELECT R3.x as x, node0_best.y as y, node0_best.z as z FROM R3 JOIN node0_best ON node0_best.z = R3.z SEMI JOIN R1 ON R1.x = R3.x AND R1.y = node0_best.y WHERE node0_best.tag = 1) node1_R1__R2__R3
);
