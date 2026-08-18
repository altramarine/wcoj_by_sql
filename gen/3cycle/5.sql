CREATE VIEW R1 AS SELECT col0 AS x, col1 AS y FROM R;
CREATE VIEW R2 AS SELECT col0 AS y, col1 AS z FROM R;
CREATE VIEW R3 AS SELECT col0 AS z, col1 AS x FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
-- tree:
-- R1(x, y), R2(y, z), R3(z, x)
-- R3 [R2, R1]
-- [1] R1__R2__R3(x, y, z) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT z, COUNT(*) as cnt FROM R2 GROUP BY z),
  best_R2 as (SELECT R3.z as z, R3.x as x, 0 as tag, cnt_R2.cnt as cnt FROM R3, cnt_R2 WHERE cnt_R2.z = R3.z),
  cnt_R1 as (SELECT x, COUNT(*) as cnt FROM R1 GROUP BY x),
  best_R1 as (SELECT best_R2.z as z, best_R2.x as x, CASE WHEN best_R2.cnt < cnt_R1.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1.cnt THEN best_R2.cnt ELSE cnt_R1.cnt END as cnt FROM best_R2, cnt_R1 WHERE cnt_R1.x = best_R2.x)
SELECT z, x, tag FROM best_R1;
SELECT COUNT(*) FROM (
SELECT node1_R1__R2__R3.x as x, node1_R1__R2__R3.y as y, node1_R1__R2__R3.z as z FROM (SELECT node0_best.x as x, R2.y as y, node0_best.z as z FROM R2 JOIN node0_best ON node0_best.z = R2.z SEMI JOIN R1 ON R1.x = node0_best.x AND R1.y = R2.y WHERE node0_best.tag = 0) node1_R1__R2__R3
UNION ALL
SELECT node1_R1__R2__R3.x as x, node1_R1__R2__R3.y as y, node1_R1__R2__R3.z as z FROM (SELECT node0_best.x as x, R1.y as y, node0_best.z as z FROM R1 JOIN node0_best ON node0_best.x = R1.x SEMI JOIN R2 ON R2.y = R1.y AND R2.z = node0_best.z WHERE node0_best.tag = 1) node1_R1__R2__R3
);
