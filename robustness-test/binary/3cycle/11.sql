CREATE VIEW R1 AS SELECT col0 AS x, col1 AS y FROM R;
CREATE VIEW R2 AS SELECT col0 AS y, col1 AS z FROM R;
CREATE VIEW R3 AS SELECT col0 AS z, col1 AS x FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (R3 t3 JOIN (R1 t1 JOIN R2 t2 ON t1.y = t2.y) ON t3.x = t1.x AND t3.z = t2.z);
