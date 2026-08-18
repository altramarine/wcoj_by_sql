CREATE VIEW R1 AS SELECT col0 AS x, col1 AS y FROM R;
CREATE VIEW R2 AS SELECT col0 AS y, col1 AS z FROM R;
CREATE VIEW R3 AS SELECT col0 AS x, col1 AS z FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM ((R3 t3 JOIN R2 t2 ON t3.z = t2.z) JOIN R1 t1 ON t3.x = t1.x AND t2.y = t1.y);
