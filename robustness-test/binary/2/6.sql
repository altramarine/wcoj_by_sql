CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (((R1 t1 CROSS JOIN R4 t4) JOIN R3 t3 ON t1.a = t3.a AND t4.d = t3.d) JOIN R2 t2 ON t1.b = t2.b AND t4.c = t2.c);
