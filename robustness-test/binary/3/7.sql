CREATE VIEW R1 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R2 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (R3 t3 JOIN (R2 t2 JOIN (R4 t4 JOIN R1 t1 ON t4.c = t1.c) ON t2.d = t1.d) ON t3.a = t2.a AND t3.b = t4.b);
