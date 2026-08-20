CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM ((R1 t1 JOIN R2 t2 ON t1.b = t2.b) JOIN (R3 t3 JOIN R4 t4 ON t3.d = t4.d) ON t1.a = t4.a AND t2.c = t3.c);
