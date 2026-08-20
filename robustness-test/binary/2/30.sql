CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM ((R3 t3 JOIN (R1 t1 JOIN R2 t2 ON t1.b = t2.b) ON t3.a = t1.a) JOIN R4 t4 ON t3.d = t4.d AND t2.c = t4.c);
