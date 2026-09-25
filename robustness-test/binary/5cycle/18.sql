CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM ((R2 t2 JOIN R1 t1 ON t2.b = t1.b) JOIN ((R3 t3 CROSS JOIN R5 t5) JOIN R4 t4 ON t3.d = t4.d AND t5.e = t4.e) ON t2.c = t3.c AND t1.a = t5.a);
