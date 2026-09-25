CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R5 AS SELECT col0 AS e, col1 AS a FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (((R4 t4 JOIN R3 t3 ON t4.d = t3.d) CROSS JOIN R1 t1) JOIN (R5 t5 CROSS JOIN R2 t2) ON t4.e = t5.e AND t3.c = t2.c AND t1.a = t5.a AND t1.b = t2.b);
