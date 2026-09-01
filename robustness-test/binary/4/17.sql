CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (R3 t3 JOIN ((R5 t5 JOIN R4 t4 ON t5.d = t4.d) JOIN (R2 t2 JOIN R1 t1 ON t2.b = t1.b) ON t5.c = t2.c AND t4.b = t2.b AND t4.b = t1.b) ON t3.c = t5.c AND t3.c = t2.c AND t3.a = t1.a);
