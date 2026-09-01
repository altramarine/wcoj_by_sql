CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (R4 t4 JOIN (R5 t5 JOIN ((R1 t1 JOIN R3 t3 ON t1.a = t3.a) JOIN R2 t2 ON t1.b = t2.b AND t3.c = t2.c) ON t5.c = t3.c AND t5.c = t2.c) ON t4.d = t5.d AND t4.b = t1.b AND t4.b = t2.b);
