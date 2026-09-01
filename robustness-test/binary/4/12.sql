CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (R2 t2 JOIN (R3 t3 JOIN (R1 t1 JOIN (R4 t4 JOIN R5 t5 ON t4.d = t5.d) ON t1.b = t4.b) ON t3.a = t1.a AND t3.c = t5.c) ON t2.c = t3.c AND t2.b = t1.b AND t2.b = t4.b AND t2.c = t5.c);
