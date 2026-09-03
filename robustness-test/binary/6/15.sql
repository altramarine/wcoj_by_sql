CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (R2 t2 JOIN ((R1 t1 JOIN R5 t5 ON t1.b = t5.b) JOIN (R4 t4 JOIN (R3 t3 JOIN R6 t6 ON t3.c = t6.c) ON t4.a = t3.a AND t4.d = t6.d) ON t1.a = t4.a AND t1.a = t3.a AND t5.d = t4.d AND t5.d = t6.d) ON t2.b = t1.b AND t2.b = t5.b AND t2.c = t3.c AND t2.c = t6.c);
