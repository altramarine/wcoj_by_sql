CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (((R5 t5 JOIN (R4 t4 JOIN R3 t3 ON t4.a = t3.a) ON t5.d = t4.d) JOIN (R2 t2 JOIN R6 t6 ON t2.c = t6.c) ON t5.b = t2.b AND t5.d = t6.d AND t4.d = t6.d AND t3.c = t2.c AND t3.c = t6.c) JOIN R1 t1 ON t5.b = t1.b AND t4.a = t1.a AND t3.a = t1.a AND t2.b = t1.b);
