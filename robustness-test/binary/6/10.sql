CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS c FROM R;
CREATE VIEW R4 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R5 AS SELECT col0 AS b, col1 AS d FROM R;
CREATE VIEW R6 AS SELECT col0 AS c, col1 AS d FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (((R5 t5 JOIN R1 t1 ON t5.b = t1.b) JOIN (R4 t4 CROSS JOIN R2 t2) ON t5.d = t4.d AND t5.b = t2.b AND t1.a = t4.a AND t1.b = t2.b) JOIN (R6 t6 JOIN R3 t3 ON t6.c = t3.c) ON t5.d = t6.d AND t1.a = t3.a AND t4.d = t6.d AND t4.a = t3.a AND t2.c = t6.c AND t2.c = t3.c);
