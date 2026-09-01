CREATE VIEW R1 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R2 AS SELECT col0 AS a, col1 AS d FROM R;
CREATE VIEW R3 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R4 AS SELECT col0 AS b, col1 AS c FROM R;

SET disabled_optimizers = 'join_order, build_side_probe_side';
SELECT COUNT(*) FROM (((R4 t4 JOIN R1 t1 ON t4.c = t1.c) JOIN R3 t3 ON t4.b = t3.b) JOIN R2 t2 ON t1.d = t2.d AND t3.a = t2.a);
