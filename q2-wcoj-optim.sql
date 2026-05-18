CREATE VIEW R_1 AS SELECT col0 as a, col1 as b FROM R;
CREATE VIEW R_2 AS SELECT col0 as b, col1 as c FROM R;
CREATE VIEW R_3 AS SELECT col0 as a, col1 as d FROM R;
CREATE VIEW R_4 AS SELECT col0 as d, col1 as c FROM R;

CREATE VIEW R_1_cnt_b AS SELECT a, COUNT(DISTINCT b) as cnt FROM R_1 GROUP BY a;
CREATE VIEW R_1_cnt_a AS SELECT b, COUNT(DISTINCT a) as cnt FROM R_1 GROUP BY b;
CREATE VIEW R_2_cnt_c AS SELECT b, COUNT(DISTINCT c) as cnt FROM R_2 GROUP BY b;
CREATE VIEW R_2_cnt_b AS SELECT c, COUNT(DISTINCT b) as cnt FROM R_2 GROUP BY c;
CREATE VIEW R_3_cnt_d AS SELECT a, COUNT(DISTINCT d) as cnt FROM R_3 GROUP BY a;
CREATE VIEW R_3_cnt_a AS SELECT d, COUNT(DISTINCT a) as cnt FROM R_3 GROUP BY d;
CREATE VIEW R_4_cnt_c AS SELECT d, COUNT(DISTINCT c) as cnt FROM R_4 GROUP BY d;
CREATE VIEW R_4_cnt_d AS SELECT c, COUNT(DISTINCT d) as cnt FROM R_4 GROUP BY c;

CREATE TEMP TABLE _ab AS SELECT R_1.a as a, R_1.b as b FROM R_1 SEMI JOIN R_2 ON R_1.b = R_2.b SEMI JOIN R_3 ON R_1.a = R_3.a;

CREATE TEMP TABLE best_ab AS WITH
  r2cnt AS (SELECT * FROM R_2_cnt_c),
  r3cnt AS (SELECT * FROM R_3_cnt_d),
  best_r2 AS (SELECT _ab.a AS a, _ab.b AS b, 1 AS tag, r2cnt.cnt AS cnt FROM _ab JOIN r2cnt ON _ab.b = r2cnt.b),
  best_r3 AS (SELECT best_r2.a AS a, best_r2.b AS b,
    CASE WHEN best_r2.cnt < r3cnt.cnt THEN best_r2.tag ELSE 2 END AS tag,
    CASE WHEN best_r2.cnt < r3cnt.cnt THEN best_r2.cnt ELSE r3cnt.cnt END AS cnt
    FROM best_r2 JOIN r3cnt ON best_r2.a = r3cnt.a),
SELECT a, b, tag FROM best_r3;

DROP TABLE _ab;

CREATE TEMP TABLE _abc as (SELECT best_ab.a as a, best_ab.b as b, R_2.c as c FROM best_ab JOIN R_2 ON best_ab.b = R_2.b SEMI JOIN R_4 ON R_2.c = R_4.c WHERE best_ab.tag=1);

CREATE TEMP TABLE _abd AS (SELECT best_ab.a AS a, best_ab.b AS b, R_3.d AS d FROM best_ab JOIN R_3 ON best_ab.a = R_3.a SEMI JOIN R_4 ON R_3.d = R_4.d WHERE best_ab.tag = 2);

DROP TEMP TABLE best_ab;

CREATE TEMP TABLE best_abc AS WITH
  r3cnt AS (SELECT * FROM R_3_cnt_d),
  r4cnt AS (SELECT * FROM R_4_cnt_d),
  best_r3 AS (SELECT _abc.a AS a, _abc.b AS b, _abc.c AS c,
    3 AS tag,
    r3cnt.cnt AS cnt
    FROM _abc JOIN r3cnt ON _abc.a = r3cnt.a),
  best_r4 AS (SELECT best_r3.a AS a, best_r3.b AS b, best_r3.c AS c,
    CASE WHEN best_r3.cnt < r4cnt.cnt THEN best_r3.tag ELSE 4 END AS tag,
    CASE WHEN best_r3.cnt < r4cnt.cnt THEN best_r3.cnt ELSE r4cnt.cnt END AS cnt
    FROM best_r3 JOIN r4cnt ON best_r3.c = r4cnt.c)
SELECT a, b, c, tag FROM best_r4;

DROP TABLE _abc;

CREATE TEMP TABLE best_abd AS WITH
  r2cnt AS (SELECT * FROM R_2_cnt_c),
  r4cnt AS (SELECT * FROM R_4_cnt_c),
  best_r2 AS (SELECT _abd.a AS a, _abd.b AS b, _abd.d AS d,
    2 AS tag,
    r2cnt.cnt AS cnt
    FROM _abd JOIN r2cnt ON _abd.b = r2cnt.b),
  best_r4 AS (SELECT best_r2.a AS a, best_r2.b AS b, best_r2.d AS d,
    CASE WHEN best_r2.cnt < r4cnt.cnt THEN best_r2.tag ELSE 4 END AS tag,
    CASE WHEN best_r2.cnt < r4cnt.cnt THEN best_r2.cnt ELSE r4cnt.cnt END AS cnt
    FROM best_r2 JOIN r4cnt ON best_r2.d = r4cnt.d)
SELECT a, b, d, tag FROM best_r4;

DROP TEMP TABLE _abd;

CREATE TEMP TABLE __result__ AS SELECT COUNT(*) FROM (
  SELECT best_abc.a AS a, best_abc.b AS b, best_abc.c AS c, R_4.d AS d
    FROM best_abc JOIN R_4 ON best_abc.c = R_4.c
    SEMI JOIN R_3 ON R_3.a = best_abc.a AND R_3.d = R_4.d
    WHERE best_abc.tag = 4
  UNION ALL
  SELECT best_abc.a AS a, best_abc.b AS b, best_abc.c AS c, R_3.d AS d
    FROM best_abc JOIN R_3 ON best_abc.a = R_3.a
    SEMI JOIN R_4 ON R_4.d = R_3.d AND R_4.c = best_abc.c
    WHERE best_abc.tag = 3
  UNION ALL
  SELECT best_abd.a AS a, best_abd.b AS b, R_2.c AS c, best_abd.d AS d
    FROM best_abd JOIN R_2 ON best_abd.b = R_2.b
    SEMI JOIN R_4 ON R_4.c = R_2.c AND R_4.d = best_abd.d
    WHERE best_abd.tag = 2
  UNION ALL
  SELECT best_abd.a AS a, best_abd.b AS b, R_4.c AS c, best_abd.d AS d
    FROM best_abd JOIN R_4 ON best_abd.d = R_4.d
    SEMI JOIN R_2 ON R_2.c = R_4.c AND R_2.b = best_abd.b
    WHERE best_abd.tag = 4
);

SELECT * FROM __result__;

-- SELECT (SELECT COUNT(*) FROM _abc) + (SELECT COUNT(*) FROM _abd) AS total;

-- SELECT COUNT(*) FROM _ab;