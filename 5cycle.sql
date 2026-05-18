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

CREATE TEMP TABLE _abc as (SELECT best_ab.a as a, best_ab.b as b, R_2.c as c FROM best_ab JOIN R_2 ON best_ab.b = R_2.b SEMI JOIN R_4 ON R_2.c = R_4.c WHERE best_ab.tag=1);

CREATE TEMP TABLE _abd AS (SELECT best_ab.a AS a, best_ab.b AS b, R_3.d AS d FROM best_ab JOIN R_3 ON best_ab.a = R_3.a SEMI JOIN R_4 ON R_3.d = R_4.d WHERE best_ab.tag = 2);

-- start from cd:

CREATE TEMP TABLE _cd AS SELECT R_4.d AS d, R_4.c AS c FROM R_4 SEMI JOIN R_2 ON R_4.c = R_2.c SEMI JOIN R_3 ON R_4.d = R_3.d;

CREATE TEMP TABLE best_cd_abc_ad AS WITH
  ad_cnt AS (SELECT * FROM R_3_cnt_a),
  abc_cnt AS (SELECT c, COUNT(DISTINCT (a, b)) AS cnt FROM _abc GROUP BY c),
  best_r1 AS (SELECT _cd.d AS d, _cd.c AS c, 3 AS tag, abc_cnt.cnt AS cnt FROM _cd JOIN abc_cnt ON _cd.c = abc_cnt.c),
  best_r2 AS (SELECT best_r1.d AS d, best_r1.c AS c,
    CASE WHEN best_r1.cnt < ad_cnt.cnt THEN best_r1.tag ELSE 6 END AS tag,
    CASE WHEN best_r1.cnt < ad_cnt.cnt THEN best_r1.cnt ELSE ad_cnt.cnt END AS cnt
    FROM best_r1 JOIN ad_cnt ON best_r1.d = ad_cnt.d)
SELECT d, c, tag FROM best_r2;

CREATE TEMP TABLE best_cd_abd_bc AS WITH
  abd_cnt AS (SELECT d, COUNT(DISTINCT (a, b)) AS cnt FROM _abd GROUP BY d),
  bc_cnt AS (SELECT * FROM R_2_cnt_b),
  best_r1 AS (SELECT _cd.d AS d, _cd.c AS c, 4 AS tag, abd_cnt.cnt AS cnt FROM _cd JOIN abd_cnt ON _cd.d = abd_cnt.d),
  best_r2 AS (SELECT best_r1.d AS d, best_r1.c AS c,
    CASE WHEN best_r1.cnt < bc_cnt.cnt THEN best_r1.tag ELSE 5 END AS tag,
    CASE WHEN best_r1.cnt < bc_cnt.cnt THEN best_r1.cnt ELSE bc_cnt.cnt END AS cnt
    FROM best_r1 JOIN bc_cnt ON best_r1.c = bc_cnt.c)
SELECT d, c, tag FROM best_r2;

SELECT COUNT(*) FROM (
  SELECT _abc.a AS a, _abc.b AS b, _abc.c AS c, best_cd_abc_ad.d AS d
    FROM best_cd_abc_ad JOIN _abc ON best_cd_abc_ad.c = _abc.c
    SEMI JOIN R_3 ON R_3.a = _abc.a AND R_3.d = best_cd_abc_ad.d
    WHERE best_cd_abc_ad.tag = 3
  UNION ALL
  SELECT _abc.a AS a, _abc.b AS b, best_cd_abc_ad.c AS c, R_3.d AS d
    FROM best_cd_abc_ad JOIN R_3 ON best_cd_abc_ad.d = R_3.d
    JOIN _abc ON _abc.c = best_cd_abc_ad.c AND _abc.a = R_3.a
    WHERE best_cd_abc_ad.tag = 6
  UNION ALL
  SELECT _abd.a AS a, _abd.b AS b, best_cd_abd_bc.c AS c, _abd.d AS d
    FROM best_cd_abd_bc JOIN _abd ON best_cd_abd_bc.d = _abd.d
    SEMI JOIN R_2 ON R_2.c = best_cd_abd_bc.c AND R_2.b = _abd.b
    WHERE best_cd_abd_bc.tag = 4
  UNION ALL
  SELECT _abd.a AS a, R_2.b AS b, best_cd_abd_bc.c AS c, _abd.d AS d
    FROM best_cd_abd_bc JOIN R_2 ON best_cd_abd_bc.c = R_2.c
    JOIN _abd ON _abd.d = best_cd_abd_bc.d AND _abd.b = R_2.b
    WHERE best_cd_abd_bc.tag = 5
);

-- (SELECT COUNT(*) FROM best_ab JOIN R_2 ON best_ab.b = R_2.b SEMI JOIN R_4 ON R_2.c = R_4.c);

-- SELECT COUNT(*) FROM _ab;