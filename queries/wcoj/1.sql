CREATE VIEW R_1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R_2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R_3 AS SELECT col0 AS a, col1 AS c FROM R;

CREATE VIEW R_1_cnt_b AS SELECT a, COUNT(*) AS cnt FROM R_1 GROUP BY a;
CREATE VIEW R_1_cnt_a AS SELECT b, COUNT(*) AS cnt FROM R_1 GROUP BY b;
CREATE VIEW R_2_cnt_c AS SELECT b, COUNT(*) AS cnt FROM R_2 GROUP BY b;
CREATE VIEW R_2_cnt_b AS SELECT c, COUNT(*) AS cnt FROM R_2 GROUP BY c;
CREATE VIEW R_3_cnt_a AS SELECT c, COUNT(*) AS cnt FROM R_3 GROUP BY c;
CREATE VIEW R_3_cnt_c AS SELECT a, COUNT(*) AS cnt FROM R_3 GROUP BY a;

-- candidate (a,b) pairs: b in R_2, a in R_3
CREATE TEMP TABLE _ab AS
  SELECT R_1.a AS a, R_1.b AS b FROM R_1
  SEMI JOIN R_2 ON R_1.b = R_2.b
  SEMI JOIN R_3 ON R_1.a = R_3.a;

-- for each (a,b), choose cheapest next: c via R_2(b,c) (tag=1) or c via R_3(c,a) (tag=2)
CREATE TEMP TABLE best_ab AS WITH
  r2cnt AS (SELECT * FROM R_2_cnt_c),
  r3cnt AS (SELECT * FROM R_3_cnt_c),
  best_r2 AS (
    SELECT _ab.a AS a, _ab.b AS b, 1 AS tag, r2cnt.cnt AS cnt
    FROM _ab JOIN r2cnt ON _ab.b = r2cnt.b),
  best_r3 AS (
    SELECT best_r2.a AS a, best_r2.b AS b,
      CASE WHEN best_r2.cnt < r3cnt.cnt THEN best_r2.tag ELSE 2 END AS tag,
      CASE WHEN best_r2.cnt < r3cnt.cnt THEN best_r2.cnt ELSE r3cnt.cnt END AS cnt
    FROM best_r2 JOIN r3cnt ON best_r2.a = r3cnt.a)
SELECT a, b, tag FROM best_r3;

SELECT COUNT(*) FROM (
  -- tag=1: extend via R_2(b,c), verify closing edge R_3(c,a)
  SELECT best_ab.a AS a, best_ab.b AS b, R_2.c AS c
  FROM best_ab JOIN R_2 ON best_ab.b = R_2.b
  SEMI JOIN R_3 ON R_2.c = R_3.c AND R_3.a = best_ab.a
  WHERE best_ab.tag = 1
  UNION ALL
  -- tag=2: extend via R_3(c,a), verify closing edge R_2(b,c)
  SELECT best_ab.a AS a, best_ab.b AS b, R_3.c AS c
  FROM best_ab JOIN R_3 ON best_ab.a = R_3.a
  SEMI JOIN R_2 ON R_3.c = R_2.c AND R_2.b = best_ab.b
  WHERE best_ab.tag = 2
);
DROP TABLE best_ab;
