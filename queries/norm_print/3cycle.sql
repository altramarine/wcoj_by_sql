CREATE VIEW R_1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R_2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R_3 AS SELECT col0 AS a, col1 AS c FROM R;

-- Lp norms of R_1: degree(a) and degree(b)
SELECT
  'R_1 degree(a)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT a, COUNT(*) AS cnt FROM R_1 GROUP BY a)
UNION ALL
SELECT
  'R_1 degree(b)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT b, COUNT(*) AS cnt FROM R_1 GROUP BY b);

-- for each (a,b) in R_1, choose cheapest next: c via R_2(b,c) (tag=1) or c via R_3(c,a) (tag=2)
CREATE TEMP TABLE best_ab AS WITH
  r2cnt AS (SELECT b, COUNT(*) AS cnt FROM R_2 GROUP BY b),
  r3cnt AS (SELECT a, COUNT(*) AS cnt FROM R_3 GROUP BY a),
  best_r2 AS (
    SELECT R_1.a AS a, R_1.b AS b, 1 AS tag, r2cnt.cnt AS cnt
    FROM R_1 JOIN r2cnt ON R_1.b = r2cnt.b),
  best_r3 AS (
    SELECT best_r2.a AS a, best_r2.b AS b,
      CASE WHEN best_r2.cnt < r3cnt.cnt THEN best_r2.tag ELSE 2 END AS tag,
      CASE WHEN best_r2.cnt < r3cnt.cnt THEN best_r2.cnt ELSE r3cnt.cnt END AS cnt
    FROM best_r2 JOIN r3cnt ON best_r2.a = r3cnt.a)
SELECT a, b, tag FROM best_r3;

-- Lp norms for best_ab tag=1 and tag=2: degree(a) and degree(b)
SELECT
  'best_ab tag=1 degree(a)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT a, COUNT(*) AS cnt FROM best_ab WHERE tag = 1 GROUP BY a)
UNION ALL
SELECT
  'best_ab tag=1 degree(b)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT b, COUNT(*) AS cnt FROM best_ab WHERE tag = 1 GROUP BY b)
UNION ALL
SELECT
  'best_ab tag=2 degree(a)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT a, COUNT(*) AS cnt FROM best_ab WHERE tag = 2 GROUP BY a)
UNION ALL
SELECT
  'best_ab tag=2 degree(b)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT b, COUNT(*) AS cnt FROM best_ab WHERE tag = 2 GROUP BY b);

-- Baseline Lp norms after ordinary two-way joins, before any split.
CREATE VIEW r1_join_r2 AS
  SELECT R_1.a AS a, R_1.b AS b, R_2.c AS c
  FROM R_1 JOIN R_2 ON R_1.b = R_2.b;

CREATE VIEW r1_join_r3 AS
  SELECT R_1.a AS a, R_1.b AS b, R_3.c AS c
  FROM R_1 JOIN R_3 ON R_1.a = R_3.a;

SELECT
  'R_1 join R_2 degree(a)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT a, COUNT(*) AS cnt FROM r1_join_r2 GROUP BY a)
UNION ALL
SELECT
  'R_1 join R_2 degree(b)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT b, COUNT(*) AS cnt FROM r1_join_r2 GROUP BY b)
UNION ALL
SELECT
  'R_1 join R_2 degree(c)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT c, COUNT(*) AS cnt FROM r1_join_r2 GROUP BY c);

SELECT
  'R_1 join R_3 degree(a)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT a, COUNT(*) AS cnt FROM r1_join_r3 GROUP BY a)
UNION ALL
SELECT
  'R_1 join R_3 degree(b)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT b, COUNT(*) AS cnt FROM r1_join_r3 GROUP BY b)
UNION ALL
SELECT
  'R_1 join R_3 degree(c)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT c, COUNT(*) AS cnt FROM r1_join_r3 GROUP BY c);

-- Option 2a: Lp norms after split and chosen expansion join, before the closing semijoin.
CREATE VIEW best_ab_join_r2 AS
  SELECT best_ab.a AS a, best_ab.b AS b, R_2.c AS c
  FROM best_ab JOIN R_2 ON best_ab.b = R_2.b
  WHERE best_ab.tag = 1;

CREATE VIEW best_ab_join_r3 AS
  SELECT best_ab.a AS a, best_ab.b AS b, R_3.c AS c
  FROM best_ab JOIN R_3 ON best_ab.a = R_3.a
  WHERE best_ab.tag = 2;

SELECT
  'best_ab join R_2 tag=1 degree(a)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT a, COUNT(*) AS cnt FROM best_ab_join_r2 GROUP BY a)
UNION ALL
SELECT
  'best_ab join R_2 tag=1 degree(b)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT b, COUNT(*) AS cnt FROM best_ab_join_r2 GROUP BY b)
UNION ALL
SELECT
  'best_ab join R_2 tag=1 degree(c)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT c, COUNT(*) AS cnt FROM best_ab_join_r2 GROUP BY c);

SELECT
  'best_ab join R_3 tag=2 degree(a)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT a, COUNT(*) AS cnt FROM best_ab_join_r3 GROUP BY a)
UNION ALL
SELECT
  'best_ab join R_3 tag=2 degree(b)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT b, COUNT(*) AS cnt FROM best_ab_join_r3 GROUP BY b)
UNION ALL
SELECT
  'best_ab join R_3 tag=2 degree(c)' AS grp,
  COUNT(*)                                   AS l0,
  SUM(cnt)                                   AS l1,
  ROUND(POWER(SUM(POWER(cnt, 2)), 1.0/2), 4) AS l2,
  MAX(cnt)                                   AS linf
FROM (SELECT c, COUNT(*) AS cnt FROM best_ab_join_r3 GROUP BY c);

DROP VIEW best_ab_join_r3;
DROP VIEW best_ab_join_r2;
DROP VIEW r1_join_r3;
DROP VIEW r1_join_r2;

DROP TABLE best_ab;
