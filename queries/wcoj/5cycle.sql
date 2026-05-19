CREATE VIEW R_1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R_2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R_3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R_4 AS SELECT col0 AS d, col1 AS e FROM R;
CREATE VIEW R_5 AS SELECT col0 AS e, col1 AS a FROM R;

CREATE VIEW R_1_cnt_b AS SELECT a, COUNT(DISTINCT b) AS cnt FROM R_1 GROUP BY a;
CREATE VIEW R_1_cnt_a AS SELECT b, COUNT(DISTINCT a) AS cnt FROM R_1 GROUP BY b;
CREATE VIEW R_2_cnt_c AS SELECT b, COUNT(DISTINCT c) AS cnt FROM R_2 GROUP BY b;
CREATE VIEW R_2_cnt_b AS SELECT c, COUNT(DISTINCT b) AS cnt FROM R_2 GROUP BY c;
CREATE VIEW R_3_cnt_d AS SELECT c, COUNT(DISTINCT d) AS cnt FROM R_3 GROUP BY c;
CREATE VIEW R_3_cnt_c AS SELECT d, COUNT(DISTINCT c) AS cnt FROM R_3 GROUP BY d;
CREATE VIEW R_4_cnt_e AS SELECT d, COUNT(DISTINCT e) AS cnt FROM R_4 GROUP BY d;
CREATE VIEW R_4_cnt_d AS SELECT e, COUNT(DISTINCT d) AS cnt FROM R_4 GROUP BY e;
CREATE VIEW R_5_cnt_a AS SELECT e, COUNT(DISTINCT a) AS cnt FROM R_5 GROUP BY e;
CREATE VIEW R_5_cnt_e AS SELECT a, COUNT(DISTINCT e) AS cnt FROM R_5 GROUP BY a;

-- candidate (a,b) pairs: b in R_2, a in R_5
CREATE TEMP TABLE _ab AS
  SELECT R_1.a AS a, R_1.b AS b FROM R_1
  SEMI JOIN R_2 ON R_1.b = R_2.b
  SEMI JOIN R_5 ON R_1.a = R_5.a;

-- for each (a,b), choose cheapest next: c via R_2 (tag=1) or e via R_5 (tag=2)
CREATE TEMP TABLE best_ab AS WITH
  r2cnt AS (SELECT * FROM R_2_cnt_c),
  r5cnt AS (SELECT * FROM R_5_cnt_e),
  best_r2 AS (
    SELECT _ab.a AS a, _ab.b AS b, 1 AS tag, r2cnt.cnt AS cnt
    FROM _ab JOIN r2cnt ON _ab.b = r2cnt.b),
  best_r5 AS (
    SELECT best_r2.a AS a, best_r2.b AS b,
      CASE WHEN best_r2.cnt < r5cnt.cnt THEN best_r2.tag ELSE 2 END AS tag,
      CASE WHEN best_r2.cnt < r5cnt.cnt THEN best_r2.cnt ELSE r5cnt.cnt END AS cnt
    FROM best_r2 JOIN r5cnt ON best_r2.a = r5cnt.a)
SELECT a, b, tag FROM best_r5;
DROP TABLE _ab;

-- tag=1: extend (a,b) -> (a,b,c), c must appear in R_3
CREATE TEMP TABLE _abc AS
  SELECT best_ab.a AS a, best_ab.b AS b, R_2.c AS c
  FROM best_ab JOIN R_2 ON best_ab.b = R_2.b
  SEMI JOIN R_3 ON R_2.c = R_3.c
  WHERE best_ab.tag = 1;

-- tag=2: extend (a,b) -> (a,b,e), e must appear in R_4
CREATE TEMP TABLE _abe AS
  SELECT best_ab.a AS a, best_ab.b AS b, R_5.e AS e
  FROM best_ab JOIN R_5 ON best_ab.a = R_5.a
  SEMI JOIN R_4 ON R_5.e = R_4.e
  WHERE best_ab.tag = 2;
DROP TABLE best_ab;

-- case _abc, start from (d,e): candidate pairs, d in R_3, e in R_5
CREATE TEMP TABLE _de AS
  SELECT R_4.d AS d, R_4.e AS e FROM R_4
  SEMI JOIN R_3 ON R_4.d = R_3.d
  SEMI JOIN R_5 ON R_4.e = R_5.e;

-- for each (d,e), choose cheapest next: c via R_3 (tag=3) or a via R_5 (tag=4)
CREATE TEMP TABLE best_de AS WITH
  r3cnt AS (SELECT * FROM R_3_cnt_c),
  r5cnt AS (SELECT * FROM R_5_cnt_a),
  best_r3 AS (
    SELECT _de.d AS d, _de.e AS e, 3 AS tag, r3cnt.cnt AS cnt
    FROM _de JOIN r3cnt ON _de.d = r3cnt.d),
  best_r5 AS (
    SELECT best_r3.d AS d, best_r3.e AS e,
      CASE WHEN best_r3.cnt < r5cnt.cnt THEN best_r3.tag ELSE 4 END AS tag,
      CASE WHEN best_r3.cnt < r5cnt.cnt THEN best_r3.cnt ELSE r5cnt.cnt END AS cnt
    FROM best_r3 JOIN r5cnt ON best_r3.e = r5cnt.e)
SELECT d, e, tag FROM best_r5;
DROP TABLE _de;

-- tag=3: extend (d,e) -> (c,d,e), c must appear in R_2
CREATE TEMP TABLE _cde AS
  SELECT R_3.c AS c, best_de.d AS d, best_de.e AS e
  FROM best_de JOIN R_3 ON best_de.d = R_3.d
  SEMI JOIN R_2 ON R_3.c = R_2.c
  WHERE best_de.tag = 3;

-- tag=4: extend (d,e) -> (d,e,a), a must appear in R_1
CREATE TEMP TABLE _dea AS
  SELECT best_de.d AS d, best_de.e AS e, R_5.a AS a
  FROM best_de JOIN R_5 ON best_de.e = R_5.e
  SEMI JOIN R_1 ON R_5.a = R_1.a
  WHERE best_de.tag = 4;
DROP TABLE best_de;

-- start from (c,d): candidate pairs, c in R_2, d in R_4
CREATE TEMP TABLE _cd AS
  SELECT R_3.c AS c, R_3.d AS d FROM R_3
  SEMI JOIN R_2 ON R_3.c = R_2.c
  SEMI JOIN R_4 ON R_3.d = R_4.d;

-- for each (c,d), choose cheapest next: e via R_4 (tag=5) or b via R_2 (tag=6)
CREATE TEMP TABLE best_cd AS WITH
  r4cnt AS (SELECT * FROM R_4_cnt_e),
  r2cnt AS (SELECT * FROM R_2_cnt_b),
  best_r4 AS (
    SELECT _cd.c AS c, _cd.d AS d, 5 AS tag, r4cnt.cnt AS cnt
    FROM _cd JOIN r4cnt ON _cd.d = r4cnt.d),
  best_r2 AS (
    SELECT best_r4.c AS c, best_r4.d AS d,
      CASE WHEN best_r4.cnt < r2cnt.cnt THEN best_r4.tag ELSE 6 END AS tag,
      CASE WHEN best_r4.cnt < r2cnt.cnt THEN best_r4.cnt ELSE r2cnt.cnt END AS cnt
    FROM best_r4 JOIN r2cnt ON best_r4.c = r2cnt.c)
SELECT c, d, tag FROM best_r2;
DROP TABLE _cd;

-- tag=5: extend (c,d) -> (c,d,e), e must appear in R_5
CREATE TEMP TABLE _cde2 AS
  SELECT best_cd.c AS c, best_cd.d AS d, R_4.e AS e
  FROM best_cd JOIN R_4 ON best_cd.d = R_4.d
  SEMI JOIN R_5 ON R_4.e = R_5.e
  WHERE best_cd.tag = 5;

-- tag=6: extend (c,d) -> (b,c,d), b must appear in R_1
CREATE TEMP TABLE _bcd AS
  SELECT R_2.b AS b, best_cd.c AS c, best_cd.d AS d
  FROM best_cd JOIN R_2 ON best_cd.c = R_2.c
  SEMI JOIN R_1 ON R_2.b = R_1.b
  WHERE best_cd.tag = 6;
DROP TABLE best_cd;

-- Case 1: _abc(a,b,c) + _cde(c,d,e), missing edge R_5(e,a), split on ea
CREATE TEMP TABLE best_ea_1 AS WITH
  abc_cnt AS (SELECT a, COUNT(DISTINCT (b, c)) AS cnt FROM _abc GROUP BY a),
  cde_cnt AS (SELECT e, COUNT(DISTINCT (c, d)) AS cnt FROM _cde GROUP BY e),
  best_cde AS (SELECT R_5.e AS e, R_5.a AS a, 1 AS tag, cde_cnt.cnt AS cnt
    FROM R_5 JOIN cde_cnt ON R_5.e = cde_cnt.e),
  best_abc AS (SELECT best_cde.e AS e, best_cde.a AS a,
    CASE WHEN best_cde.cnt < abc_cnt.cnt THEN best_cde.tag ELSE 2 END AS tag,
    CASE WHEN best_cde.cnt < abc_cnt.cnt THEN best_cde.cnt ELSE abc_cnt.cnt END AS cnt
    FROM best_cde JOIN abc_cnt ON best_cde.a = abc_cnt.a)
SELECT e, a, tag FROM best_abc;

-- Case 2: _abc(a,b,c) + _dea(d,e,a), missing edge R_3(c,d), split on cd
CREATE TEMP TABLE best_cd_2 AS WITH
  abc_cnt AS (SELECT c, COUNT(DISTINCT (a, b)) AS cnt FROM _abc GROUP BY c),
  dea_cnt AS (SELECT d, COUNT(DISTINCT (e, a)) AS cnt FROM _dea GROUP BY d),
  best_abc AS (SELECT R_3.c AS c, R_3.d AS d, 1 AS tag, abc_cnt.cnt AS cnt
    FROM R_3 JOIN abc_cnt ON R_3.c = abc_cnt.c),
  best_dea AS (SELECT best_abc.c AS c, best_abc.d AS d,
    CASE WHEN best_abc.cnt < dea_cnt.cnt THEN best_abc.tag ELSE 2 END AS tag,
    CASE WHEN best_abc.cnt < dea_cnt.cnt THEN best_abc.cnt ELSE dea_cnt.cnt END AS cnt
    FROM best_abc JOIN dea_cnt ON best_abc.d = dea_cnt.d)
SELECT c, d, tag FROM best_dea;

-- Case 3: _abe(a,b,e) + _cde2(c,d,e), missing edge R_2(b,c), split on bc
CREATE TEMP TABLE best_bc_3 AS WITH
  abe_cnt AS (SELECT b, COUNT(DISTINCT (a, e)) AS cnt FROM _abe GROUP BY b),
  cde2_cnt AS (SELECT c, COUNT(DISTINCT (d, e)) AS cnt FROM _cde2 GROUP BY c),
  best_abe AS (SELECT R_2.b AS b, R_2.c AS c, 1 AS tag, abe_cnt.cnt AS cnt
    FROM R_2 JOIN abe_cnt ON R_2.b = abe_cnt.b),
  best_cde2 AS (SELECT best_abe.b AS b, best_abe.c AS c,
    CASE WHEN best_abe.cnt < cde2_cnt.cnt THEN best_abe.tag ELSE 2 END AS tag,
    CASE WHEN best_abe.cnt < cde2_cnt.cnt THEN best_abe.cnt ELSE cde2_cnt.cnt END AS cnt
    FROM best_abe JOIN cde2_cnt ON best_abe.c = cde2_cnt.c)
SELECT b, c, tag FROM best_cde2;

-- Case 4: _abe(a,b,e) + _bcd(b,c,d), missing edge R_4(d,e), split on de
CREATE TEMP TABLE best_de_4 AS WITH
  bcd_cnt AS (SELECT d, COUNT(DISTINCT (b, c)) AS cnt FROM _bcd GROUP BY d),
  abe_cnt AS (SELECT e, COUNT(DISTINCT (a, b)) AS cnt FROM _abe GROUP BY e),
  best_bcd AS (SELECT R_4.d AS d, R_4.e AS e, 1 AS tag, bcd_cnt.cnt AS cnt
    FROM R_4 JOIN bcd_cnt ON R_4.d = bcd_cnt.d),
  best_abe AS (SELECT best_bcd.d AS d, best_bcd.e AS e,
    CASE WHEN best_bcd.cnt < abe_cnt.cnt THEN best_bcd.tag ELSE 2 END AS tag,
    CASE WHEN best_bcd.cnt < abe_cnt.cnt THEN best_bcd.cnt ELSE abe_cnt.cnt END AS cnt
    FROM best_bcd JOIN abe_cnt ON best_bcd.e = abe_cnt.e)
SELECT d, e, tag FROM best_abe;

SELECT COUNT(*) FROM (
  -- Case 1: _abc(a,b,c) + _cde(c,d,e), missing edge R_5(e,a)
  -- tag=1: _cde cheaper -> enumerate _cde, join _abc on a
  SELECT _abc.a AS a, _abc.b AS b, _abc.c AS c, _cde.d AS d, _cde.e AS e
    FROM best_ea_1 JOIN _cde ON best_ea_1.e = _cde.e
    JOIN _abc ON best_ea_1.a = _abc.a AND _cde.c = _abc.c
    WHERE best_ea_1.tag = 1
  UNION ALL
  -- tag=2: _abc cheaper -> enumerate _abc, join _cde on a
  SELECT _abc.a AS a, _abc.b AS b, _abc.c AS c, _cde.d AS d, _cde.e AS e
    FROM best_ea_1 JOIN _abc ON best_ea_1.a = _abc.a
    JOIN _cde ON best_ea_1.e = _cde.e AND _cde.c = _abc.c
    WHERE best_ea_1.tag = 2
  UNION ALL
  -- Case 2: _abc(a,b,c) + _dea(d,e,a), missing edge R_3(c,d)
  -- tag=1: _abc cheaper -> enumerate _abc, join _dea on d
  SELECT _abc.a AS a, _abc.b AS b, _abc.c AS c, _dea.d AS d, _dea.e AS e
    FROM best_cd_2 JOIN _abc ON best_cd_2.c = _abc.c
    JOIN _dea ON best_cd_2.d = _dea.d AND _dea.a = _abc.a
    WHERE best_cd_2.tag = 1
  UNION ALL
  -- tag=2: _dea cheaper -> enumerate _dea, join _abc on c
  SELECT _abc.a AS a, _abc.b AS b, _abc.c AS c, _dea.d AS d, _dea.e AS e
    FROM best_cd_2 JOIN _dea ON best_cd_2.d = _dea.d
    JOIN _abc ON best_cd_2.c = _abc.c AND _abc.a = _dea.a
    WHERE best_cd_2.tag = 2
  UNION ALL
  -- Case 3: _abe(a,b,e) + _cde2(c,d,e), missing edge R_2(b,c)
  -- tag=1: _abe cheaper -> enumerate _abe, join _cde2 on c
  SELECT _abe.a AS a, _abe.b AS b, _cde2.c AS c, _cde2.d AS d, _cde2.e AS e
    FROM best_bc_3 JOIN _abe ON best_bc_3.b = _abe.b
    JOIN _cde2 ON best_bc_3.c = _cde2.c AND _cde2.e = _abe.e
    WHERE best_bc_3.tag = 1
  UNION ALL
  -- tag=2: _cde2 cheaper -> enumerate _cde2, join _abe on b
  SELECT _abe.a AS a, _abe.b AS b, _cde2.c AS c, _cde2.d AS d, _cde2.e AS e
    FROM best_bc_3 JOIN _cde2 ON best_bc_3.c = _cde2.c
    JOIN _abe ON best_bc_3.b = _abe.b AND _abe.e = _cde2.e
    WHERE best_bc_3.tag = 2
  UNION ALL
  -- Case 4: _abe(a,b,e) + _bcd(b,c,d), missing edge R_4(d,e)
  -- tag=1: _bcd cheaper -> enumerate _bcd, join _abe on e
  SELECT _abe.a AS a, _bcd.b AS b, _bcd.c AS c, _bcd.d AS d, _abe.e AS e
    FROM best_de_4 JOIN _bcd ON best_de_4.d = _bcd.d
    JOIN _abe ON best_de_4.e = _abe.e AND _abe.b = _bcd.b
    WHERE best_de_4.tag = 1
  UNION ALL
  -- tag=2: _abe cheaper -> enumerate _abe, join _bcd on d
  SELECT _abe.a AS a, _bcd.b AS b, _bcd.c AS c, _bcd.d AS d, _abe.e AS e
    FROM best_de_4 JOIN _abe ON best_de_4.e = _abe.e
    JOIN _bcd ON best_de_4.d = _bcd.d AND _bcd.b = _abe.b
    WHERE best_de_4.tag = 2
);
DROP TABLE _abc;
DROP TABLE _abe;
DROP TABLE _dea;
DROP TABLE _cde;
DROP TABLE _cde2;
DROP TABLE _bcd;
DROP TABLE best_ea_1;
DROP TABLE best_cd_2;
DROP TABLE best_bc_3;
DROP TABLE best_de_4;
