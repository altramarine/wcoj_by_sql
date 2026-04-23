CREATE VIEW R_1 AS SELECT * FROM R;
CREATE VIEW R_2 AS SELECT * FROM R;
CREATE VIEW R_3 AS SELECT * FROM R;
CREATE VIEW R_4 AS SELECT * FROM R;
CREATE TEMP TABLE query_1000_ AS (SELECT R_1.col0 as a FROM R_1 SEMI JOIN R_2 ON R_2.col0 = R_1.col0 GROUP BY a);
CREATE TEMP TABLE best_1000_ as WITH 
count_1000_R_1_b AS (SELECT R_1.col0 as a, COUNT(DISTINCT R_1.col1) as cnt_b FROM R_1    GROUP BY  R_1.col0 ),
best_1000_R_1_b as (SELECT query_1000_.a as a, 1 as posi_tag, count_1000_R_1_b.cnt_b as cnt_tag FROM query_1000_, count_1000_R_1_b WHERE query_1000_.a = count_1000_R_1_b.a ) ,
count_1000_R_3_b AS (SELECT COUNT(DISTINCT R_3.col0) as cnt_b FROM R_3      ),
best_1000_R_3_b AS ( SELECT best_1000_R_1_b.a as a, CASE WHEN best_1000_R_1_b.cnt_tag < count_1000_R_3_b.cnt_b THEN best_1000_R_1_b.posi_tag ELSE 9 END as posi_tag, CASE WHEN best_1000_R_1_b.cnt_tag < count_1000_R_3_b.cnt_b THEN best_1000_R_1_b.cnt_tag ELSE count_1000_R_3_b.cnt_b END as cnt_tag FROM best_1000_R_1_b, count_1000_R_3_b   ) ,
SELECT best_1000_R_3_b.a as a, best_1000_R_3_b.posi_tag as posi_tag FROM best_1000_R_3_b
;
DROP TABLE query_1000_;
CREATE TEMP TABLE query_1100_ AS (
SELECT best_1000_.a as a, R_1.col1 as b FROM best_1000_ JOIN R_1 ON ( R_1.col0 = best_1000_.a AND best_1000_.posi_tag = 1 )  SEMI JOIN R_3 ON R_3.col0 = R_1.col1
 UNION ALL 
SELECT best_1000_.a as a, R_3.col0 as b FROM best_1000_ JOIN R_3 ON ( best_1000_.posi_tag = 9 )  SEMI JOIN R_1 ON R_1.col0 = best_1000_.a AND R_1.col1 = R_3.col0);
DROP TABLE best_1000_;
CREATE TEMP TABLE best_1100_ as WITH 
count_1100_R_2_c AS (SELECT R_2.col0 as a, COUNT(DISTINCT R_2.col1) as cnt_c FROM R_2    GROUP BY  R_2.col0 ),
best_1100_R_2_c as (SELECT query_1100_.a as a, query_1100_.b as b, 6 as posi_tag, count_1100_R_2_c.cnt_c as cnt_tag FROM query_1100_, count_1100_R_2_c WHERE query_1100_.a = count_1100_R_2_c.a ) ,
count_1100_R_4_c AS (SELECT COUNT(DISTINCT R_4.col1) as cnt_c FROM R_4      ),
best_1100_R_4_c AS ( SELECT best_1100_R_2_c.a as a, best_1100_R_2_c.b as b, CASE WHEN best_1100_R_2_c.cnt_tag < count_1100_R_4_c.cnt_c THEN best_1100_R_2_c.posi_tag ELSE 14 END as posi_tag, CASE WHEN best_1100_R_2_c.cnt_tag < count_1100_R_4_c.cnt_c THEN best_1100_R_2_c.cnt_tag ELSE count_1100_R_4_c.cnt_c END as cnt_tag FROM best_1100_R_2_c, count_1100_R_4_c   ) ,
SELECT best_1100_R_4_c.a as a, best_1100_R_4_c.b as b, best_1100_R_4_c.posi_tag as posi_tag FROM best_1100_R_4_c
;
DROP TABLE query_1100_;
CREATE TEMP TABLE query_1110_ AS (
SELECT best_1100_.a as a, best_1100_.b as b, R_2.col1 as c FROM best_1100_ JOIN R_2 ON ( R_2.col0 = best_1100_.a AND best_1100_.posi_tag = 6 )  SEMI JOIN R_4 ON R_4.col1 = R_2.col1
 UNION ALL 
SELECT best_1100_.a as a, best_1100_.b as b, R_4.col1 as c FROM best_1100_ JOIN R_4 ON ( best_1100_.posi_tag = 14 )  SEMI JOIN R_2 ON R_2.col0 = best_1100_.a AND R_2.col1 = R_4.col1);

CREATE TEMP TABLE query_0110_ as SELECT query_1110_.b as b, query_1110_.c as c FROM query_1110_ group by b, c;

DROP TABLE best_1100_;
CREATE TEMP TABLE best_0110_ as WITH
count_0110_R_3_d AS (SELECT R_3.col0 as b, COUNT(DISTINCT R_3.col1) as cnt_d FROM R_3    GROUP BY  R_3.col0 ),
best_0110_R_3_d as (SELECT query_0110_.b as b, query_0110_.c as c, 11 as posi_tag, count_0110_R_3_d.cnt_d as cnt_tag FROM query_0110_, count_0110_R_3_d WHERE query_0110_.b = count_0110_R_3_d.b ) ,
count_0110_R_4_d AS (SELECT R_4.col1 as c, COUNT(DISTINCT R_4.col0) as cnt_d FROM R_4    GROUP BY  R_4.col1 ),
best_0110_R_4_d AS ( SELECT best_0110_R_3_d.b as b, best_0110_R_3_d.c as c, CASE WHEN best_0110_R_3_d.cnt_tag < count_0110_R_4_d.cnt_d THEN best_0110_R_3_d.posi_tag ELSE 15 END as posi_tag, CASE WHEN best_0110_R_3_d.cnt_tag < count_0110_R_4_d.cnt_d THEN best_0110_R_3_d.cnt_tag ELSE count_0110_R_4_d.cnt_d END as cnt_tag FROM best_0110_R_3_d, count_0110_R_4_d WHERE best_0110_R_3_d.c = count_0110_R_4_d.c ) ,
SELECT best_0110_R_4_d.b as b, best_0110_R_4_d.c as c, best_0110_R_4_d.posi_tag as posi_tag FROM best_0110_R_4_d;
DROP TABLE query_0110_;

EXPLAIN ANALYZE CREATE TEMP TABLE query_1111_ as WITH
  query_0111_ AS (
  SELECT best_0110_.b as b, best_0110_.c as c, R_3.col1 as d FROM best_0110_ JOIN R_3 ON R_3.col0 = best_0110_.b AND best_0110_.posi_tag = 11 SEMI JOIN R_4 ON R_4.col0 = R_3.col1 AND R_4.col1 = best_0110_.c
  UNION ALL
  SELECT best_0110_.b as b, best_0110_.c as c, R_4.col0 as d FROM best_0110_ JOIN R_4 ON R_4.col1 = best_0110_.c AND best_0110_.posi_tag = 15 SEMI JOIN R_3 ON R_3.col0 = best_0110_.b AND R_3.col1 = R_4.col0),
SELECT SUM(a)+SUM(b)+SUM(c)+SUM(d) FROM (SELECT query_1110_.a as a, query_0111_.b as b, query_0111_.c as c, query_0111_.d as d FROM query_0111_, query_1110_,  WHERE query_1110_.b = query_0111_.b AND query_1110_.c = query_0111_.c);

DROP TABLE best_0110_;
DROP TABLE query_1110_;