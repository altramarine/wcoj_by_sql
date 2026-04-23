CREATE VIEW R_1 AS SELECT * FROM R;
CREATE VIEW R_2 AS SELECT * FROM R;
CREATE VIEW R_3 AS SELECT * FROM R;
CREATE VIEW R_4 AS SELECT * FROM R;
CREATE TEMP TABLE query_1000_ AS (SELECT R_1.col0 as a FROM R_1 SEMI JOIN R_4 ON R_4.col0 = R_1.col0 GROUP BY a);
CREATE TEMP TABLE best_1000_ as WITH 
count_1000_R_1_b AS (SELECT R_1.col0 as a, COUNT(DISTINCT R_1.col1) as cnt_b FROM R_1    GROUP BY  R_1.col0 ),
best_1000_R_1_b as (SELECT query_1000_.a as a, 1 as posi_tag, count_1000_R_1_b.cnt_b as cnt_tag FROM query_1000_, count_1000_R_1_b WHERE query_1000_.a = count_1000_R_1_b.a ) ,
count_1000_R_2_b AS (SELECT COUNT(DISTINCT R_2.col0) as cnt_b FROM R_2      ),
best_1000_R_2_b AS ( SELECT best_1000_R_1_b.a as a, CASE WHEN best_1000_R_1_b.cnt_tag < count_1000_R_2_b.cnt_b THEN best_1000_R_1_b.posi_tag ELSE 5 END as posi_tag, CASE WHEN best_1000_R_1_b.cnt_tag < count_1000_R_2_b.cnt_b THEN best_1000_R_1_b.cnt_tag ELSE count_1000_R_2_b.cnt_b END as cnt_tag FROM best_1000_R_1_b, count_1000_R_2_b   ) ,
SELECT best_1000_R_2_b.a as a, best_1000_R_2_b.posi_tag as posi_tag FROM best_1000_R_2_b
;
DROP TABLE query_1000_;
CREATE TEMP TABLE query_1100_ AS (
SELECT best_1000_.a as a, R_1.col1 as b FROM best_1000_ JOIN R_1 ON ( R_1.col0 = best_1000_.a AND best_1000_.posi_tag = 1 )  SEMI JOIN R_2 ON R_2.col0 = R_1.col1
 UNION ALL 
SELECT best_1000_.a as a, R_2.col0 as b FROM best_1000_ JOIN R_2 ON ( best_1000_.posi_tag = 5 )  SEMI JOIN R_1 ON R_1.col0 = best_1000_.a AND R_1.col1 = R_2.col0);
DROP TABLE best_1000_;
CREATE TEMP TABLE best_1100_ as WITH 
count_1100_R_2_d AS (SELECT R_2.col0 as b, COUNT(DISTINCT R_2.col1) as cnt_d FROM R_2    GROUP BY  R_2.col0 ),
best_1100_R_2_d as (SELECT query_1100_.a as a, query_1100_.b as b, 7 as posi_tag, count_1100_R_2_d.cnt_d as cnt_tag FROM query_1100_, count_1100_R_2_d WHERE query_1100_.b = count_1100_R_2_d.b ) ,
count_1100_R_3_d AS (SELECT COUNT(DISTINCT R_3.col0) as cnt_d FROM R_3      ),
best_1100_R_3_d AS ( SELECT best_1100_R_2_d.a as a, best_1100_R_2_d.b as b, CASE WHEN best_1100_R_2_d.cnt_tag < count_1100_R_3_d.cnt_d THEN best_1100_R_2_d.posi_tag ELSE 11 END as posi_tag, CASE WHEN best_1100_R_2_d.cnt_tag < count_1100_R_3_d.cnt_d THEN best_1100_R_2_d.cnt_tag ELSE count_1100_R_3_d.cnt_d END as cnt_tag FROM best_1100_R_2_d, count_1100_R_3_d   ) ,
SELECT best_1100_R_3_d.a as a, best_1100_R_3_d.b as b, best_1100_R_3_d.posi_tag as posi_tag FROM best_1100_R_3_d
;
DROP TABLE query_1100_;
CREATE TEMP TABLE query_1101_ AS (
SELECT best_1100_.a as a, best_1100_.b as b, R_2.col1 as d FROM best_1100_ JOIN R_2 ON ( R_2.col0 = best_1100_.b AND best_1100_.posi_tag = 7 )  SEMI JOIN R_3 ON R_3.col0 = R_2.col1
 UNION ALL 
SELECT best_1100_.a as a, best_1100_.b as b, R_3.col0 as d FROM best_1100_ JOIN R_3 ON ( best_1100_.posi_tag = 11 )  SEMI JOIN R_2 ON R_2.col0 = best_1100_.b AND R_2.col1 = R_3.col0);
DROP TABLE best_1100_;

CREATE TEMP TABLE query_1000_ AS (SELECT R_1.col0 as a FROM R_1 SEMI JOIN R_4 ON R_4.col0 = R_1.col0 GROUP BY a);
CREATE TEMP TABLE best_1000_ as WITH 
count_1000_R_3_c AS (SELECT COUNT(DISTINCT R_3.col1) as cnt_c FROM R_3      ),
best_1000_R_3_c as (SELECT query_1000_.a as a, 10 as posi_tag, count_1000_R_3_c.cnt_c as cnt_tag FROM query_1000_, count_1000_R_3_c   ) ,
count_1000_R_4_c AS (SELECT R_4.col0 as a, COUNT(DISTINCT R_4.col1) as cnt_c FROM R_4    GROUP BY  R_4.col0 ),
best_1000_R_4_c AS ( SELECT best_1000_R_3_c.a as a, CASE WHEN best_1000_R_3_c.cnt_tag < count_1000_R_4_c.cnt_c THEN best_1000_R_3_c.posi_tag ELSE 14 END as posi_tag, CASE WHEN best_1000_R_3_c.cnt_tag < count_1000_R_4_c.cnt_c THEN best_1000_R_3_c.cnt_tag ELSE count_1000_R_4_c.cnt_c END as cnt_tag FROM best_1000_R_3_c, count_1000_R_4_c WHERE best_1000_R_3_c.a = count_1000_R_4_c.a ) ,
SELECT best_1000_R_4_c.a as a, best_1000_R_4_c.posi_tag as posi_tag FROM best_1000_R_4_c
;
DROP TABLE query_1000_;
CREATE TEMP TABLE query_1010_ AS (
SELECT best_1000_.a as a, R_3.col1 as c FROM best_1000_ JOIN R_3 ON ( best_1000_.posi_tag = 10 )  SEMI JOIN R_4 ON R_4.col0 = best_1000_.a AND R_4.col1 = R_3.col1
 UNION ALL 
SELECT best_1000_.a as a, R_4.col1 as c FROM best_1000_ JOIN R_4 ON ( R_4.col0 = best_1000_.a AND best_1000_.posi_tag = 14 )  SEMI JOIN R_3 ON R_3.col1 = R_4.col1);
DROP TABLE best_1000_;
CREATE TEMP TABLE best_1010_ as WITH 
count_1010_R_2_d AS (SELECT COUNT(DISTINCT R_2.col1) as cnt_d FROM R_2      ),
best_1010_R_2_d as (SELECT query_1010_.a as a, query_1010_.c as c, 7 as posi_tag, count_1010_R_2_d.cnt_d as cnt_tag FROM query_1010_, count_1010_R_2_d   ) ,
count_1010_R_3_d AS (SELECT R_3.col1 as c, COUNT(DISTINCT R_3.col0) as cnt_d FROM R_3    GROUP BY  R_3.col1 ),
best_1010_R_3_d AS ( SELECT best_1010_R_2_d.a as a, best_1010_R_2_d.c as c, CASE WHEN best_1010_R_2_d.cnt_tag < count_1010_R_3_d.cnt_d THEN best_1010_R_2_d.posi_tag ELSE 11 END as posi_tag, CASE WHEN best_1010_R_2_d.cnt_tag < count_1010_R_3_d.cnt_d THEN best_1010_R_2_d.cnt_tag ELSE count_1010_R_3_d.cnt_d END as cnt_tag FROM best_1010_R_2_d, count_1010_R_3_d WHERE best_1010_R_2_d.c = count_1010_R_3_d.c ) ,
SELECT best_1010_R_3_d.a as a, best_1010_R_3_d.c as c, best_1010_R_3_d.posi_tag as posi_tag FROM best_1010_R_3_d
;
DROP TABLE query_1010_;
CREATE TEMP TABLE query_1011_ AS (
SELECT best_1010_.a as a, best_1010_.c as c, R_2.col1 as d FROM best_1010_ JOIN R_2 ON ( best_1010_.posi_tag = 7 )  SEMI JOIN R_3 ON R_3.col0 = R_2.col1 AND R_3.col1 = best_1010_.c
 UNION ALL 
SELECT best_1010_.a as a, best_1010_.c as c, R_3.col0 as d FROM best_1010_ JOIN R_3 ON ( R_3.col1 = best_1010_.c AND best_1010_.posi_tag = 11 )  SEMI JOIN R_2 ON R_2.col1 = R_3.col0);
DROP TABLE best_1010_;

CREATE TEMP TABLE res AS SELECT SUM(a) + SUM(b) + SUM(c) + SUM(d) FROM (SELECT query_1101_.a as a, query_1101_.b as b, query_1011_.c as c, query_1101_.d as d FROM query_1101_, query_1011_ WHERE query_1101_.a = query_1011_.a and query_1101_.d = query_1011_.d);