CREATE VIEW R_1 AS SELECT * FROM R;
CREATE VIEW R_2 AS SELECT * FROM R;
CREATE VIEW R_3 AS SELECT * FROM R;
CREATE VIEW R_4 AS SELECT * FROM R;
CREATE TEMP TABLE query_0 AS (SELECT R_1.col0 as a FROM R_1 SEMI JOIN R_4 ON R_4.col0 = R_1.col0 GROUP BY a);
CREATE TEMP TABLE query_1_count_R_1 as (SELECT R_1.col0 as a, COUNT(DISTINCT R_1.col1) as cnt_b FROM R_1    GROUP BY  R_1.col0 );
CREATE TEMP TABLE query_1_best_R_1 as (SELECT query_0.a as a, 1 as posi_tag, query_1_count_R_1.cnt_b as cnt_tag FROM query_0, query_1_count_R_1 WHERE query_0.a = query_1_count_R_1.a ) ;
DROP TABLE query_1_count_R_1;
DROP TABLE query_0;
CREATE TEMP TABLE query_1_count_R_2 as (SELECT COUNT(DISTINCT R_2.col0) as cnt_b FROM R_2      );
CREATE TEMP TABLE query_1_best_R_2 as ( SELECT query_1_best_R_1.a as a, CASE WHEN query_1_best_R_1.cnt_tag < query_1_count_R_2.cnt_b THEN query_1_best_R_1.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_1_best_R_1.cnt_tag < query_1_count_R_2.cnt_b THEN query_1_best_R_1.cnt_tag ELSE query_1_count_R_2.cnt_b END as cnt_tag FROM query_1_best_R_1, query_1_count_R_2   ) ;
DROP TABLE query_1_count_R_2;
DROP TABLE query_1_best_R_1;
CREATE TEMP TABLE query_1_best_ as (SELECT query_1_best_R_2.a as a, query_1_best_R_2.posi_tag as posi_tag FROM query_1_best_R_2   );
DROP TABLE query_1_best_R_2;
CREATE TEMP TABLE query_1_prop_ AS (
SELECT query_1_best_.a as a, R_1.col1 as b FROM R_1, query_1_best_ WHERE ( R_1.col0 = query_1_best_.a AND query_1_best_.posi_tag = 1 ) 
 UNION ALL 
SELECT query_1_best_.a as a, R_2.col0 as b FROM R_2, query_1_best_ WHERE ( query_1_best_.posi_tag = 2 ) );
DROP TABLE query_1_best_;
CREATE TEMP TABLE query_1 AS (SELECT query_1_prop_.a as a, query_1_prop_.b as b FROM query_1_prop_  SEMI JOIN R_1 ON R_1.col0 = query_1_prop_.a AND R_1.col1 = query_1_prop_.b SEMI JOIN R_2 ON R_2.col0 = query_1_prop_.b GROUP BY a, b);
DROP TABLE query_1_prop_;
CREATE TEMP TABLE query_2_count_R_2 as (SELECT R_2.col0 as b, COUNT(DISTINCT R_2.col1) as cnt_c FROM R_2    GROUP BY  R_2.col0 );
CREATE TEMP TABLE query_2_best_R_2 as (SELECT query_1.a as a, query_1.b as b, 1 as posi_tag, query_2_count_R_2.cnt_c as cnt_tag FROM query_1, query_2_count_R_2 WHERE query_1.b = query_2_count_R_2.b ) ;
DROP TABLE query_2_count_R_2;
DROP TABLE query_1;
CREATE TEMP TABLE query_2_count_R_3 as (SELECT COUNT(DISTINCT R_3.col0) as cnt_c FROM R_3      );
CREATE TEMP TABLE query_2_best_R_3 as ( SELECT query_2_best_R_2.a as a, query_2_best_R_2.b as b, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_c THEN query_2_best_R_2.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_c THEN query_2_best_R_2.cnt_tag ELSE query_2_count_R_3.cnt_c END as cnt_tag FROM query_2_best_R_2, query_2_count_R_3   ) ;
DROP TABLE query_2_count_R_3;
DROP TABLE query_2_best_R_2;
CREATE TEMP TABLE query_2_best_ as (SELECT query_2_best_R_3.a as a, query_2_best_R_3.b as b, query_2_best_R_3.posi_tag as posi_tag FROM query_2_best_R_3   );
DROP TABLE query_2_best_R_3;
CREATE TEMP TABLE query_2_prop_ AS (
SELECT query_2_best_.a as a, query_2_best_.b as b, R_2.col1 as c FROM R_2, query_2_best_ WHERE ( R_2.col0 = query_2_best_.b AND query_2_best_.posi_tag = 1 ) 
 UNION ALL 
SELECT query_2_best_.a as a, query_2_best_.b as b, R_3.col0 as c FROM R_3, query_2_best_ WHERE ( query_2_best_.posi_tag = 2 ) );
DROP TABLE query_2_best_;
CREATE TEMP TABLE query_2 AS (SELECT query_2_prop_.a as a, query_2_prop_.b as b, query_2_prop_.c as c FROM query_2_prop_  SEMI JOIN R_2 ON R_2.col0 = query_2_prop_.b AND R_2.col1 = query_2_prop_.c SEMI JOIN R_3 ON R_3.col0 = query_2_prop_.c GROUP BY a, b, c);
DROP TABLE query_2_prop_;


CREATE TEMP TABLE query_3_count_R_3 as (SELECT R_3.col0 as c, COUNT(DISTINCT R_3.col1) as cnt_d FROM R_3    GROUP BY  R_3.col0 );
CREATE TEMP TABLE query_3_best_R_3 as (SELECT query_2.a as a, query_2.b as b, query_2.c as c, 1 as posi_tag, query_3_count_R_3.cnt_d as cnt_tag FROM query_2, query_3_count_R_3 WHERE query_2.c = query_3_count_R_3.c ) ;

DROP TABLE query_3_count_R_3;
DROP TABLE query_2;

CREATE TEMP TABLE query_3_count_R_4 as (SELECT R_4.col0 as a, COUNT(DISTINCT R_4.col1) as cnt_d FROM R_4    GROUP BY  R_4.col0 );
CREATE TEMP TABLE query_3_best_R_4 as ( SELECT query_3_best_R_3.a as a, query_3_best_R_3.b as b, query_3_best_R_3.c as c, CASE WHEN query_3_best_R_3.cnt_tag < query_3_count_R_4.cnt_d THEN query_3_best_R_3.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_3_best_R_3.cnt_tag < query_3_count_R_4.cnt_d THEN query_3_best_R_3.cnt_tag ELSE query_3_count_R_4.cnt_d END as cnt_tag FROM query_3_best_R_3, query_3_count_R_4 WHERE query_3_best_R_3.a = query_3_count_R_4.a ) ;

DROP TABLE query_3_count_R_4;
DROP TABLE query_3_best_R_3;

CREATE TEMP TABLE query_3_best_ as (SELECT query_3_best_R_4.a as a, query_3_best_R_4.b as b, query_3_best_R_4.c as c, query_3_best_R_4.posi_tag as posi_tag FROM query_3_best_R_4   );

SELECT SUM(query_3_best_R_4.cnt_tag) FROM query_3_best_R_4

sum(query_3_best_R_4.cnt_tag)
-------------------------------------
12736152112

DROP TABLE query_3_best_R_4;

CREATE TEMP TABLE t1 AS SELECT query_3_best_.a as a, query_3_best_.b as b, query_3_best_.c as c, R_3.col1 as d FROM R_3, query_3_best_ WHERE ( R_3.col0 = query_3_best_.c AND query_3_best_.posi_tag = 1 ) SEMI JOIN R_4 ON R_4.col0 = a and R_4.col1 = d;
CREATE TEMP TABLE t2 AS SELECT query_3_best_.a as a, query_3_best_.b as b, query_3_best_.c as c, R_4.col1 as d FROM R_4, query_3_best_ WHERE ( R_4.col0 = query_3_best_.a AND query_3_best_.posi_tag = 2 ) SEMI JOIN R_3 ON R_3.col0 = c and R_3.col1 = d;

CREATE TEMP TABLE query_3_prop_ AS (
t1
 UNION ALL 
t2 );
DROP TABLE query_3_best_;
CREATE TEMP TABLE query_3 AS (SELECT query_3_prop_.a as a, query_3_prop_.b as b, query_3_prop_.c as c, query_3_prop_.d as d FROM query_3_prop_  SEMI JOIN R_3 ON R_3.col0 = query_3_prop_.c AND R_3.col1 = query_3_prop_.d SEMI JOIN R_4 ON R_4.col0 = query_3_prop_.a AND R_4.col1 = query_3_prop_.d GROUP BY a, b, c, d);
DROP TABLE query_3_prop_;
