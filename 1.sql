 CREATE TEMP TABLE R1 AS (SELECT * FROM R);
 CREATE TEMP TABLE R2 AS (SELECT * FROM R);
 CREATE TEMP TABLE R3 AS (SELECT * FROM R);
CREATE TEMP TABLE query_0 AS (SELECT R1.col0 as x FROM R1 SEMI JOIN R3 ON R3.col0 = R1.col0 GROUP BY x);
CREATE TEMP TABLE query_1_count_R1 as (SELECT R1.col0 as x, COUNT(DISTINCT R1.col1) as cnt_y FROM R1    GROUP BY  R1.col0 );
CREATE TEMP TABLE query_1_best_R1 as (SELECT query_0.x as x, 1 as posi_tag, query_1_count_R1.cnt_y as cnt_tag FROM query_0, query_1_count_R1 WHERE query_0.x = query_1_count_R1.x ) ;
CREATE TEMP TABLE query_1_count_R2 as (SELECT COUNT(DISTINCT R2.col0) as cnt_y FROM R2      );
CREATE TEMP TABLE query_1_best_R2 as ( SELECT query_1_best_R1.x as x, CASE WHEN query_1_best_R1.cnt_tag < query_1_count_R2.cnt_y THEN query_1_best_R1.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_1_best_R1.cnt_tag < query_1_count_R2.cnt_y THEN query_1_best_R1.cnt_tag ELSE query_1_count_R2.cnt_y END as cnt_tag FROM query_1_best_R1, query_1_count_R2   ) ;
CREATE TEMP TABLE query_1_best_ as (SELECT query_1_best_R2.x as x, query_1_best_R2.posi_tag as posi_tag FROM query_1_best_R2   );
CREATE TEMP TABLE query_1_prop_ AS (
SELECT query_1_best_.x as x, R1.col1 as y FROM R1, query_1_best_ WHERE ( R1.col0 = query_1_best_.x AND query_1_best_.posi_tag = 1 ) 
 UNION ALL 
SELECT query_1_best_.x as x, R2.col0 as y FROM R2, query_1_best_ WHERE ( query_1_best_.posi_tag = 2 ) );
CREATE TEMP TABLE query_1 AS (SELECT query_1_prop_.x as x, query_1_prop_.y as y FROM query_1_prop_  SEMI JOIN R1 ON R1.col0 = query_1_prop_.x AND R1.col1 = query_1_prop_.y SEMI JOIN R2 ON R2.col0 = query_1_prop_.y GROUP BY x, y);
CREATE TEMP TABLE query_2_count_R2 as (SELECT R2.col0 as y, COUNT(DISTINCT R2.col1) as cnt_z FROM R2    GROUP BY  R2.col0 );
CREATE TEMP TABLE query_2_best_R2 as (SELECT query_1.x as x, query_1.y as y, 1 as posi_tag, query_2_count_R2.cnt_z as cnt_tag FROM query_1, query_2_count_R2 WHERE query_1.y = query_2_count_R2.y ) ;
CREATE TEMP TABLE query_2_count_R3 as (SELECT R3.col0 as x, COUNT(DISTINCT R3.col1) as cnt_z FROM R3    GROUP BY  R3.col0 );
CREATE TEMP TABLE query_2_best_R3 as ( SELECT query_2_best_R2.x as x, query_2_best_R2.y as y, CASE WHEN query_2_best_R2.cnt_tag < query_2_count_R3.cnt_z THEN query_2_best_R2.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_2_best_R2.cnt_tag < query_2_count_R3.cnt_z THEN query_2_best_R2.cnt_tag ELSE query_2_count_R3.cnt_z END as cnt_tag FROM query_2_best_R2, query_2_count_R3 WHERE query_2_best_R2.x = query_2_count_R3.x ) ;
CREATE TEMP TABLE query_2_best_ as (SELECT query_2_best_R3.x as x, query_2_best_R3.y as y, query_2_best_R3.posi_tag as posi_tag FROM query_2_best_R3   );
CREATE TEMP TABLE query_2_prop_ AS (
SELECT query_2_best_.x as x, query_2_best_.y as y, R2.col1 as z FROM R2, query_2_best_ WHERE ( R2.col0 = query_2_best_.y AND query_2_best_.posi_tag = 1 ) 
 UNION ALL 
SELECT query_2_best_.x as x, query_2_best_.y as y, R3.col1 as z FROM R3, query_2_best_ WHERE ( R3.col0 = query_2_best_.x AND query_2_best_.posi_tag = 2 ) );
CREATE TEMP TABLE query_2 AS (SELECT query_2_prop_.x as x, query_2_prop_.y as y, query_2_prop_.z as z FROM query_2_prop_  SEMI JOIN R2 ON R2.col0 = query_2_prop_.y AND R2.col1 = query_2_prop_.z SEMI JOIN R3 ON R3.col0 = query_2_prop_.x AND R3.col1 = query_2_prop_.z GROUP BY x, y, z);