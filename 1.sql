CREATE VIEW R_1 AS SELECT * FROM R;
CREATE VIEW R_2 AS SELECT * FROM R;
CREATE VIEW R_3 AS SELECT * FROM R;

CREATE TEMP TABLE __result__ AS WITH
query_0 AS (SELECT R_1.col0 as x FROM R_1 SEMI JOIN R_3 ON R_3.col0 = R_1.col0 GROUP BY x),
query_1_count_R_1 as (SELECT R_1.col0 as x, COUNT(DISTINCT R_1.col1) as cnt_y FROM R_1    GROUP BY  R_1.col0 ),
query_1_best_R_1 as (SELECT query_0.x as x, 1 as posi_tag, query_1_count_R_1.cnt_y as cnt_tag FROM query_0, query_1_count_R_1 WHERE query_0.x = query_1_count_R_1.x ) ,
query_1_count_R_2 as (SELECT COUNT(DISTINCT R_2.col0) as cnt_y FROM R_2      ),
query_1_best_R_2 as ( SELECT query_1_best_R_1.x as x, CASE WHEN query_1_best_R_1.cnt_tag < query_1_count_R_2.cnt_y THEN query_1_best_R_1.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_1_best_R_1.cnt_tag < query_1_count_R_2.cnt_y THEN query_1_best_R_1.cnt_tag ELSE query_1_count_R_2.cnt_y END as cnt_tag FROM query_1_best_R_1, query_1_count_R_2   ) ,
query_1_best_ as (SELECT query_1_best_R_2.x as x, query_1_best_R_2.posi_tag as posi_tag FROM query_1_best_R_2   ),
query_1_prop_ AS (
SELECT query_1_best_.x as x, R_1.col1 as y FROM R_1, query_1_best_ WHERE ( R_1.col0 = query_1_best_.x AND query_1_best_.posi_tag = 1 ) 
 UNION ALL 
SELECT query_1_best_.x as x, R_2.col0 as y FROM R_2, query_1_best_ WHERE ( query_1_best_.posi_tag = 2 ) ),
query_1 AS (SELECT query_1_prop_.x as x, query_1_prop_.y as y FROM query_1_prop_  SEMI JOIN R_1 ON R_1.col0 = query_1_prop_.x AND R_1.col1 = query_1_prop_.y SEMI JOIN R_2 ON R_2.col0 = query_1_prop_.y GROUP BY x, y),
query_2_count_R_2 as (SELECT R_2.col0 as y, COUNT(DISTINCT R_2.col1) as cnt_z FROM R_2    GROUP BY  R_2.col0 ),
query_2_best_R_2 as (SELECT query_1.x as x, query_1.y as y, 1 as posi_tag, query_2_count_R_2.cnt_z as cnt_tag FROM query_1, query_2_count_R_2 WHERE query_1.y = query_2_count_R_2.y ) ,
query_2_count_R_3 as (SELECT R_3.col0 as x, COUNT(DISTINCT R_3.col1) as cnt_z FROM R_3    GROUP BY  R_3.col0 ),
query_2_best_R_3 as ( SELECT query_2_best_R_2.x as x, query_2_best_R_2.y as y, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_z THEN query_2_best_R_2.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_z THEN query_2_best_R_2.cnt_tag ELSE query_2_count_R_3.cnt_z END as cnt_tag FROM query_2_best_R_2, query_2_count_R_3 WHERE query_2_best_R_2.x = query_2_count_R_3.x ) ,
query_2_best_ as (SELECT query_2_best_R_3.x as x, query_2_best_R_3.y as y, query_2_best_R_3.posi_tag as posi_tag FROM query_2_best_R_3   ),
query_2_prop_ AS (
SELECT query_2_best_.x as x, query_2_best_.y as y, R_2.col1 as z FROM R_2, query_2_best_ WHERE ( R_2.col0 = query_2_best_.y AND query_2_best_.posi_tag = 1 ) 
 UNION ALL 
SELECT query_2_best_.x as x, query_2_best_.y as y, R_3.col1 as z FROM R_3, query_2_best_ WHERE ( R_3.col0 = query_2_best_.x AND query_2_best_.posi_tag = 2 ) ),
query_2 AS (SELECT query_2_prop_.x as x, query_2_prop_.y as y, query_2_prop_.z as z FROM query_2_prop_  SEMI JOIN R_2 ON R_2.col0 = query_2_prop_.y AND R_2.col1 = query_2_prop_.z SEMI JOIN R_3 ON R_3.col0 = query_2_prop_.x AND R_3.col1 = query_2_prop_.z GROUP BY x, y, z),
SELECT * FROM query_2;
