CREATE VIEW R_1 AS SELECT * FROM R;
CREATE VIEW R_2 AS SELECT * FROM R;
CREATE VIEW R_3 AS SELECT * FROM R;
CREATE TEMP TABLE query_1 AS (SELECT R_1.col0 as x, R_1.col1 as y FROM R_1 SEMI JOIN R_2 ON R_2.col0 = R_1.col1 SEMI JOIN R_3 ON R_3.col0 = R_1.col0);
CREATE TEMP TABLE query_2_best_ AS WITH
query_2_count_R_2 as (SELECT R_2.col0 as y, COUNT(DISTINCT R_2.col1) as cnt_z FROM R_2    GROUP BY  R_2.col0 ),
query_2_best_R_2 as (SELECT query_1.x as x, query_1.y as y, 1 as posi_tag, query_2_count_R_2.cnt_z as cnt_tag FROM query_1, query_2_count_R_2 WHERE query_1.y = query_2_count_R_2.y ) ,
query_2_count_R_3 as (SELECT R_3.col0 as x, COUNT(DISTINCT R_3.col1) as cnt_z FROM R_3    GROUP BY  R_3.col0 ),
query_2_best_R_3 as ( SELECT query_2_best_R_2.x as x, query_2_best_R_2.y as y, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_z THEN query_2_best_R_2.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_z THEN query_2_best_R_2.cnt_tag ELSE query_2_count_R_3.cnt_z END as cnt_tag FROM query_2_best_R_2, query_2_count_R_3 WHERE query_2_best_R_2.x = query_2_count_R_3.x ) ,
SELECT * FROM query_2_best_R_3;
DROP TABLE query_1;
CREATE TEMP TABLE query_2 AS (
SELECT query_2_best_.x as x, query_2_best_.y as y, R_2.col1 as z FROM query_2_best_ JOIN R_2 ON ( R_2.col0 = query_2_best_.y AND query_2_best_.posi_tag = 1 )  SEMI JOIN R_3 ON R_3.col0 = query_2_best_.x AND R_3.col1 = R_2.col1
 UNION ALL 
SELECT query_2_best_.x as x, query_2_best_.y as y, R_3.col1 as z FROM query_2_best_ JOIN R_3 ON ( R_3.col0 = query_2_best_.x AND query_2_best_.posi_tag = 2 )  SEMI JOIN R_2 ON R_2.col0 = query_2_best_.y AND R_2.col1 = R_3.col1);
DROP TABLE query_2_best_;