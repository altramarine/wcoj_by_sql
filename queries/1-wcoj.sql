CREATE VIEW R_1 AS SELECT * FROM R;
CREATE VIEW R_2 AS SELECT * FROM R;
CREATE VIEW R_3 AS SELECT * FROM R;
CREATE TEMP TABLE __best__ AS WITH
query_1 AS (SELECT R_1.col0 as x, R_1.col1 as y FROM R_1 SEMI JOIN R_2 ON R_2.col0 = R_1.col1 SEMI JOIN R_3 ON R_3.col1 = R_1.col0),
query_2_count_R_2 as (SELECT R_2.col0 as y, COUNT(DISTINCT R_2.col1) as cnt_z FROM R_2    GROUP BY  R_2.col0 ),
query_2_best_R_2 as (SELECT query_1.x as x, query_1.y as y, 1 as posi_tag, query_2_count_R_2.cnt_z as cnt_tag FROM query_1, query_2_count_R_2 WHERE query_1.y = query_2_count_R_2.y ) ,
query_2_count_R_3 as (SELECT R_3.col1 as x, COUNT(DISTINCT R_3.col0) as cnt_z FROM R_3    GROUP BY  R_3.col1 ),
query_2_best_R_3 as ( SELECT query_2_best_R_2.x as x, query_2_best_R_2.y as y, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_z THEN query_2_best_R_2.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_2_best_R_2.cnt_tag < query_2_count_R_3.cnt_z THEN query_2_best_R_2.cnt_tag ELSE query_2_count_R_3.cnt_z END as cnt_tag FROM query_2_best_R_2, query_2_count_R_3 WHERE query_2_best_R_2.x = query_2_count_R_3.x ) ,
SELECT query_2_best_R_3.x as x, query_2_best_R_3.y as y, query_2_best_R_3.posi_tag as posi_tag FROM query_2_best_R_3;
CREATE TEMP TABLE __branch1__ AS
SELECT __best__.x as x, __best__.y as y, R_2.col1 as z FROM __best__ JOIN R_2 ON ( R_2.col0 = __best__.y AND __best__.posi_tag = 1 ) SEMI JOIN R_3 ON R_3.col0 = R_2.col1 AND R_3.col1 = __best__.x;
CREATE TEMP TABLE __branch2__ AS
SELECT __best__.x as x, __best__.y as y, R_3.col0 as z FROM __best__ JOIN R_3 ON ( R_3.col1 = __best__.x AND __best__.posi_tag = 2 ) SEMI JOIN R_2 ON R_2.col0 = __best__.y AND R_2.col1 = R_3.col0;
DROP TABLE __best__;
CREATE TEMP TABLE __result__ AS SELECT * FROM __branch1__ UNION ALL SELECT * FROM __branch2__;
DROP TABLE __branch1__;
DROP TABLE __branch2__;