CREATE TABLE A (col0 INTEGER, col1 INTEGER);
CREATE TABLE B (col0 INTEGER, col1 INTEGER);
CREATE TABLE C (col0 INTEGER, col1 INTEGER);
INSERT INTO A VALUES (1, 2), (1, 3), (2, 4);
INSERT INTO B VALUES (2, 3), (3, 5), (4, 6);
INSERT INTO C VALUES (3, 1), (5, 1), (6, 2);


WITH
  query_0 AS (SELECT A.col0 as x FROM A, C WHERE ( C.col1 = A.col0 ) GROUP BY x),
  query_1_count_A as (SELECT A.col0 as x, COUNT(DISTINCT A.col1) as cnt_y FROM A    GROUP BY  A.col0 ),
  query_1_best_A as (SELECT query_0.x as x, 1 as posi_tag, query_1_count_A.cnt_y as cnt_tag FROM query_0, query_1_count_A WHERE query_0.x = query_1_count_A.x ) ,
  query_1_count_B as (SELECT COUNT(DISTINCT B.col0) as cnt_y FROM B      ),
  query_1_best_B as ( SELECT query_1_best_A.x as x, CASE WHEN query_1_best_A.cnt_tag > query_1_count_B.cnt_y THEN query_1_best_A.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_1_best_A.cnt_tag > query_1_count_B.cnt_y THEN query_1_best_A.cnt_tag ELSE query_1_count_B.cnt_y END as cnt_tag FROM query_1_best_A, query_1_count_B   ) ,
  query_1_best_ as (SELECT query_1_best_B.x as x, query_1_best_B.posi_tag as posi_tag FROM query_1_best_B, query_1_count_B   ),
  query_1_prop_ AS (
  SELECT query_1_best_.x as x, A.col1 as y FROM A, query_1_best_ WHERE ( A.col0 = query_1_best_.x AND query_1_best_.posi_tag = 1 ) 
   UNION ALL 
  SELECT query_1_best_.x as x, B.col0 as y FROM B, query_1_best_ WHERE ( query_1_best_.posi_tag = 2 ) ),
  query_1 AS (SELECT query_1_prop_.x as x, query_1_prop_.y as y FROM A, B, query_1_prop_ WHERE ( A.col0 = query_1_prop_.x AND A.col1 = query_1_prop_.y AND B.col0 = query_1_prop_.y ) GROUP BY x, y),
  query_2_count_B as (SELECT B.col0 as y, COUNT(DISTINCT B.col1) as cnt_z FROM B    GROUP BY  B.col0 ),
  query_2_best_B as (SELECT query_1.x as x, query_1.y as y, 1 as posi_tag, query_2_count_B.cnt_z as cnt_tag FROM query_1, query_2_count_B WHERE query_1.y = query_2_count_B.y ) ,
  query_2_count_C as (SELECT C.col1 as x, COUNT(DISTINCT C.col0) as cnt_z FROM C    GROUP BY  C.col1 ),
  query_2_best_C as ( SELECT query_2_best_B.x as x, query_2_best_B.y as y, CASE WHEN query_2_best_B.cnt_tag > query_2_count_C.cnt_z THEN query_2_best_B.posi_tag ELSE 2 END as posi_tag, CASE WHEN query_2_best_B.cnt_tag > query_2_count_C.cnt_z THEN query_2_best_B.cnt_tag ELSE query_2_count_C.cnt_z END as cnt_tag FROM query_2_best_B, query_2_count_C WHERE query_2_best_B.x = query_2_count_C.x ) ,
  query_2_best_ as (SELECT query_2_best_C.x as x, query_2_best_C.y as y, query_2_best_C.posi_tag as posi_tag FROM query_2_best_C, query_2_count_C   ),
  query_2_prop_ AS (
  SELECT query_2_best_.x as x, query_2_best_.y as y, B.col1 as z FROM B, query_2_best_ WHERE ( B.col0 = query_2_best_.y AND query_2_best_.posi_tag = 1 ) 
   UNION ALL 
  SELECT query_2_best_.x as x, query_2_best_.y as y, C.col0 as z FROM C, query_2_best_ WHERE ( C.col1 = query_2_best_.x AND query_2_best_.posi_tag = 2 ) ),
  query_2 AS (SELECT query_2_prop_.x as x, query_2_prop_.y as y, query_2_prop_.z as z FROM B, C, query_2_prop_ WHERE ( B.col0 = query_2_prop_.y AND B.col1 = query_2_prop_.z AND C.col0 = query_2_prop_.z AND C.col1 = query_2_prop_.x ) GROUP BY x, y, z),
SELECT * FROM query_2;