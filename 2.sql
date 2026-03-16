CREATE TEMP TABLE query as WITH
  R1 AS (SELECT * FROM R),
  R2 AS (SELECT * FROM R),
  R3 AS (SELECT * FROM R),
SELECT R1.col0 as x, R2.col0 as y, R3.col0 as z FROM R1, R2, R3 WHERE R1.col1 = R2.col0 and R2.col1 = R3.col1 and R1.col0 = R3.col0;