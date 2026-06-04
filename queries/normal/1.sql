-- q(x, y, z) :- R(x, y), R(y, z), R(x, z)
SELECT COUNT(*) FROM
  R r1, R r2, R r3
WHERE r1.col1 = r2.col0
  AND r1.col0 = r3.col0
  AND r2.col1 = r3.col1;
