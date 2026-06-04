-- q(a, b, c, d) :- R(a, b), R(b, c), R(a, d), R(d, c)
SELECT COUNT(*) FROM
  R r1, R r2, R r3, R r4
WHERE r1.col1 = r2.col0
  AND r1.col0 = r3.col0
  AND r3.col1 = r4.col0
  AND r2.col1 = r4.col1;
