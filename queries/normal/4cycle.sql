-- q(a, b, c, d) :- R(a, b), R(b, c), R(c, d), R(d, a)
SELECT COUNT(*) FROM
  R r1, R r2, R r3, R r4
WHERE r1.col1 = r2.col0
  AND r2.col1 = r3.col0
  AND r3.col1 = r4.col0
  AND r4.col1 = r1.col0;
