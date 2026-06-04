-- q(a, b, c, d) :- R(a, b), R(b, c), R(a, c), R(a, d), R(b, d), R(c, d)
SELECT COUNT(*) FROM
  R r1, R r2, R r3, R r4, R r5, R r6
WHERE r1.col0 = r3.col0
  AND r1.col0 = r4.col0
  AND r1.col1 = r2.col0
  AND r1.col1 = r5.col0
  AND r2.col1 = r3.col1
  AND r2.col1 = r6.col0
  AND r4.col1 = r5.col1
  AND r4.col1 = r6.col1;
