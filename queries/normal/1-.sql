-- q(x, y, z) :- R1(x, y), R2(y, z), R3(x, z)
SELECT COUNT(*)
FROM R AS r1
JOIN R AS r3
  ON r1.col0 = r3.col0
SEMI JOIN R AS r2
  ON r1.col1 = r2.col0
 AND r3.col1 = r2.col1;
