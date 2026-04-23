CREATE TEMP TABLE __query__result__ AS SELECT SUM(a)+SUM(b)+SUM(c)+SUM(d) FROM (SELECT R_1.col0 as a, R_1.col1 as b, R_3.col1 as c, R_2.col1 as d FROM R as R_3, R as R_4, R as R_1, R as R_2 WHERE R_1.col0 = R_4.col0 and R_1.col1 = R_2.col0 and R_3.col1 = R_4.col1 and R_2.col1 = R_3.col0);

