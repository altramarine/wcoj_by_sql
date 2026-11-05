-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_OutDeg" AS SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x";

CREATE TEMP TABLE "fl_Arc_o0_o1_L" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."x" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_Arc_o0_o1_H" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."x" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_R1__R6__R7_by_R1" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "f" FROM "fl_Arc_o0_o1_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."x" JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."y"
UNION
SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "f" FROM "fl_Arc_o0_o1_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y" JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y";

CREATE TEMP TABLE "fl_Result" AS SELECT r1."b" AS "a", r2."b" AS "b", r0."b" AS "e", r0."f" AS "f", r3."b" AS "c", r0."a" AS "d" FROM "fl_R1__R6__R7_by_R1" AS r0 JOIN "fl_R1__R6__R7_by_R1" AS r1 ON r1."a" = r0."b" AND r1."f" = r0."f" JOIN "fl_R1__R6__R7_by_R1" AS r2 ON r2."a" = r1."b" AND r2."f" = r0."f" JOIN "fl_R1__R6__R7_by_R1" AS r3 ON r3."a" = r2."b" AND r3."f" = r0."f" JOIN "fl_R1__R6__R7_by_R1" AS r4 ON r4."a" = r3."b" AND r4."b" = r0."a" AND r4."f" = r0."f";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
