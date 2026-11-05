-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_R4__R5_join" AS SELECT r0."x" AS "d", r0."y" AS "e", r1."y" AS "a" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y";

CREATE TEMP TABLE "fl_OutDeg" AS SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x";

CREATE TEMP TABLE "fl_InDeg" AS SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y";

CREATE TEMP TABLE "fl_R1__R2__R6_by_R1" AS SELECT r0."x" AS "a", r0."y" AS "b", r3."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_Arc" AS r3 ON r3."x" = r0."y" JOIN "fl_Arc" AS r4 ON r4."x" = r0."x" AND r4."y" = r3."y" WHERE r1."k" <= r2."k"
UNION
SELECT r0."x" AS "a", r0."y" AS "b", r3."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_Arc" AS r3 ON r3."x" = r0."x" JOIN "fl_Arc" AS r4 ON r4."x" = r0."y" AND r4."y" = r3."y" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_R3__R7_join" AS SELECT r0."x" AS "c", r0."y" AS "d", r1."x" AS "b" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."y";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."a" AS "a", r3."b" AS "b", r3."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R4__R5_join" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."a" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."d" JOIN "fl_R1__R2__R6_by_R1" AS r3 ON r3."a" = r0."a" JOIN "fl_R3__R7_join" AS r4 ON r4."c" = r3."c" AND r4."d" = r0."d" AND r4."b" = r3."b" WHERE r1."k" <= r2."k"
UNION
SELECT r0."a" AS "a", r3."b" AS "b", r3."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R4__R5_join" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."a" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."d" JOIN "fl_R3__R7_join" AS r3 ON r3."d" = r0."d" JOIN "fl_R1__R2__R6_by_R1" AS r4 ON r4."a" = r0."a" AND r4."b" = r3."b" AND r4."c" = r3."c" WHERE r2."k" < r1."k";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
