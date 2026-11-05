-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_OutDeg" AS SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x";

CREATE TEMP TABLE "fl_InDeg" AS SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y";

CREATE TEMP TABLE "fl_Arc_o1_i0_L" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_R1__R2_by_R1" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM "fl_Arc_o1_i0_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y";

CREATE TEMP TABLE "fl_Arc_o1_i0_H" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_R3__R4_by_R4" AS SELECT r1."x" AS "c", r0."x" AS "d", r0."y" AS "e" FROM "fl_Arc_o1_i0_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."x";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2_by_R1" AS r0 JOIN "fl_R3__R4_by_R4" AS r1 ON r1."c" = r0."c" JOIN "fl_Arc" AS r2 ON r2."x" = r1."e" AND r2."y" = r0."a"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."y" AS "d", r2."b" AS "e" FROM "fl_R1__R2_by_R1" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."c" JOIN "fl_R1__R2_by_R1" AS r2 ON r2."a" = r1."y" AND r2."c" = r0."a"
UNION
SELECT r0."d" AS "a", r0."e" AS "b", r1."d" AS "c", r1."e" AS "d", r0."c" AS "e" FROM "fl_R3__R4_by_R4" AS r0 JOIN "fl_R3__R4_by_R4" AS r1 ON r1."c" = r0."e" JOIN "fl_Arc" AS r2 ON r2."x" = r1."e" AND r2."y" = r0."c"
UNION
SELECT r0."d" AS "a", r0."e" AS "b", r1."y" AS "c", r2."b" AS "d", r0."c" AS "e" FROM "fl_R3__R4_by_R4" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."e" JOIN "fl_R1__R2_by_R1" AS r2 ON r2."a" = r1."y" AND r2."c" = r0."c";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
