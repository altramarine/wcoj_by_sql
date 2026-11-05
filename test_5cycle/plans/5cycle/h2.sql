-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_OutDeg" AS SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x";

CREATE TEMP TABLE "fl_InDeg" AS SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y";

CREATE TEMP TABLE "fl_Arc_o1_i0_H" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_Arc_o1_i0_L" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_R1__R2_by_R1" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM "fl_Arc_o1_i0_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y";

CREATE TEMP TABLE "fl_R1__R2__R3_by_R3" AS SELECT r1."a" AS "a", r1."b" AS "b", r0."x" AS "c", r0."y" AS "d" FROM "fl_Arc_o1_i0_H" AS r0 JOIN "fl_R1__R2_by_R1" AS r1 ON r1."c" = r0."x";

CREATE TEMP TABLE "fl_R4__R5_join" AS SELECT r0."x" AS "d", r0."y" AS "e", r1."y" AS "a" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y";

CREATE TEMP TABLE "fl_R3__R4__R5_by_R3" AS SELECT r0."x" AS "c", r0."y" AS "d", r1."e" AS "e", r1."a" AS "a" FROM "fl_Arc_o1_i0_L" AS r0 JOIN "fl_R4__R5_join" AS r1 ON r1."d" = r0."y";

CREATE TEMP TABLE "fl_R1__R5_by_R1" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."x" AS "e" FROM "fl_Arc_o1_i0_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."x";

CREATE TEMP TABLE "fl_R1__R4__R5_by_R4" AS SELECT r1."a" AS "a", r1."b" AS "b", r0."x" AS "d", r0."y" AS "e" FROM "fl_Arc_o1_i0_L" AS r0 JOIN "fl_R1__R5_by_R1" AS r1 ON r1."e" = r0."y";

CREATE TEMP TABLE "fl_R2__R3__R4_by_R4" AS SELECT r1."e" AS "b", r1."a" AS "c", r0."x" AS "d", r0."y" AS "e" FROM "fl_Arc_o1_i0_H" AS r0 JOIN "fl_R1__R5_by_R1" AS r1 ON r1."b" = r0."x";

CREATE TEMP TABLE "fl_R2__R3__R4_by_R2" AS SELECT r0."x" AS "b", r0."y" AS "c", r1."b" AS "d", r1."c" AS "e" FROM "fl_Arc_o1_i0_L" AS r0 JOIN "fl_R1__R2_by_R1" AS r1 ON r1."a" = r0."y";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2__R3_by_R3" AS r0 JOIN "fl_R4__R5_join" AS r1 ON r1."d" = r0."d" AND r1."a" = r0."a"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2_by_R1" AS r0 JOIN "fl_R3__R4__R5_by_R3" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r1."a" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R1__R4__R5_by_R4" AS r0 JOIN "fl_R1__R5_by_R1" AS r1 ON r1."b" = r0."d" AND r1."e" = r0."b"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r1."d" AS "d", r0."e" AS "e" FROM "fl_R1__R5_by_R1" AS r0 JOIN "fl_R2__R3__R4_by_R4" AS r1 ON r1."b" = r0."b" AND r1."e" = r0."e"
UNION
SELECT r0."c" AS "a", r0."d" AS "b", r0."e" AS "c", r1."b" AS "d", r0."b" AS "e" FROM "fl_R2__R3__R4_by_R4" AS r0 JOIN "fl_R1__R2_by_R1" AS r1 ON r1."a" = r0."e" AND r1."c" = r0."b"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r1."d" AS "d", r0."e" AS "e" FROM "fl_R1__R5_by_R1" AS r0 JOIN "fl_R2__R3__R4_by_R2" AS r1 ON r1."b" = r0."b" AND r1."e" = r0."e";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
