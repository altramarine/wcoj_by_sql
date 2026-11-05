-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_InDeg" AS SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y";

CREATE TEMP TABLE "fl_Arc_i0_i1_H" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."x" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."y" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_OutDeg" AS SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x";

CREATE TEMP TABLE "fl_Arc_o1_i0_L" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_Arc_o0_o1_H" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."x" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_Arc_o0_o1_L" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."x" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_R1__R2__R6_by_R1" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM "fl_Arc_o0_o1_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y" JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y"
UNION
SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM "fl_Arc_o0_o1_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."x" JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."y";

CREATE TEMP TABLE "fl_Arc_o1_i0_H" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_R1__R2__R3__R6__R7__R9_by_R9" AS SELECT r0."y" AS "a", r1."b" AS "b", r1."c" AS "c", r0."x" AS "d" FROM "fl_Arc_o1_i0_L" AS r0 JOIN "fl_R1__R2__R6_by_R1" AS r1 ON r1."a" = r0."y" JOIN "fl_R1__R2__R6_by_R1" AS r2 ON r2."a" = r1."b" AND r2."b" = r1."c" AND r2."c" = r0."x"
UNION
SELECT r0."y" AS "a", r1."a" AS "b", r1."b" AS "c", r0."x" AS "d" FROM "fl_Arc_o1_i0_H" AS r0 JOIN "fl_R1__R2__R6_by_R1" AS r1 ON r1."c" = r0."x" JOIN "fl_R1__R2__R6_by_R1" AS r2 ON r2."a" = r0."y" AND r2."b" = r1."a" AND r2."c" = r1."b";

CREATE TEMP TABLE "fl_Arc_i0_i1_L" AS SELECT r0."x" AS "x", r0."y" AS "y" FROM "fl_Arc" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."x" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."y" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_Result" AS SELECT r1."a" AS "a", r0."y" AS "b", r0."x" AS "e", r1."c" AS "c", r1."d" AS "d" FROM "fl_Arc_i0_i1_H" AS r0 JOIN "fl_R1__R2__R3__R6__R7__R9_by_R9" AS r1 ON r1."b" = r0."y" JOIN "fl_R1__R2__R3__R6__R7__R9_by_R9" AS r2 ON r2."a" = r1."c" AND r2."b" = r1."d" AND r2."c" = r0."x" AND r2."d" = r1."a"
UNION
SELECT r1."d" AS "a", r0."y" AS "b", r0."x" AS "e", r1."a" AS "c", r1."b" AS "d" FROM "fl_Arc_i0_i1_L" AS r0 JOIN "fl_R1__R2__R3__R6__R7__R9_by_R9" AS r1 ON r1."c" = r0."x" JOIN "fl_R1__R2__R3__R6__R7__R9_by_R9" AS r2 ON r2."a" = r1."d" AND r2."b" = r0."y" AND r2."c" = r1."a" AND r2."d" = r1."b";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
