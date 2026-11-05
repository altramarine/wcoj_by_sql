-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_R1__R2_join" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y";

CREATE TEMP TABLE "fl_R5__R6__R8_join" AS SELECT r0."x" AS "e", r0."y" AS "a", r1."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y" JOIN "fl_Arc" AS r2 ON r2."x" = r1."y" AND r2."y" = r0."x";

CREATE TEMP TABLE "fl_R3__R5__R6__R8__R9_join" AS SELECT r0."e" AS "c", r0."a" AS "d", r1."e" AS "e", r0."c" AS "a" FROM "fl_R5__R6__R8_join" AS r0 JOIN "fl_R5__R6__R8_join" AS r1 ON r1."a" = r0."c" AND r1."c" = r0."e";

CREATE TEMP TABLE "fl_R1__R2__R3__R5__R6__R8__R9_join" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2_join" AS r0 JOIN "fl_R3__R5__R6__R8__R9_join" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R1__R2__R3__R5__R6__R8__R9_join" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."d" AND r1."y" = r0."e" JOIN "fl_Arc" AS r2 ON r2."x" = r0."b" AND r2."y" = r0."d";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
