-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_R1__R2__R6_join" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y" JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y";

CREATE TEMP TABLE "fl_R1__R2__R3__R4__R6__R7__R8_join" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."c" AS "d", r2."c" AS "e" FROM "fl_R1__R2__R6_join" AS r0 JOIN "fl_R1__R2__R6_join" AS r1 ON r1."a" = r0."b" AND r1."b" = r0."c" JOIN "fl_R1__R2__R6_join" AS r2 ON r2."a" = r0."c" AND r2."b" = r1."c";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R1__R2__R3__R4__R6__R7__R8_join" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."e" AND r1."y" = r0."a";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
