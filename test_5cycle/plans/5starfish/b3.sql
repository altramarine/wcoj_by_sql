-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_R1__R6__R7_join" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "f" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."x" JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."y";

CREATE TEMP TABLE "fl_R1__R2__R6__R7__R8_join" AS SELECT r0."a" AS "a", r0."b" AS "b", r1."b" AS "c", r0."f" AS "f" FROM "fl_R1__R6__R7_join" AS r0 JOIN "fl_R1__R6__R7_join" AS r1 ON r1."a" = r0."b" AND r1."f" = r0."f";

CREATE TEMP TABLE "fl_Result" AS SELECT r1."b" AS "a", r2."b" AS "b", r0."c" AS "e", r0."f" AS "f", r0."a" AS "c", r0."b" AS "d" FROM "fl_R1__R2__R6__R7__R8_join" AS r0 JOIN "fl_R1__R6__R7_join" AS r1 ON r1."a" = r0."c" AND r1."f" = r0."f" JOIN "fl_R1__R2__R6__R7__R8_join" AS r2 ON r2."a" = r1."b" AND r2."c" = r0."a" AND r2."f" = r0."f";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
