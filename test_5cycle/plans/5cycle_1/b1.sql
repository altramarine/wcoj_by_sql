-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_R1__R2__R6_join" AS SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y" JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y";

CREATE TEMP TABLE "fl_R3__R4__R5__R6_join" AS SELECT r0."x" AS "c", r0."y" AS "d", r1."y" AS "e", r2."y" AS "a" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y" JOIN "fl_Arc" AS r2 ON r2."x" = r1."y" JOIN "fl_Arc" AS r3 ON r3."x" = r2."y" AND r3."y" = r0."x";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2__R6_join" AS r0 JOIN "fl_R3__R4__R5__R6_join" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
