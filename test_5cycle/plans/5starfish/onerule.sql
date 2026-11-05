-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."x" AS "a", r0."y" AS "b", r3."y" AS "e", r4."y" AS "f", r1."y" AS "c", r2."y" AS "d" FROM "fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y" JOIN "fl_Arc" AS r2 ON r2."x" = r1."y" JOIN "fl_Arc" AS r3 ON r3."x" = r2."y" JOIN "fl_Arc" AS r4 ON r4."x" = r3."y" JOIN "fl_Arc" AS r5 ON r5."x" = r3."y" AND r5."y" = r0."x" JOIN "fl_Arc" AS r6 ON r6."x" = r0."x" AND r6."y" = r4."y" JOIN "fl_Arc" AS r7 ON r7."x" = r0."y" AND r7."y" = r4."y" JOIN "fl_Arc" AS r8 ON r8."x" = r1."y" AND r8."y" = r4."y" JOIN "fl_Arc" AS r9 ON r9."x" = r2."y" AND r9."y" = r4."y";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
