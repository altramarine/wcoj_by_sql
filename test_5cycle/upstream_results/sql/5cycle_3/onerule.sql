-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  )
SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c", r2."y" AS "d", r3."y" AS "e" FROM (((((("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") JOIN "fl_Arc" AS r2 ON r2."x" = r1."y") JOIN "fl_Arc" AS r3 ON r3."x" = r2."y") SEMI JOIN "fl_Arc" AS r4 ON r4."x" = r3."y" AND r4."y" = r0."x") SEMI JOIN "fl_Arc" AS r5 ON r5."x" = r0."x" AND r5."y" = r1."y") SEMI JOIN "fl_Arc" AS r6 ON r6."x" = r1."y" AND r6."y" = r3."y")
) AS answers;

SET disabled_optimizers = '';
