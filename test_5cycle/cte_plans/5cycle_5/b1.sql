-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_R1__R2__R3__R4__R5_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c", r2."y" AS "d", r3."y" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") JOIN "fl_Arc" AS r2 ON r2."x" = r1."y") JOIN "fl_Arc" AS r3 ON r3."x" = r2."y") SEMI JOIN "fl_Arc" AS r4 ON r4."x" = r3."y" AND r4."y" = r0."x")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM (((("fl_R1__R2__R3__R4__R5_join" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."a" AND r1."y" = r0."c") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."b" AND r2."y" = r0."d") SEMI JOIN "fl_Arc" AS r3 ON r3."x" = r0."c" AND r3."y" = r0."e") SEMI JOIN "fl_Arc" AS r4 ON r4."x" = r0."d" AND r4."y" = r0."a")
) AS answers;

SET disabled_optimizers = '';
