-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_FourCycle" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "c", r1."y" AS "d", r2."y" AS "e" FROM ((("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") JOIN "fl_Arc" AS r2 ON r2."x" = r1."y") SEMI JOIN "fl_Arc" AS r3 ON r3."x" = r2."y" AND r3."y" = r0."x")
  ),
  "fl_Tri" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
  )
SELECT r0."a" AS "a", r1."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_FourCycle" AS r0 JOIN "fl_Tri" AS r1 ON r1."a" = r0."a" AND r1."c" = r0."c")
) AS answers;

SET disabled_optimizers = '';
