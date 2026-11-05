-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_TwoEdge" AS MATERIALIZED (
    SELECT r0."x" AS "u", r0."y" AS "x", r1."y" AS "y" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  )
SELECT r0."u" AS "u", r0."x" AS "x", r0."y" AS "y", r1."y" AS "z", r2."x" AS "w" FROM (("fl_TwoEdge" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") JOIN "fl_TwoEdge" AS r2 ON r2."u" = r1."y" AND r2."y" = r0."u")
) AS answers;

SET disabled_optimizers = '';
