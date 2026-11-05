-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_R1__R2__R6_join" AS MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
  ),
  "fl_R1__R5__R6_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."x" AS "e", r2."y" AS "c" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."x") JOIN "fl_Arc" AS r2 ON r2."x" = r0."x")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r2."b" AS "d", r1."e" AS "e" FROM (("fl_R1__R2__R6_join" AS r0 JOIN "fl_R1__R5__R6_join" AS r1 ON r1."a" = r0."a" AND r1."b" = r0."b" AND r1."c" = r0."c") JOIN "fl_R1__R2__R6_join" AS r2 ON r2."a" = r0."c" AND r2."c" = r1."e")
) AS answers;

SET disabled_optimizers = '';
