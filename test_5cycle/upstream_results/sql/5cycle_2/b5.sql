-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_R1__R2_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  ),
  "fl_R4__R5_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "d", r0."y" AS "e", r1."y" AS "a" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  ),
  "fl_R1__R2__R4__R5_join" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM ("fl_R1__R2_join" AS r0 JOIN "fl_R4__R5_join" AS r1 ON r1."a" = r0."a")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ((("fl_R1__R2__R4__R5_join" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."c" AND r1."y" = r0."d") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."a" AND r2."y" = r0."c") SEMI JOIN "fl_Arc" AS r3 ON r3."x" = r0."b" AND r3."y" = r0."d")
) AS answers;

SET disabled_optimizers = '';
