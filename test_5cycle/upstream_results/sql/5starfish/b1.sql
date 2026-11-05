-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_R1__R6__R7_join" AS MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "f" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."y")
  ),
  "fl_R4__R5_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "d", r0."y" AS "e", r1."y" AS "a" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  ),
  "fl_R1__R2__R3__R4__R5__R6__R7__R8__R9_join" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r1."b" AS "c", r2."b" AS "d", r3."e" AS "e", r0."f" AS "f" FROM ((("fl_R1__R6__R7_join" AS r0 JOIN "fl_R1__R6__R7_join" AS r1 ON r1."a" = r0."b" AND r1."f" = r0."f") JOIN "fl_R1__R6__R7_join" AS r2 ON r2."a" = r1."b" AND r2."f" = r0."f") JOIN "fl_R4__R5_join" AS r3 ON r3."d" = r2."b" AND r3."a" = r0."a")
  )
SELECT r1."a" AS "a", r1."b" AS "b", r0."x" AS "e", r0."y" AS "f", r1."c" AS "c", r1."d" AS "d" FROM ("fl_Arc" AS r0 JOIN "fl_R1__R2__R3__R4__R5__R6__R7__R8__R9_join" AS r1 ON r1."e" = r0."x" AND r1."f" = r0."y")
) AS answers;

SET disabled_optimizers = '';
