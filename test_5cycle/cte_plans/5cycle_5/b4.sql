-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_R2__R3__R7_join" AS MATERIALIZED (
    SELECT r0."x" AS "b", r0."y" AS "c", r1."y" AS "d" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
  ),
  "fl_R3__R6__R9_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "c", r0."y" AS "d", r1."x" AS "a" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."x")
  ),
  "fl_R1__R2__R3__R6__R7__R9_join" AS NOT MATERIALIZED (
    SELECT r0."b" AS "a", r0."c" AS "b", r0."d" AS "c", r1."d" AS "d" FROM (("fl_R2__R3__R7_join" AS r0 JOIN "fl_R2__R3__R7_join" AS r1 ON r1."b" = r0."c" AND r1."c" = r0."d") SEMI JOIN "fl_R3__R6__R9_join" AS r2 ON r2."c" = r0."d" AND r2."d" = r1."d" AND r2."a" = r0."b")
  ),
  "fl_R1__R2__R3__R6__R7__R8__R9_join" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r1."y" AS "e" FROM ("fl_R1__R2__R3__R6__R7__R9_join" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."c")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R3__R6__R7__R8__R9_join" AS r0 SEMI JOIN "fl_R2__R3__R7_join" AS r1 ON r1."b" = r0."d" AND r1."c" = r0."e" AND r1."d" = r0."a")
) AS answers;

SET disabled_optimizers = '';
