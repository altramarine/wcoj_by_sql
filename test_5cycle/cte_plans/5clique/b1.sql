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
  "fl_R6f" AS NOT MATERIALIZED (
    SELECT DISTINCT r0."a" AS "a", r0."c" AS "c" FROM "fl_R1__R2__R6_join" AS r0
  ),
  "fl_R5__R6f__R8_join" AS MATERIALIZED (
    SELECT r0."x" AS "e", r0."y" AS "a", r1."c" AS "c" FROM (("fl_Arc" AS r0 JOIN "fl_R6f" AS r1 ON r1."a" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r1."c" AND r2."y" = r0."x")
  ),
  "fl_R8f" AS NOT MATERIALIZED (
    SELECT DISTINCT r0."c" AS "c", r0."e" AS "e" FROM "fl_R5__R6f__R8_join" AS r0
  ),
  "fl_R3__R4__R8f_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "c", r0."y" AS "d", r1."y" AS "e" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_R8f" AS r2 ON r2."c" = r0."x" AND r2."e" = r1."y")
  ),
  "fl_R1__R2__R3__R4__R5__R6__R6f__R8__R8f_join" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM (("fl_R1__R2__R6_join" AS r0 JOIN "fl_R3__R4__R8f_join" AS r1 ON r1."c" = r0."c") SEMI JOIN "fl_R5__R6f__R8_join" AS r2 ON r2."e" = r1."e" AND r2."a" = r0."a" AND r2."c" = r0."c")
  )
SELECT r1."a" AS "a", r0."y" AS "b", r0."x" AS "e", r1."c" AS "c", r1."d" AS "d" FROM ((("fl_Arc" AS r0 JOIN "fl_R1__R2__R3__R4__R5__R6__R6f__R8__R8f_join" AS r1 ON r1."b" = r0."y" AND r1."e" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."d") SEMI JOIN "fl_Arc" AS r3 ON r3."x" = r1."d" AND r3."y" = r1."a")
) AS answers;

SET disabled_optimizers = '';
