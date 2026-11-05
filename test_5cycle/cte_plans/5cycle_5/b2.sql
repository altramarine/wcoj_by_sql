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
  "fl_R3__R6f__R9_join" AS MATERIALIZED (
    SELECT r0."x" AS "c", r0."y" AS "d", r1."a" AS "a" FROM (("fl_Arc" AS r0 JOIN "fl_R6f" AS r1 ON r1."c" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."a")
  ),
  "fl_R9f" AS NOT MATERIALIZED (
    SELECT DISTINCT r0."d" AS "d", r0."a" AS "a" FROM "fl_R3__R6f__R9_join" AS r0
  ),
  "fl_R4__R5__R9f_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "d", r0."y" AS "e", r1."y" AS "a" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_R9f" AS r2 ON r2."d" = r0."x" AND r2."a" = r1."y")
  ),
  "fl_R1__R2__R3__R4__R5__R6__R6f__R9__R9f_join" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r2."e" AS "e" FROM (("fl_R1__R2__R6_join" AS r0 JOIN "fl_R3__R6f__R9_join" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a") JOIN "fl_R4__R5__R9f_join" AS r2 ON r2."d" = r1."d" AND r2."a" = r0."a")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM (("fl_R1__R2__R3__R4__R5__R6__R6f__R9__R9f_join" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."b" AND r1."y" = r0."d") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."c" AND r2."y" = r0."e")
) AS answers;

SET disabled_optimizers = '';
