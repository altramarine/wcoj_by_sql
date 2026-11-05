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
  "fl_R6f" AS MATERIALIZED (
    SELECT DISTINCT r0."a" AS "a", r0."c" AS "c" FROM "fl_R1__R2__R6_join" AS r0
  ),
  "fl_OutDeg" AS MATERIALIZED (
    SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x"
  ),
  "fl_InDeg" AS MATERIALIZED (
    SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y"
  ),
  "fl_R3__R6f_by_R6f" AS NOT MATERIALIZED (
    SELECT r0."c" AS "c", r3."y" AS "d", r0."a" AS "a" FROM ((("fl_R6f" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."c") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a") JOIN "fl_Arc" AS r3 ON r3."x" = r0."c") WHERE r1."k" <= r2."k"
  ),
  "fl_R4__R5_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "d", r0."y" AS "e", r1."y" AS "a" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  ),
  "fl_R3__R4__R5__R6f_join" AS NOT MATERIALIZED (
    SELECT r0."c" AS "c", r0."d" AS "d", r1."e" AS "e", r0."a" AS "a" FROM ("fl_R3__R6f_by_R6f" AS r0 JOIN "fl_R4__R5_join" AS r1 ON r1."d" = r0."d" AND r1."a" = r0."a")
  ),
  "fl_R1__R2__R3__R4__R5__R6__R6f_join" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM ("fl_R1__R2__R6_join" AS r0 JOIN "fl_R3__R4__R5__R6f_join" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a")
  ),
  "fl_R3__R4_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "c", r0."y" AS "d", r1."y" AS "e" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  ),
  "fl_R5__R6f_by_R6f" AS NOT MATERIALIZED (
    SELECT r3."x" AS "e", r0."a" AS "a", r0."c" AS "c" FROM ((("fl_R6f" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."c") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a") JOIN "fl_Arc" AS r3 ON r3."y" = r0."a") WHERE r2."k" < r1."k"
  ),
  "fl_R3__R4__R5__R6f_join_2" AS NOT MATERIALIZED (
    SELECT r0."c" AS "c", r0."d" AS "d", r0."e" AS "e", r1."a" AS "a" FROM ("fl_R3__R4_join" AS r0 JOIN "fl_R5__R6f_by_R6f" AS r1 ON r1."e" = r0."e" AND r1."c" = r0."c")
  ),
  "fl_R1__R2__R3__R4__R5__R6__R6f_join_2" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM ("fl_R1__R2__R6_join" AS r0 JOIN "fl_R3__R4__R5__R6f_join_2" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R3__R4__R5__R6__R6f_join" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."b" AND r1."y" = r0."d")
UNION ALL
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R3__R4__R5__R6__R6f_join_2" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."b" AND r1."y" = r0."d")
) AS answers;

SET disabled_optimizers = '';
