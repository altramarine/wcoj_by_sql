-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_OutDeg" AS MATERIALIZED (
    SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x"
  ),
  "fl_route_0" AS MATERIALIZED (
    SELECT r0."x" AS "x", r0."y" AS "y", CASE WHEN r1."k" <= r2."k" THEN 0 ELSE 1 END AS "__fl_case" FROM (("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."x") JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y")
  ),
  "fl_Arc_o0_o1_H" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_0" WHERE "__fl_case" = 1
  ),
  "fl_Arc_o0_o1_L" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_0" WHERE "__fl_case" = 0
  ),
  "fl_R2__R3__R7_by_R2" AS MATERIALIZED (
    SELECT r0."x" AS "b", r0."y" AS "c", r1."y" AS "d" FROM (("fl_Arc_o0_o1_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
    UNION ALL
    SELECT r0."x" AS "b", r0."y" AS "c", r1."y" AS "d" FROM (("fl_Arc_o0_o1_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."y")
  ),
  "fl_R1__R2__R3__R4__R6__R7__R8_join" AS NOT MATERIALIZED (
    SELECT r0."b" AS "a", r0."c" AS "b", r0."d" AS "c", r1."d" AS "d", r2."d" AS "e" FROM (("fl_R2__R3__R7_by_R2" AS r0 JOIN "fl_R2__R3__R7_by_R2" AS r1 ON r1."b" = r0."c" AND r1."c" = r0."d") JOIN "fl_R2__R3__R7_by_R2" AS r2 ON r2."b" = r0."d" AND r2."c" = r1."d")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R3__R4__R6__R7__R8_join" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."e" AND r1."y" = r0."a")
) AS answers;

SET disabled_optimizers = '';
