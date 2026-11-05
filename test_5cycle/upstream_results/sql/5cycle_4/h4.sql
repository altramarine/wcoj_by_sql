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
  "fl_R1__R2__R6_by_R1" AS MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM (("fl_Arc_o0_o1_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
    UNION ALL
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM (("fl_Arc_o0_o1_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."y")
  ),
  "fl_InDeg" AS MATERIALIZED (
    SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y"
  ),
  "fl_route_1" AS MATERIALIZED (
    SELECT r0."x" AS "x", r0."y" AS "y", CASE WHEN r1."k" <= r2."k" THEN 0 ELSE 1 END AS "__fl_case" FROM (("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x")
  ),
  "fl_Arc_o1_i0_L" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_1" WHERE "__fl_case" = 0
  ),
  "fl_R6f" AS MATERIALIZED (
    SELECT DISTINCT r0."a" AS "a", r0."c" AS "c" FROM "fl_R1__R2__R6_by_R1" AS r0
  ),
  "fl_Arc_o1_i0_H" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_1" WHERE "__fl_case" = 1
  ),
  "fl_R5__R6f__R8f_by_R8f" AS NOT MATERIALIZED (
    SELECT r0."y" AS "e", r2."y" AS "a", r0."x" AS "c" FROM ((("fl_Arc_o1_i0_L" AS r0 SEMI JOIN "fl_R6f" AS r1 ON r1."a" = r0."x" AND r1."c" = r0."y") JOIN "fl_Arc" AS r2 ON r2."x" = r0."y") SEMI JOIN "fl_R6f" AS r3 ON r3."a" = r2."y" AND r3."c" = r0."x")
    UNION ALL
    SELECT r0."y" AS "e", r2."a" AS "a", r0."x" AS "c" FROM ((("fl_Arc_o1_i0_H" AS r0 SEMI JOIN "fl_R6f" AS r1 ON r1."a" = r0."x" AND r1."c" = r0."y") JOIN "fl_R6f" AS r2 ON r2."c" = r0."x") SEMI JOIN "fl_Arc" AS r3 ON r3."x" = r0."y" AND r3."y" = r2."a")
  ),
  "fl_R1__R2__R3__R4__R5__R6__R6f__R8__R8f_join" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."b" AS "d", r1."c" AS "e" FROM (("fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_R1__R2__R6_by_R1" AS r1 ON r1."a" = r0."c") SEMI JOIN "fl_R5__R6f__R8f_by_R8f" AS r2 ON r2."e" = r1."c" AND r2."a" = r0."a" AND r2."c" = r0."c")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R3__R4__R5__R6__R6f__R8__R8f_join" AS r0 SEMI JOIN "fl_R1__R2__R6_by_R1" AS r1 ON r1."a" = r0."b" AND r1."b" = r0."c" AND r1."c" = r0."d")
) AS answers;

SET disabled_optimizers = '';
