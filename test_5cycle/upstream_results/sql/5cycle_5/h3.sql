-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_InDeg" AS MATERIALIZED (
    SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y"
  ),
  "fl_route_0" AS MATERIALIZED (
    SELECT r0."x" AS "x", r0."y" AS "y", CASE WHEN r1."k" <= r2."k" THEN 0 ELSE 1 END AS "__fl_case" FROM (("fl_Arc" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."x") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."y")
  ),
  "fl_Arc_i0_i1_H" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_0" WHERE "__fl_case" = 1
  ),
  "fl_OutDeg" AS MATERIALIZED (
    SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x"
  ),
  "fl_route_1" AS MATERIALIZED (
    SELECT r0."x" AS "x", r0."y" AS "y", CASE WHEN r1."k" <= r2."k" THEN 0 ELSE 1 END AS "__fl_case" FROM (("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x")
  ),
  "fl_Arc_o1_i0_L" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_1" WHERE "__fl_case" = 0
  ),
  "fl_Arc_o1_i0_H" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_1" WHERE "__fl_case" = 1
  ),
  "fl_R1__R7__R9_by_R1" AS MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "d" FROM (("fl_Arc_o1_i0_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r1."y" AND r2."y" = r0."x")
    UNION ALL
    SELECT r0."x" AS "a", r0."y" AS "b", r1."x" AS "d" FROM (("fl_Arc_o1_i0_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."x")
  ),
  "fl_R1__R2__R3__R7__R9_by_R3" AS NOT MATERIALIZED (
    SELECT r1."a" AS "a", r1."b" AS "b", r0."x" AS "c", r0."y" AS "d" FROM (("fl_Arc_i0_i1_H" AS r0 JOIN "fl_R1__R7__R9_by_R1" AS r1 ON r1."d" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r1."b" AND r2."y" = r0."x")
  ),
  "fl_R1__R4__R7__R9_by_R4" AS NOT MATERIALIZED (
    SELECT r1."a" AS "a", r1."b" AS "b", r0."x" AS "d", r0."y" AS "e" FROM ("fl_Arc_o1_i0_H" AS r0 JOIN "fl_R1__R7__R9_by_R1" AS r1 ON r1."d" = r0."x")
  ),
  "fl_Arc_i0_i1_L" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_0" WHERE "__fl_case" = 0
  ),
  "fl_R2__R3_by_R3" AS MATERIALIZED (
    SELECT r1."x" AS "b", r0."x" AS "c", r0."y" AS "d" FROM ("fl_Arc_i0_i1_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."x")
  ),
  "fl_R4__R5__R6__R8_by_R4" AS NOT MATERIALIZED (
    SELECT r0."x" AS "d", r0."y" AS "e", r1."b" AS "a", r1."d" AS "c" FROM ("fl_Arc_o1_i0_L" AS r0 JOIN "fl_R1__R7__R9_by_R1" AS r1 ON r1."a" = r0."y")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r1."y" AS "e" FROM (("fl_R1__R2__R3__R7__R9_by_R3" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."d") SEMI JOIN "fl_R1__R7__R9_by_R1" AS r2 ON r2."a" = r1."y" AND r2."b" = r0."a" AND r2."d" = r0."c")
UNION ALL
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM (("fl_R1__R4__R7__R9_by_R4" AS r0 JOIN "fl_R2__R3_by_R3" AS r1 ON r1."b" = r0."b" AND r1."d" = r0."d") SEMI JOIN "fl_R1__R7__R9_by_R1" AS r2 ON r2."a" = r0."e" AND r2."b" = r0."a" AND r2."d" = r1."c")
UNION ALL
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r0."d" AS "d", r2."e" AS "e" FROM (("fl_R1__R7__R9_by_R1" AS r0 JOIN "fl_R2__R3_by_R3" AS r1 ON r1."b" = r0."b" AND r1."d" = r0."d") JOIN "fl_R4__R5__R6__R8_by_R4" AS r2 ON r2."d" = r0."d" AND r2."a" = r0."a" AND r2."c" = r1."c")
) AS answers;

SET disabled_optimizers = '';
