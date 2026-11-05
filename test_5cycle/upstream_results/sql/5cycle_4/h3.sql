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
  "fl_Arc_o1_i0_H" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_1" WHERE "__fl_case" = 1
  ),
  "fl_R5__R6__R8_by_R5" AS MATERIALIZED (
    SELECT r0."x" AS "e", r0."y" AS "a", r1."y" AS "c" FROM (("fl_Arc_o1_i0_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r1."y" AND r2."y" = r0."x")
    UNION ALL
    SELECT r0."x" AS "e", r0."y" AS "a", r1."x" AS "c" FROM (("fl_Arc_o1_i0_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."y" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."x")
  ),
  "fl_R1__R2__R5__R6__R8_by_R5__R6__R8" AS MATERIALIZED (
    SELECT r0."a" AS "a", r3."b" AS "b", r0."c" AS "c", r0."e" AS "e" FROM ((("fl_R5__R6__R8_by_R5" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."a") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."e") JOIN "fl_R1__R2__R6_by_R1" AS r3 ON r3."a" = r0."a" AND r3."c" = r0."c") WHERE r1."k" <= r2."k"
  ),
  "fl_R2__R3__R4__R7__R8_by_R3__R4__R8" AS NOT MATERIALIZED (
    SELECT r3."a" AS "b", r0."a" AS "c", r0."b" AS "d", r0."c" AS "e" FROM ((("fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."a") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a") JOIN "fl_R1__R2__R6_by_R1" AS r3 ON r3."b" = r0."a" AND r3."c" = r0."b") WHERE r2."k" < r1."k"
  ),
  "fl_R1__R2__R3__R6__R7_by_R1__R2__R6" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r3."c" AS "d" FROM ((("fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."b") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a") JOIN "fl_R1__R2__R6_by_R1" AS r3 ON r3."a" = r0."b" AND r3."b" = r0."c") WHERE r1."k" <= r2."k"
  ),
  "fl_R3__R4__R5__R6__R8_by_R5__R6__R8" AS MATERIALIZED (
    SELECT r0."c" AS "c", r3."b" AS "d", r0."e" AS "e", r0."a" AS "a" FROM ((("fl_R5__R6__R8_by_R5" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."a") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."e") JOIN "fl_R1__R2__R6_by_R1" AS r3 ON r3."a" = r0."c" AND r3."c" = r0."e") WHERE r2."k" < r1."k"
  )
SELECT r3."a" AS "a", r3."b" AS "b", r0."a" AS "c", r0."b" AS "d", r0."c" AS "e" FROM (((("fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."a") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a") JOIN "fl_R1__R2__R5__R6__R8_by_R5__R6__R8" AS r3 ON r3."c" = r0."a" AND r3."e" = r0."c") SEMI JOIN "fl_R1__R2__R6_by_R1" AS r4 ON r4."a" = r3."b" AND r4."b" = r0."a" AND r4."c" = r0."b") WHERE r1."k" <= r2."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R5__R6__R8_by_R5__R6__R8" AS r0 JOIN "fl_R2__R3__R4__R7__R8_by_R3__R4__R8" AS r1 ON r1."b" = r0."b" AND r1."c" = r0."c" AND r1."e" = r0."e")
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r1."e" AS "e" FROM ("fl_R1__R2__R3__R6__R7_by_R1__R2__R6" AS r0 JOIN "fl_R3__R4__R5__R6__R8_by_R5__R6__R8" AS r1 ON r1."c" = r0."c" AND r1."d" = r0."d" AND r1."a" = r0."a")
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r3."d" AS "d", r3."e" AS "e" FROM (((("fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."b") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a") JOIN "fl_R3__R4__R5__R6__R8_by_R5__R6__R8" AS r3 ON r3."c" = r0."c" AND r3."a" = r0."a") SEMI JOIN "fl_R1__R2__R6_by_R1" AS r4 ON r4."a" = r0."b" AND r4."b" = r0."c" AND r4."c" = r3."d") WHERE r2."k" < r1."k"
) AS answers;

SET disabled_optimizers = '';
