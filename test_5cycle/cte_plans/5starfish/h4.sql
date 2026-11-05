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
  "fl_Arc_o0_o1_L" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_0" WHERE "__fl_case" = 0
  ),
  "fl_Arc_o0_o1_H" AS NOT MATERIALIZED (
    SELECT "x", "y" FROM "fl_route_0" WHERE "__fl_case" = 1
  ),
  "fl_R1__R6__R7_by_R1" AS MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "f" FROM (("fl_Arc_o0_o1_L" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."x") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."y" AND r2."y" = r1."y")
    UNION ALL
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "f" FROM (("fl_Arc_o0_o1_H" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
  ),
  "fl_InDeg" AS MATERIALIZED (
    SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y"
  ),
  "fl_R1__R2__R6__R7__R8_by_R1__R6__R7" AS MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r3."b" AS "c", r0."f" AS "f" FROM ((("fl_R1__R6__R7_by_R1" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."b") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."f") JOIN "fl_R1__R6__R7_by_R1" AS r3 ON r3."a" = r0."b" AND r3."f" = r0."f") WHERE r1."k" <= r2."k"
  ),
  "fl_R10__R3__R4__R8__R9_by_R10__R4__R9" AS NOT MATERIALIZED (
    SELECT r0."b" AS "e", r0."f" AS "f", r3."a" AS "c", r0."a" AS "d" FROM ((("fl_R1__R6__R7_by_R1" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."b") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."f") JOIN "fl_R1__R6__R7_by_R1" AS r3 ON r3."b" = r0."a" AND r3."f" = r0."f") WHERE r2."k" < r1."k"
  ),
  "fl_R1__R2__R3__R6__R7__R8__R9_by_R1__R6__R7" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r3."a" AS "c", r3."b" AS "d", r0."f" AS "f" FROM (((("fl_R1__R6__R7_by_R1" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."b") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."f") JOIN "fl_R1__R6__R7_by_R1" AS r3 ON r3."f" = r0."f") SEMI JOIN "fl_R1__R6__R7_by_R1" AS r4 ON r4."a" = r0."b" AND r4."b" = r3."a" AND r4."f" = r0."f") WHERE r2."k" < r1."k"
  )
SELECT r0."c" AS "a", r1."b" AS "b", r0."b" AS "e", r0."f" AS "f", r1."c" AS "c", r0."a" AS "d" FROM (("fl_R1__R2__R6__R7__R8_by_R1__R6__R7" AS r0 JOIN "fl_R1__R2__R6__R7__R8_by_R1__R6__R7" AS r1 ON r1."a" = r0."c" AND r1."f" = r0."f") SEMI JOIN "fl_R1__R6__R7_by_R1" AS r2 ON r2."a" = r1."c" AND r2."b" = r0."a" AND r2."f" = r0."f")
UNION ALL
SELECT r1."b" AS "a", r2."b" AS "b", r0."e" AS "e", r0."f" AS "f", r0."c" AS "c", r0."d" AS "d" FROM (("fl_R10__R3__R4__R8__R9_by_R10__R4__R9" AS r0 JOIN "fl_R1__R6__R7_by_R1" AS r1 ON r1."a" = r0."e" AND r1."f" = r0."f") JOIN "fl_R1__R2__R6__R7__R8_by_R1__R6__R7" AS r2 ON r2."a" = r1."b" AND r2."c" = r0."c" AND r2."f" = r0."f")
UNION ALL
SELECT r1."b" AS "a", r2."b" AS "b", r0."b" AS "e", r0."f" AS "f", r2."c" AS "c", r0."a" AS "d" FROM (("fl_R1__R6__R7_by_R1" AS r0 JOIN "fl_R1__R6__R7_by_R1" AS r1 ON r1."a" = r0."b" AND r1."f" = r0."f") JOIN "fl_R1__R2__R3__R6__R7__R8__R9_by_R1__R6__R7" AS r2 ON r2."a" = r1."b" AND r2."d" = r0."a" AND r2."f" = r0."f")
) AS answers;

SET disabled_optimizers = '';
