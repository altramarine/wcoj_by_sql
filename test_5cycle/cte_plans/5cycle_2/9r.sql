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
    SELECT r0."x" AS "a", r0."y" AS "b", CASE WHEN r1."k" <= r2."k" THEN 0 ELSE 1 END AS "__fl_case" FROM (("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."x")
  ),
  "fl_RL_oo" AS NOT MATERIALIZED (
    SELECT "a", "b" FROM "fl_route_0" WHERE "__fl_case" = 0
  ),
  "fl_RH_oo" AS NOT MATERIALIZED (
    SELECT "a", "b" FROM "fl_route_0" WHERE "__fl_case" = 1
  ),
  "fl_R1__R2__R6_by_R1" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r1."y" AS "c" FROM (("fl_RL_oo" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."b") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."a" AND r2."y" = r1."y")
    UNION ALL
    SELECT r0."a" AS "a", r0."b" AS "b", r1."y" AS "c" FROM (("fl_RH_oo" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."a") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."b" AND r2."y" = r1."y")
  ),
  "fl_R2f__R3__R7_by_R2f" AS NOT MATERIALIZED (
    SELECT r0."a" AS "b", r0."b" AS "c", r1."y" AS "d" FROM (("fl_RL_oo" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."b") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."a" AND r2."y" = r1."y")
    UNION ALL
    SELECT r0."a" AS "b", r0."b" AS "c", r1."y" AS "d" FROM (("fl_RH_oo" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."a") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."b" AND r2."y" = r1."y")
  ),
  "fl_R4__R5_join" AS NOT MATERIALIZED (
    SELECT r0."x" AS "d", r0."y" AS "e", r1."y" AS "a" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r2."e" AS "e" FROM (("fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_R2f__R3__R7_by_R2f" AS r1 ON r1."b" = r0."b" AND r1."c" = r0."c") JOIN "fl_R4__R5_join" AS r2 ON r2."d" = r1."d" AND r2."a" = r0."a")
) AS answers;

SET disabled_optimizers = '';
