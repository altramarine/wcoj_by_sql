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
  "fl_InDeg" AS MATERIALIZED (
    SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y"
  ),
  "fl_R1__R5__R6_by_R5" AS MATERIALIZED (
    SELECT r0."y" AS "a", r5."y" AS "b", r0."x" AS "e" FROM (((((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_InDeg" AS r4 ON r4."x" = r0."y") JOIN "fl_Arc" AS r5 ON r5."x" = r0."y") SEMI JOIN "fl_Arc" AS r6 ON r6."x" = r0."x" AND r6."y" = r5."y") WHERE r1."k" <= r2."k" AND r1."k" <= r3."k" AND r1."k" <= r4."k"
    UNION ALL
    SELECT r0."y" AS "a", r5."y" AS "b", r0."x" AS "e" FROM (((((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_InDeg" AS r4 ON r4."x" = r0."y") JOIN "fl_Arc" AS r5 ON r5."x" = r0."x") SEMI JOIN "fl_Arc" AS r6 ON r6."x" = r0."y" AND r6."y" = r5."y") WHERE r3."k" < r1."k" AND r3."k" < r2."k" AND r3."k" <= r4."k"
  ),
  "fl_R1__R4__R5__R6__R7_by_R7" AS MATERIALIZED (
    SELECT r0."y" AS "a", r4."b" AS "b", r0."x" AS "d", r4."e" AS "e" FROM ((((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_R1__R5__R6_by_R5" AS r4 ON r4."a" = r0."y") SEMI JOIN "fl_Arc" AS r5 ON r5."x" = r0."x" AND r5."y" = r4."e") WHERE r1."k" <= r2."k" AND r1."k" <= r3."k"
  ),
  "fl_R2__R3_by_R2" AS MATERIALIZED (
    SELECT r0."x" AS "b", r0."y" AS "c", r3."y" AS "d" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_Arc" AS r3 ON r3."x" = r0."y") WHERE r1."k" <= r2."k"
  ),
  "fl_R3__R7_by_R7" AS MATERIALIZED (
    SELECT r4."x" AS "c", r0."x" AS "d", r0."y" AS "a" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_Arc" AS r4 ON r4."y" = r0."x") WHERE r2."k" < r1."k" AND r2."k" <= r3."k"
  ),
  "fl_R1__R4__R5__R6_by_R4" AS NOT MATERIALIZED (
    SELECT r3."a" AS "a", r3."b" AS "b", r0."x" AS "d", r0."y" AS "e" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R1__R5__R6_by_R5" AS r3 ON r3."e" = r0."y") WHERE r1."k" <= r2."k"
  ),
  "fl_R2__R3__R7_by_R3__R7" AS MATERIALIZED (
    SELECT r4."x" AS "b", r0."c" AS "c", r0."d" AS "d", r0."a" AS "a" FROM (((("fl_R3__R7_by_R7" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."a") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."c") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."d") JOIN "fl_Arc" AS r4 ON r4."y" = r0."c") WHERE r2."k" < r1."k" AND r2."k" <= r3."k"
  ),
  "fl_R1__R2__R5__R6_by_R2" AS MATERIALIZED (
    SELECT r3."a" AS "a", r0."x" AS "b", r0."y" AS "c", r3."e" AS "e" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R1__R5__R6_by_R5" AS r3 ON r3."b" = r0."x") WHERE r2."k" < r1."k"
  ),
  "fl_R3__R4__R7_by_R3__R7" AS MATERIALIZED (
    SELECT r0."c" AS "c", r0."d" AS "d", r4."y" AS "e", r0."a" AS "a" FROM (((("fl_R3__R7_by_R7" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."a") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."c") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."d") JOIN "fl_Arc" AS r4 ON r4."x" = r0."d") WHERE r3."k" < r1."k" AND r3."k" < r2."k"
  ),
  "fl_R4__R7_by_R7" AS MATERIALIZED (
    SELECT r0."x" AS "d", r4."y" AS "e", r0."y" AS "a" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_Arc" AS r4 ON r4."x" = r0."x") WHERE r3."k" < r1."k" AND r3."k" < r2."k"
  ),
  "fl_R3__R4__R7_by_R4__R7" AS NOT MATERIALIZED (
    SELECT r3."x" AS "c", r0."d" AS "d", r0."e" AS "e", r0."a" AS "a" FROM ((("fl_R4__R7_by_R7" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."e") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."d") JOIN "fl_Arc" AS r3 ON r3."y" = r0."d") WHERE r2."k" < r1."k"
  ),
  "fl_R1__R6_by_R6" AS MATERIALIZED (
    SELECT r4."x" AS "a", r0."y" AS "b", r0."x" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y") JOIN "fl_InDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_Arc" AS r4 ON r4."y" = r0."y") WHERE r1."k" <= r2."k" AND r1."k" <= r3."k"
  ),
  "fl_R1__R2__R6_by_R2" AS MATERIALIZED (
    SELECT r3."a" AS "a", r0."x" AS "b", r0."y" AS "c", r3."e" AS "e" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R1__R6_by_R6" AS r3 ON r3."b" = r0."x") WHERE r2."k" < r1."k"
  ),
  "fl_R4__R5__R7_by_R5" AS MATERIALIZED (
    SELECT r5."x" AS "d", r0."x" AS "e", r0."y" AS "a" FROM (((((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_InDeg" AS r4 ON r4."x" = r0."y") JOIN "fl_Arc" AS r5 ON r5."y" = r0."x") SEMI JOIN "fl_Arc" AS r6 ON r6."x" = r5."x" AND r6."y" = r0."y") WHERE r2."k" < r1."k" AND r2."k" <= r3."k" AND r2."k" <= r4."k"
    UNION ALL
    SELECT r5."x" AS "d", r0."x" AS "e", r0."y" AS "a" FROM (((((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_InDeg" AS r4 ON r4."x" = r0."y") JOIN "fl_Arc" AS r5 ON r5."y" = r0."y") SEMI JOIN "fl_Arc" AS r6 ON r6."x" = r5."x" AND r6."y" = r0."x") WHERE r4."k" < r1."k" AND r4."k" < r2."k" AND r4."k" < r3."k"
  ),
  "fl_R3__R4__R5__R7_by_R3" AS MATERIALIZED (
    SELECT r0."x" AS "c", r0."y" AS "d", r3."e" AS "e", r3."a" AS "a" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R4__R5__R7_by_R5" AS r3 ON r3."d" = r0."y") WHERE r1."k" <= r2."k"
  ),
  "fl_R2__R6_by_R6" AS MATERIALIZED (
    SELECT r0."y" AS "b", r4."y" AS "c", r0."x" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y") JOIN "fl_InDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_Arc" AS r4 ON r4."x" = r0."y") WHERE r2."k" < r1."k" AND r2."k" <= r3."k"
  ),
  "fl_R2__R3__R6_by_R3" AS MATERIALIZED (
    SELECT r3."b" AS "b", r0."x" AS "c", r0."y" AS "d", r3."e" AS "e" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R2__R6_by_R6" AS r3 ON r3."c" = r0."x") WHERE r2."k" < r1."k"
  ),
  "fl_R1__R4__R5__R7_by_R1" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r3."d" AS "d", r3."e" AS "e" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R4__R5__R7_by_R5" AS r3 ON r3."a" = r0."x") WHERE r2."k" < r1."k"
  ),
  "fl_R1__R2__R6_by_R1" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r3."c" AS "c", r3."e" AS "e" FROM ((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R2__R6_by_R6" AS r3 ON r3."b" = r0."y") WHERE r1."k" <= r2."k"
  ),
  "fl_R1__R4__R5__R6__R7_by_R6" AS MATERIALIZED (
    SELECT r4."a" AS "a", r0."y" AS "b", r4."d" AS "d", r0."x" AS "e" FROM ((((("fl_Arc" AS r0 JOIN "fl_InDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."y") JOIN "fl_InDeg" AS r3 ON r3."x" = r0."x") JOIN "fl_R4__R5__R7_by_R5" AS r4 ON r4."e" = r0."x") SEMI JOIN "fl_Arc" AS r5 ON r5."x" = r4."a" AND r5."y" = r0."y") WHERE r3."k" < r1."k" AND r3."k" < r2."k"
  )
SELECT r3."a" AS "a", r0."x" AS "b", r0."y" AS "c", r3."d" AS "d", r3."e" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R1__R4__R5__R6__R7_by_R7" AS r3 ON r3."b" = r0."x") SEMI JOIN "fl_Arc" AS r4 ON r4."x" = r0."y" AND r4."y" = r3."d") WHERE r2."k" < r1."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R4__R5__R6__R7_by_R7" AS r0 JOIN "fl_R2__R3_by_R2" AS r1 ON r1."b" = r0."b" AND r1."d" = r0."d")
UNION
SELECT r0."a" AS "a", r4."b" AS "b", r0."c" AS "c", r0."d" AS "d", r4."e" AS "e" FROM (((((("fl_R3__R7_by_R7" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."a") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."c") JOIN "fl_OutDeg" AS r3 ON r3."x" = r0."d") JOIN "fl_R1__R5__R6_by_R5" AS r4 ON r4."a" = r0."a") SEMI JOIN "fl_Arc" AS r5 ON r5."x" = r4."b" AND r5."y" = r0."c") SEMI JOIN "fl_Arc" AS r6 ON r6."x" = r0."d" AND r6."y" = r4."e") WHERE r1."k" <= r2."k" AND r1."k" <= r3."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R4__R5__R6_by_R4" AS r0 JOIN "fl_R2__R3__R7_by_R3__R7" AS r1 ON r1."b" = r0."b" AND r1."d" = r0."d" AND r1."a" = r0."a")
UNION
SELECT r3."a" AS "a", r3."b" AS "b", r3."c" AS "c", r0."x" AS "d", r0."y" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R2__R3__R7_by_R3__R7" AS r3 ON r3."d" = r0."x") SEMI JOIN "fl_R1__R5__R6_by_R5" AS r4 ON r4."a" = r3."a" AND r4."b" = r3."b" AND r4."e" = r0."y") WHERE r2."k" < r1."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R5__R6_by_R2" AS r0 JOIN "fl_R3__R4__R7_by_R3__R7" AS r1 ON r1."c" = r0."c" AND r1."e" = r0."e" AND r1."a" = r0."a")
UNION
SELECT r3."a" AS "a", r0."x" AS "b", r0."y" AS "c", r3."d" AS "d", r3."e" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R3__R4__R7_by_R3__R7" AS r3 ON r3."c" = r0."y") SEMI JOIN "fl_R1__R5__R6_by_R5" AS r4 ON r4."a" = r3."a" AND r4."b" = r0."x" AND r4."e" = r3."e") WHERE r1."k" <= r2."k"
UNION
SELECT r0."a" AS "a", r3."b" AS "b", r3."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM (((("fl_R4__R7_by_R7" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."e") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."d") JOIN "fl_R1__R2__R5__R6_by_R2" AS r3 ON r3."a" = r0."a" AND r3."e" = r0."e") SEMI JOIN "fl_Arc" AS r4 ON r4."x" = r3."c" AND r4."y" = r0."d") WHERE r1."k" <= r2."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R5__R6_by_R2" AS r0 JOIN "fl_R3__R4__R7_by_R4__R7" AS r1 ON r1."c" = r0."c" AND r1."e" = r0."e" AND r1."a" = r0."a")
UNION
SELECT r3."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r3."e" AS "e" FROM (((("fl_R2__R3_by_R2" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."d") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."b") JOIN "fl_R1__R5__R6_by_R5" AS r3 ON r3."b" = r0."b") SEMI JOIN "fl_R4__R7_by_R7" AS r4 ON r4."d" = r0."d" AND r4."e" = r3."e" AND r4."a" = r3."a") WHERE r2."k" < r1."k"
UNION
SELECT r3."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r3."e" AS "e" FROM (((("fl_R2__R3_by_R2" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."d") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."b") JOIN "fl_R4__R7_by_R7" AS r3 ON r3."d" = r0."d") SEMI JOIN "fl_R1__R5__R6_by_R5" AS r4 ON r4."a" = r3."a" AND r4."b" = r0."b" AND r4."e" = r3."e") WHERE r1."k" <= r2."k"
UNION
SELECT r3."a" AS "a", r3."b" AS "b", r0."x" AS "c", r0."y" AS "d", r3."e" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R1__R2__R6_by_R2" AS r3 ON r3."c" = r0."x") SEMI JOIN "fl_R4__R5__R7_by_R5" AS r4 ON r4."d" = r0."y" AND r4."e" = r3."e" AND r4."a" = r3."a") WHERE r2."k" < r1."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R6_by_R2" AS r0 JOIN "fl_R3__R4__R5__R7_by_R3" AS r1 ON r1."c" = r0."c" AND r1."e" = r0."e" AND r1."a" = r0."a")
UNION
SELECT r3."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r3."e" AS "e" FROM (((("fl_R2__R3_by_R2" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."d") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."b") JOIN "fl_R1__R6_by_R6" AS r3 ON r3."b" = r0."b") SEMI JOIN "fl_R4__R5__R7_by_R5" AS r4 ON r4."d" = r0."d" AND r4."e" = r3."e" AND r4."a" = r3."a") WHERE r2."k" < r1."k"
UNION
SELECT r3."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r3."e" AS "e" FROM (((("fl_R2__R3_by_R2" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."d") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."b") JOIN "fl_R4__R5__R7_by_R5" AS r3 ON r3."d" = r0."d") SEMI JOIN "fl_R1__R6_by_R6" AS r4 ON r4."a" = r3."a" AND r4."b" = r0."b" AND r4."e" = r3."e") WHERE r1."k" <= r2."k"
UNION
SELECT r0."x" AS "a", r0."y" AS "b", r3."c" AS "c", r3."d" AS "d", r3."e" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R2__R3__R6_by_R3" AS r3 ON r3."b" = r0."y") SEMI JOIN "fl_R4__R5__R7_by_R5" AS r4 ON r4."d" = r3."d" AND r4."e" = r3."e" AND r4."a" = r0."x") WHERE r1."k" <= r2."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R4__R5__R7_by_R1" AS r0 JOIN "fl_R2__R3__R6_by_R3" AS r1 ON r1."b" = r0."b" AND r1."d" = r0."d" AND r1."e" = r0."e")
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_R3__R4__R5__R7_by_R3" AS r1 ON r1."c" = r0."c" AND r1."e" = r0."e" AND r1."a" = r0."a")
UNION
SELECT r0."x" AS "a", r0."y" AS "b", r3."c" AS "c", r3."d" AS "d", r3."e" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R3__R4__R5__R7_by_R3" AS r3 ON r3."a" = r0."x") SEMI JOIN "fl_R2__R6_by_R6" AS r4 ON r4."b" = r0."y" AND r4."c" = r3."c" AND r4."e" = r3."e") WHERE r2."k" < r1."k"
UNION
SELECT r3."a" AS "a", r0."x" AS "b", r0."y" AS "c", r3."d" AS "d", r3."e" AS "e" FROM (((("fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y") JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x") JOIN "fl_R1__R4__R5__R6__R7_by_R6" AS r3 ON r3."b" = r0."x") SEMI JOIN "fl_Arc" AS r4 ON r4."x" = r0."y" AND r4."y" = r3."d") WHERE r2."k" < r1."k"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r1."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ("fl_R1__R4__R5__R6__R7_by_R6" AS r0 JOIN "fl_R2__R3_by_R2" AS r1 ON r1."b" = r0."b" AND r1."d" = r0."d")
) AS answers;

SET disabled_optimizers = '';
