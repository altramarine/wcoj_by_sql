-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_T" AS MATERIALIZED (
    SELECT r0."x" AS "x", r0."y" AS "y", r1."y" AS "z" FROM ("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y")
  ),
  "fl_TriABC" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r0."z" AS "c" FROM ("fl_T" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."x" AND r1."y" = r0."z")
  ),
  "fl_TriADC" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "c", r0."z" AS "d" FROM ("fl_T" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."z" AND r1."y" = r0."x")
  ),
  "fl_TriADE" AS NOT MATERIALIZED (
    SELECT r0."z" AS "a", r0."x" AS "d", r0."y" AS "e" FROM ("fl_T" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."x" AND r1."y" = r0."z")
  ),
  "fl_ThreeTri" AS NOT MATERIALIZED (
    SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r2."e" AS "e" FROM (("fl_TriABC" AS r0 JOIN "fl_TriADC" AS r1 ON r1."a" = r0."a" AND r1."c" = r0."c") JOIN "fl_TriADE" AS r2 ON r2."a" = r0."a" AND r2."d" = r1."d")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM ((("fl_ThreeTri" AS r0 SEMI JOIN "fl_Arc" AS r1 ON r1."x" = r0."e" AND r1."y" = r0."b") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."c" AND r2."y" = r0."e") SEMI JOIN "fl_Arc" AS r3 ON r3."x" = r0."b" AND r3."y" = r0."d")
) AS answers;

SET disabled_optimizers = '';
