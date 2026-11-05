-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT count(*) AS result_count FROM (
WITH
  "fl_Arc" AS MATERIALIZED (
    SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R"
  ),
  "fl_TriABC" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "b", r1."y" AS "c" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
  ),
  "fl_TriACE" AS NOT MATERIALIZED (
    SELECT r0."x" AS "a", r0."y" AS "c", r1."y" AS "e" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r1."y" AND r2."y" = r0."x")
  ),
  "fl_TriCDE" AS NOT MATERIALIZED (
    SELECT r0."x" AS "c", r0."y" AS "d", r1."y" AS "e" FROM (("fl_Arc" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."y") SEMI JOIN "fl_Arc" AS r2 ON r2."x" = r0."x" AND r2."y" = r1."y")
  )
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r2."d" AS "d", r1."e" AS "e" FROM (("fl_TriABC" AS r0 JOIN "fl_TriACE" AS r1 ON r1."a" = r0."a" AND r1."c" = r0."c") JOIN "fl_TriCDE" AS r2 ON r2."c" = r0."c" AND r2."e" = r1."e")
) AS answers;

SET disabled_optimizers = '';
