-- Faithful nonrecursive Datalog rewrite with set semantics.
-- Input: R(col0, col1).
-- Degree expressions are from the rule bodies, not plan comments.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE "fl_Arc" AS SELECT DISTINCT "col0" AS "x", "col1" AS "y" FROM "R";

CREATE TEMP TABLE "fl_OutDeg" AS SELECT "x" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "x";

CREATE TEMP TABLE "fl_R1__R2__R6_by_R1" AS SELECT r0."x" AS "a", r0."y" AS "b", r3."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_Arc" AS r3 ON r3."x" = r0."y" JOIN "fl_Arc" AS r4 ON r4."x" = r0."x" AND r4."y" = r3."y" WHERE r1."k" <= r2."k"
UNION
SELECT r0."x" AS "a", r0."y" AS "b", r3."y" AS "c" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_OutDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_Arc" AS r3 ON r3."x" = r0."x" JOIN "fl_Arc" AS r4 ON r4."x" = r0."y" AND r4."y" = r3."y" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_InDeg" AS SELECT "y" AS "x", count(*) AS "k" FROM "fl_Arc" GROUP BY "y";

CREATE TEMP TABLE "fl_R6f" AS SELECT DISTINCT r0."a" AS "a", r0."c" AS "c" FROM "fl_R1__R2__R6_by_R1" AS r0;

CREATE TEMP TABLE "fl_R3__R6f_by_R6f" AS SELECT r0."c" AS "c", r3."y" AS "d", r0."a" AS "a" FROM "fl_R6f" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."c" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a" JOIN "fl_Arc" AS r3 ON r3."x" = r0."c" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_R3__R4__R5__R6f_by_R4" AS SELECT r3."c" AS "c", r0."x" AS "d", r0."y" AS "e", r3."a" AS "a" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_R3__R6f_by_R6f" AS r3 ON r3."d" = r0."x" JOIN "fl_Arc" AS r4 ON r4."x" = r0."y" AND r4."y" = r3."a" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_R1__R2__R3__R4__R5__R6__R6f_join" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_R3__R4__R5__R6f_by_R4" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a";

CREATE TEMP TABLE "fl_R4__R5_by_R4" AS SELECT r0."x" AS "d", r0."y" AS "e", r3."y" AS "a" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_Arc" AS r3 ON r3."x" = r0."y" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_R3__R4__R5__R6f_join" AS SELECT r0."c" AS "c", r0."d" AS "d", r1."e" AS "e", r0."a" AS "a" FROM "fl_R3__R6f_by_R6f" AS r0 JOIN "fl_R4__R5_by_R4" AS r1 ON r1."d" = r0."d" AND r1."a" = r0."a";

CREATE TEMP TABLE "fl_R1__R2__R3__R4__R5__R6__R6f_join_2" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_R3__R4__R5__R6f_join" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a";

CREATE TEMP TABLE "fl_R3__R4_by_R4" AS SELECT r3."x" AS "c", r0."x" AS "d", r0."y" AS "e" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_Arc" AS r3 ON r3."y" = r0."x" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_R5__R6f_by_R6f" AS SELECT r3."x" AS "e", r0."a" AS "a", r0."c" AS "c" FROM "fl_R6f" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."c" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."a" JOIN "fl_Arc" AS r3 ON r3."y" = r0."a" WHERE r2."k" < r1."k";

CREATE TEMP TABLE "fl_R3__R4__R5__R6f_join_2" AS SELECT r0."c" AS "c", r0."d" AS "d", r0."e" AS "e", r1."a" AS "a" FROM "fl_R3__R4_by_R4" AS r0 JOIN "fl_R5__R6f_by_R6f" AS r1 ON r1."e" = r0."e" AND r1."c" = r0."c";

CREATE TEMP TABLE "fl_R1__R2__R3__R4__R5__R6__R6f_join_3" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_R3__R4__R5__R6f_join_2" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a";

CREATE TEMP TABLE "fl_R3__R4__R5__R6f_by_R4_2" AS SELECT r3."c" AS "c", r0."x" AS "d", r0."y" AS "e", r3."a" AS "a" FROM "fl_Arc" AS r0 JOIN "fl_OutDeg" AS r1 ON r1."x" = r0."y" JOIN "fl_InDeg" AS r2 ON r2."x" = r0."x" JOIN "fl_R5__R6f_by_R6f" AS r3 ON r3."e" = r0."y" JOIN "fl_Arc" AS r4 ON r4."x" = r3."c" AND r4."y" = r0."x" WHERE r1."k" <= r2."k";

CREATE TEMP TABLE "fl_R1__R2__R3__R4__R5__R6__R6f_join_4" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r1."d" AS "d", r1."e" AS "e" FROM "fl_R1__R2__R6_by_R1" AS r0 JOIN "fl_R3__R4__R5__R6f_by_R4_2" AS r1 ON r1."c" = r0."c" AND r1."a" = r0."a";

CREATE TEMP TABLE "fl_Result" AS SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R1__R2__R3__R4__R5__R6__R6f_join" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."b" AND r1."y" = r0."d"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R1__R2__R3__R4__R5__R6__R6f_join_2" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."b" AND r1."y" = r0."d"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R1__R2__R3__R4__R5__R6__R6f_join_3" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."b" AND r1."y" = r0."d"
UNION
SELECT r0."a" AS "a", r0."b" AS "b", r0."c" AS "c", r0."d" AS "d", r0."e" AS "e" FROM "fl_R1__R2__R3__R4__R5__R6__R6f_join_4" AS r0 JOIN "fl_Arc" AS r1 ON r1."x" = r0."b" AND r1."y" = r0."d";

SELECT count(*) AS result_count FROM "fl_Result";

SET disabled_optimizers = '';
