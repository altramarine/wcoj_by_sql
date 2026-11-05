-- Readable equivalent of hand_selected_split/5cycle_2/9rr.dl.
-- Input R(col0,col1). Fresh connection, nonNULL integer graph values.
-- Full query: ab, bc, cd, de, ea, ac, bd.
SET disabled_optimizers = 'join_order,build_side_probe_side';

CREATE TEMP TABLE Arc AS
SELECT DISTINCT col0 AS src, col1 AS dst FROM R;

CREATE TEMP TABLE OutDeg AS
SELECT src AS vertex, count(*) AS degree FROM Arc GROUP BY src;

-- Same predicate and tie policy as Arc_o1_o0_L / Arc_o1_o0_H.
-- Inner joins deliberately keep absent degree keys absent, as in the .dl.
CREATE TEMP TABLE Arc_split AS
SELECT e.src, e.dst,
       CASE WHEN dy.degree <= dx.degree THEN 0 ELSE 1 END AS tag
FROM Arc e
JOIN OutDeg dy ON dy.vertex = e.dst
JOIN OutDeg dx ON dx.vertex = e.src;

-- The two cases retain their own join order and only meet at a set UNION.
CREATE TEMP TABLE T_left AS
SELECT e.src AS a, e.dst AS b, bc.dst AS c
FROM Arc_split e
JOIN Arc bc ON bc.src = e.dst
JOIN Arc ac ON ac.src = e.src AND ac.dst = bc.dst
WHERE e.tag = 0
UNION
SELECT e.src AS a, e.dst AS b, ac.dst AS c
FROM Arc_split e
JOIN Arc ac ON ac.src = e.src
JOIN Arc bc ON bc.src = e.dst AND bc.dst = ac.dst
WHERE e.tag = 1;

-- Keep the original triangle and use this set only as an edge filter.
-- Multiple different a witnesses must produce one (b,c) filter tuple.
CREATE TEMP TABLE R2f AS
SELECT DISTINCT b,c FROM T_left;

CREATE TEMP TABLE T_right AS
SELECT e.src AS b, e.dst AS c, cd.dst AS d
FROM Arc_split e
JOIN R2f f ON f.b = e.src AND f.c = e.dst
JOIN Arc cd ON cd.src = e.dst
JOIN Arc bd ON bd.src = e.src AND bd.dst = cd.dst
WHERE e.tag = 0
UNION
SELECT e.src AS b, e.dst AS c, bd.dst AS d
FROM Arc_split e
JOIN R2f f ON f.b = e.src AND f.c = e.dst
JOIN Arc bd ON bd.src = e.src
JOIN Arc cd ON cd.src = e.dst AND cd.dst = bd.dst
WHERE e.tag = 1;

CREATE TEMP TABLE P_dea AS
SELECT de.src AS d, de.dst AS e, ea.dst AS a
FROM Arc de JOIN Arc ea ON de.dst = ea.src;

-- Each full (a,b,c,d,e) assignment fixes one tuple in each set input.
-- This last rule is unique on all its variables, so COUNT can stream.
SELECT count(*)
FROM T_left l
JOIN T_right r ON l.b = r.b AND l.c = r.c
JOIN P_dea p ON r.d = p.d AND l.a = p.a;

SET disabled_optimizers = '';
