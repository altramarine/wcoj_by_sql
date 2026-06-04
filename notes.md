For q4

q(a, b, c, d) :- R(a, b), R(b, c), R(a, c), R(b ,d), R(c, d)

a normal plan would be 
```
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R2 [R1, R3, R4, R5]
-- [1] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R1__R2__R3, R5]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [3] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- [4] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R1__R2__R3, R5]
-- | [5] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [6] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- [7] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R2__R4__R5, R3]
-- | [8] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [9] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
-- [10] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R2__R4__R5, R3]
-- | [11] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [12] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
```

But we found that actually we are way faster w/ merging node1 w/ node4 and merging node7 w/ node10.

Hence the plan is:

```
-- R1(a, b), R2(b, c), R3(a, c), R4(b, d), R5(c, d)
-- R2 [R1, R3, R4, R5]
-- [1/4] R1__R2__R3(a, b, c), R4(b, d), R5(c, d)
-- | R4 [R1__R2__R3, R5]
-- | [2] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [3] R1__R2__R3(a, b, c), R4__R5(b, d, c) [acyclic]
-- [7/10] R1(a, b), R2__R4__R5(b, c, d), R3(a, c)
-- | R1 [R2__R4__R5, R3]
-- | [8] R1__R2__R3__R4__R5(a, b, c, d) [acyclic]
-- | [9] R1__R3(a, b, c), R2__R4__R5(b, c, d) [acyclic]
```

How to generalize it?
- if single case goes to different subcases, we should merge them! 
  -- The following join is actually ```[gen_R_1|gen_R_2] join {other relations}```

- What happens if query is a clique? we can not merge them but still we might have some duplicated calculation.
  -- e.g. we get {[r1, r2] [r3, r4] r5} twice in 5cycles, we can not join them? is there a sematic to define what's necessary?

Suppose a Function \Sigma : join graph, \Est -> split, candidates, where \Est is that you give any join according to join graph.

How to do join? Does a split help w/ Estimation?

The question is in what case we should simply split the work?