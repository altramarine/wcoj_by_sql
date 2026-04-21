Problems with directly writing: 

1. We should use semi join whenever it is a projection, otherwise it won't be detected.
2. Sometimes CTE optimization is messed up.
3. The Union All part is slow!

Union would have amplification -> use WITH to forbid this!

To force materialization

```
WITH query_0 AS MATERIALIZED (
  SELECT ...
),
query_1 AS MATERIALIZED (
  ...
)
```

variation would:

for every bitmask
1. generate query_{bitmask} from possible best_{past_bitmask} 
2. search for every possible extension (pos_id, var_id), as best_{bitmask}

- large constant merging query_{bitmask} to every query
- intermediate result from query is large.

- best_{bitmask} is materialized - to wait for query_{bitmask}
- extra overhead compared to normal WCOJ 
  - var_id introduced and materialized. this is not bottleneck!
  - every varaible is scanned multiple times
  - where does q2's runtime overhead comefrom?

q2: 
wcoj_var: len(prop_1111) = 13576777569
wcoj    : len(prop_3) = 13627736762

q3: 
wcoj_var: len(prop_1111) = 10267520216
wcoj    : len(prop_3) = 12736152112


NOW: muti-threading optimization is bad. when threads=20 we have quite good performance

两个 RIGHT_SEMI JOIN 合计占了 82s CPU（80%），且 operator_cardinality 分别是 1498万和 1270万行，但它们的 build side 是通过 INNER JOIN 膨胀出来的 2.4亿/2.86亿行中间结果。

这正是之前看到的问题：build side 选错了，大表做 build，小表做 probe，hash table 巨大导致 cache miss 严重。

UNION 两个分支是串行的（operator_timing: 0.00s 但 cpu_time 85s），说明它们没有并行，是顺序执行的。