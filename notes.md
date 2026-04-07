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