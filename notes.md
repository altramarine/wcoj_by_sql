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