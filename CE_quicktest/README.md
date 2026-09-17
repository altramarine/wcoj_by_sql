# Cardinality estimator quick tests

This folder contains the quick-test scripts and the logs produced while studying split-join candidate estimates for Topcats `queries/1.sql`:

```text
q(x, y, z) :- R(x, y), R(y, z), R(x, z)
```

The exact split candidate count is

\[
S=\sum_{(a,b)\in R_1}\min(x_a,y_b)=779,094,625,
\]

with (N=28,511,807) distinct R1 edges. The baseline fixed-order bounds are 2,112,368,511 and 2,662,974,839.

The main completed results are:

| Method | Result |
|---|---:|
| Exact baseline | 779,094,625 |
| Sorted matching of edge-weighted degree marginals | 1,871,015,295 |
| Truncated additive bound, best \(\tau=418\) | 1,871,698,414 |
| Exact edge moment \(\sum\sqrt{x_a y_b}\) | 1,476,746,354 |
| Uniform edge sampling, 5,340 samples | 95th-percentile absolute error 2.81% |
| 1% induced-vertex subgraph, triangle output | 151 average sampled triangles over 10 trials |
| 1% induced-vertex subgraph, split count scaled by \(p^{-3}\) | Not valid; degree shrinkage causes bias |
| 1% sampled vertices, original degrees on sampled edges, scaled by \(p^{-2}\) | Mean 780,933,000; relative SD 10.02% |
| Random-sign sketch, \(m=5,340\) | Relative error +225.12% |

The scripts are diagnostic experiments. Several use full edge/degree data while preparing validation statistics; those preparation costs are recorded in the corresponding logs and must not be mistaken for an (o(N)) optimizer-time algorithm.

The key distinction is between sampling R1 edges and running an induced vertex subgraph. For sampled R1 edges with original degrees, an edge is retained with probability about (p^2), so (p^{-2}) is the appropriate Horvitz–Thompson scaling. In an induced subgraph, candidate degrees also shrink, and there is no universal (p^{-3}) correction for `min`.

Scripts can be run from the repository root, for example:

```bash
.venv/bin/python CE_quicktest/quick_test_vertex_subgraph.py --trials 10
.venv/bin/python CE_quicktest/quick_test_vertex_sample.py --fraction 0.01
```

`log/` contains the preserved outputs and intermediate statistics for all quick tests.
