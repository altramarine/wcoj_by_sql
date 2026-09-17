# README

To use:

```
uv sync

run.sh
```

## Paper datasets

`download.sh` retains the repository's original four downloads (`skitters`,
`topcats`, `gplus`, and `uspatent`) and adds the six datasets from Table 8 of
the paper. Each dataset is converted to a headerless `source,destination` CSV
under `datasets/`; existing CSV files are left untouched. Downloaded `.txt.gz`
and `.tar.gz` archives are removed automatically after successful conversion:

```bash
./download.sh
```

With no arguments, the command downloads only the original four datasets.
The paper datasets must be selected explicitly by name.

Individual datasets can be selected by name. This is useful for the very large
Twitter-2010 graph (the compressed download alone is about 5.5 GB, and the CSV
requires substantially more space):

```bash
./download.sh skitters topcats gplus uspatent
./download.sh epinions livejournal berkstan google amazon
./download.sh twitter
```

Run `./download.sh --help` for the accepted names and output-directory option.

## Robustness tests and plots

Run robustness plans with a 15-minute timeout per query:

```bash
./run_robustness_test.sh \
  -d skitters \
  -t 900 \
  -o robustness-test/results-skitters-15min.tsv
```

Multiple datasets can be supplied with spaces, commas, or repeated `-d`
arguments. Without `-o`, each dataset is written to its own result file:

```bash
./run_robustness_test.sh -d "skitters,epinions,amazon" -t 900
```

The regular result format is:

```text
family  query  status  execution_time
```

Run the normal SQL queries in `queries/normal` as baselines. Results for every
selected dataset are appended to the shared `robustness-test/baseline.tsv`:

```bash
./run_robustness_test.sh \
  -b \
  -d "skitters epinions amazon" \
  -t 900
```

The baseline result format is:

```text
dataset  query  status  execution_time
```

`robustness-test/test.sh` keeps the commands for the complete experiment. Commands
for result files that have already been generated are commented out; currently the
active command appends to `baseline.tsv`.

Generate all regular and filtered plots from the `results-*-15min.tsv` files:

```bash
./robustness-test/plot.sh
```

The plots report slowdown relative to the matching baseline on a base-2 logarithmic
axis. A successful baseline is the normalization time. If a baseline times out or
runs out of memory, the median of the successful binary plans for that dataset and
query type is used for both binary and WCOJ. That fallback time is shown in red below
the query label. The baseline `1x` sample is included only in the binary box plot.

Each box plot uses the five-number summary directly: 0%, 25%, 50%, 75%, and 100%.
Timeout and out-of-memory plans are excluded from the box statistics and shown as
red counts aligned at the top of the plot. The filtered plots additionally exclude
disconnected binary plans containing `CROSS JOIN`.

## Quick test: fractional Lp bounds for triangle 1.sql

Compare `p = 0, 1, 2, 3, 4, 5, infinity` with the same statistics plus
`p = 1/2, 1/3, 1/4, 1/5`, and a third group using only those fractional
moments (no key-count, integer-norm, or maximum-degree constraints):

```bash
uv run python quick_test_lp.py --dataset epinions
```

Save the summary and the existing LP formatter's variables and active boundary
inequalities for all three estimates (overwrites the named log):

```bash
uv run python quick_test_lp.py --dataset topcats --log log/topcats-1-lp.log
```

The script uses `queries/1.sql` and its entropy LP template. All moments are
collected together per physical column and reused across estimates. It reports
only triangle output upper bounds, using both degree directions of all three
atoms. It does not execute the triangle join. Inputs use set semantics.
Loading, statistics collection, and each LP solve are
timed separately; the first LP timing includes lazy solver import/startup, so it
should not be compared directly with subsequent solve timings.

The general statistics CLI also supports reciprocal moments:

```bash
uv run python lp_statistics.py --dataset epinions --query queries/1.sql \
  --lp queries/lp_boundaries/1.sql.txt --max-p 5 --include-reciprocals
```

## Quick test: asymmetric Cauchy bounds

Run a separate experiment for the first split of `queries/1.sql`:

```bash
uv run python quick_test_cauchy.py --dataset topcats --exact --log log/topcats-1-cauchy.log
```

This tests 21 exponents: `0.05, 0.10, ..., 0.95`, plus `1/3` and `2/3`.
For each `p`, it reports
`sqrt(sum(dout^(1+2p)) * sum(din*dout^(2-2p)))`, alongside both fixed
candidate counts. All moments are aggregated together over a degree profile
that aligns each vertex's indegree and outdegree. `--exact` separately validates
the split work by summing `min(dout(source), dout(destination))` over distinct
edges; it does not run the triangle join. Loading, degree preparation, moment
collection, and exact validation are timed separately. The reported best exponent
is the best tested grid point, not a continuous optimum. This experiment does
not modify the triangle-output LP test.

For different conjugate Holder exponents `r=1/t`, `s=1/(1-t)`, run:

```bash
uv run python quick_test_holder.py --dataset topcats --exact --log log/topcats-1-holder.log
```

This evaluates `A^(1/r) * B^(1/s)`, where
`A=sum(dout^(1+p*r))` and `B=sum(din*dout^((1-p)*s))`.
It tests 399 `(p,t)` grid pairs, then refines numerically over a bounded interior.
The degree profile is compressed by exactly equal outdegrees (no approximate
bins); numerical moment evaluations reuse that in-memory profile. This uses more
information than the original cached scalar norm set, and the reported profile
collection/search costs should be accounted for separately. The log includes the
best Cauchy point, best Holder grid point, refinement status, and fixed-plan
comparison. Refinement is not a certified global optimum.

## Quick test: geometric versus additive min-power bounds

```bash
uv run python quick_test_min_power.py --dataset topcats --log log/topcats-1-min-power.log
```

For `p = 1/5, 1/4, 1/3, 1/2, 1, 2, 3, 4, 5` and
`alpha = 1/4, 1/2, 3/4`, compare the exact sum of `min(x,y)^p` with
`sum(x^(p*alpha)*y^(p*(1-alpha)))` and `sum(x^p+y^p)` along the edges.
This means `i=2*p*alpha`, `j=2*p*(1-alpha)`, ensuring `i+j=2*p`.
All 27 parameter combinations share one aggregate query over degree-annotated
edges. This is an exact-statistics experiment, not a cheap planning estimator.
The final log columns convert the power-sum bounds into bounds on `sum(min)`:
for `p>=1` they use `N^(1-1/p)*bound^(1/p)`; for `p<1` they use the valid but
potentially loose `bound^(1/p)` without a negative power of `N`.

## Quick test: negative power means

```bash
uv run python quick_test_negative_mean.py --dataset topcats --log log/topcats-1-negative-mean.log
```

Tests the pairwise mean `F_k(x,y)=((x^(-k)+y^(-k))/2)^(-1/k)` for
`k=1/4,1/2,1,2,4,8,16,32`. It satisfies
`min(x,y) <= F_k(x,y) <= 2^(1/k)*min(x,y)` and converges down to min;
a zero input uses the limiting value zero. The SQL uses an equivalent expression
with min/max for numerical stability, so this experiment is not a cheaper
alternative to exact min. The log first verifies a counterexample to substituting
negative p into LpBound's positive-p entropy inequality. These pairwise means
are not negative-p LP estimates, and the existing LP statistics API is unchanged.

## Quick test: cheap coupling bounds for negative means

Collect exact weighted degree marginals and optionally validate against edges:

```bash
uv run python quick_test_coupling.py --dataset topcats --exact \
  --save-profile log/topcats-degree-marginals.json --log log/topcats-1-coupling.log
```

Evaluate bounds from the saved profile without reading the graph or degree tables:

```bash
uv run python quick_test_coupling.py --profile log/topcats-degree-marginals.json \
  --log log/topcats-1-coupling-cached.log
```

Each source degree is weighted by its edge occurrences, and each destination
degree by the total indegree of vertices with that outdegree. Equal degrees are
compressed exactly; this is not approximate binning. Sorted matching maximizes
the negative power mean over all couplings of these marginals, while reverse
matching minimizes it. The `inf` row applies the same procedure directly to min.
This needs richer cached statistics than a few scalar norms. The profiles must
be regenerated when the data changes. Ignoring actual edge pairing makes the
bound conservative; it does not exactly recover the true negative-mean sum.

## Quick test: both-direction vertex capacities

```bash
uv run python quick_test_degree_capacity.py --dataset topcats --exact \
  --save-profile log/topcats-vertex-degree-pairs.json --log log/topcats-1-degree-capacity.log
uv run python quick_test_degree_capacity.py --profile log/topcats-vertex-degree-pairs.json \
  --log log/topcats-1-degree-capacity-cached.log
```

For each integer threshold t, let V_t contain vertices with outdegree at least t.
The number of edges within V_t cannot exceed either the sum of
`min(outdegree(v), |V_t|)` or the sum of `min(indegree(v), |V_t|)` over V_t.
Summing the smaller capacity over thresholds bounds the split candidate count.
Fenwick trees evaluate these sums at degree change points without rescanning
vertices for every threshold. The cached profile contains counts of vertices
with each exact (outdegree, indegree) pair, not pairings of vertices across edges.
This uses set semantics, permits self-loops, and is specific to the self-join in
`queries/1.sql`. Its capacity bound is provably no larger than the marginal
matching bound. Data changes require profile regeneration.

## ```make_split_plan.py```

This is a maker for split plans, split framework is:

```
ask_for_plan(query: a bunch of relations)
  1. upon acyclic, return.
  2. ask for [relation to split], and [consider joining with which of following relations].
  3. recursively go to another relation, try with acyclic joins. 
```

Q: this is the most 'general' way to describe a split that I can ever think of, but is this the 'right framework'?

This gives a tree-like structure on splitting, on each split we go into different subqueries.

### Random split-plan generator

`robustness-test/random_plan_generator.py` drives `make_split_plan.py` through its interactive stdin/stdout
interface. Its default `wcoj` mode chooses one relation and two relations that share a
variable with it at each split. The `binary` mode delegates to the binary-plan generator.

```bash
uv run python robustness-test/random_plan_generator.py --seed 7 \
  'q(a,b,c,d) :- R(a,b), R(b,c), R(a,d), R(d,c)' > random-plan.sql
```

Generate a random binary join plan:

```bash
uv run python robustness-test/random_plan_generator.py --mode binary --seed 7 \
  'q(a,b,c,d) :- R(a,b), R(b,c), R(a,d), R(d,c)' > random-binary-plan.sql
```

The query can also be read from a file or stdin:

```bash
uv run python robustness-test/random_plan_generator.py --query-file queries/2.sql > random-plan.sql
```

An example of returned along with comment for the plan is 
```sql
CREATE VIEW R1 AS SELECT col0 AS a, col1 AS b FROM R;
CREATE VIEW R2 AS SELECT col0 AS b, col1 AS c FROM R;
CREATE VIEW R3 AS SELECT col0 AS c, col1 AS d FROM R;
CREATE VIEW R4 AS SELECT col0 AS d, col1 AS a FROM R;
-- tree:
-- R1(a, b), R2(b, c), R3(c, d), R4(d, a)
-- R1 [R2, R4]
-- [1] R1__R2(a, b, c), R3(c, d), R4(d, a)
-- | R3 [R1__R2, R4]
-- | [2] R1__R2__R3__R4(a, b, c, d) [acyclic]
-- | [3] R1__R2(a, b, c), R3__R4(c, d, a) [acyclic]
-- [4] R1__R4(a, b, d), R2(b, c), R3(c, d)
-- | R3 [R2, R1__R4]
-- | [5] R1__R4(a, b, d), R2__R3(b, c, d) [acyclic]
-- | [6] R1__R2__R3__R4(a, b, d, c) [acyclic]
CREATE TEMP TABLE node0_best AS WITH
  cnt_R2 as (SELECT b, COUNT(*) as cnt FROM R2 GROUP BY b),
  best_R2 as (SELECT R1.a as a, R1.b as b, 0 as tag, cnt_R2.cnt as cnt FROM R1, cnt_R2 WHERE cnt_R2.b = R1.b),
  cnt_R4 as (SELECT a, COUNT(*) as cnt FROM R4 GROUP BY a),
  best_R4 as (SELECT best_R2.a as a, best_R2.b as b, CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R4.cnt THEN best_R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R2, cnt_R4 WHERE cnt_R4.a = best_R2.a),
SELECT a, b, tag FROM best_R4;
CREATE TEMP TABLE node1_R1__R2 AS SELECT node0_best.a as a, node0_best.b as b, R2.c as c FROM node0_best JOIN R2 ON node0_best.b = R2.b WHERE node0_best.tag = 0;
CREATE TEMP TABLE node4_R1__R4 AS SELECT node0_best.a as a, node0_best.b as b, R4.d as d FROM node0_best JOIN R4 ON node0_best.a = R4.a WHERE node0_best.tag = 1;
CREATE TEMP TABLE node1_best AS WITH
  cnt_R1__R2 as (SELECT c, COUNT(*) as cnt FROM node1_R1__R2 GROUP BY c),
  best_R1__R2 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R1__R2.cnt as cnt FROM R3, cnt_R1__R2 WHERE cnt_R1__R2.c = R3.c),
  cnt_R4 as (SELECT d, COUNT(*) as cnt FROM R4 GROUP BY d),
  best_R4 as (SELECT best_R1__R2.c as c, best_R1__R2.d as d, CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.tag ELSE 1 END as tag,CASE WHEN best_R1__R2.cnt < cnt_R4.cnt THEN best_R1__R2.cnt ELSE cnt_R4.cnt END as cnt FROM best_R1__R2, cnt_R4 WHERE cnt_R4.d = best_R1__R2.d),
SELECT c, d, tag FROM best_R4;
CREATE TEMP TABLE node4_best AS WITH
  cnt_R2 as (SELECT c, COUNT(*) as cnt FROM R2 GROUP BY c),
  best_R2 as (SELECT R3.c as c, R3.d as d, 0 as tag, cnt_R2.cnt as cnt FROM R3, cnt_R2 WHERE cnt_R2.c = R3.c),
  cnt_R1__R4 as (SELECT d, COUNT(*) as cnt FROM node4_R1__R4 GROUP BY d),
  best_R1__R4 as (SELECT best_R2.c as c, best_R2.d as d, CASE WHEN best_R2.cnt < cnt_R1__R4.cnt THEN best_R2.tag ELSE 1 END as tag,CASE WHEN best_R2.cnt < cnt_R1__R4.cnt THEN best_R2.cnt ELSE cnt_R1__R4.cnt END as cnt FROM best_R2, cnt_R1__R4 WHERE cnt_R1__R4.d = best_R2.d),
SELECT c, d, tag FROM best_R1__R4;
SELECT COUNT(*) FROM (
SELECT node2_R1__R2__R3__R4.a as a, node2_R1__R2__R3__R4.b as b, node2_R1__R2__R3__R4.c as c, node2_R1__R2__R3__R4.d as d FROM (SELECT node1_R1__R2.a as a, node1_R1__R2.b as b, node1_best.c as c, node1_best.d as d FROM node1_best JOIN node1_R1__R2 ON node1_best.c = node1_R1__R2.c SEMI JOIN R4 ON R4.d = node1_best.d AND R4.a = node1_R1__R2.a WHERE node1_best.tag = 0) node2_R1__R2__R3__R4
UNION ALL
SELECT node3_R3__R4.a as a, node1_R1__R2.b as b, node3_R3__R4.c as c, node3_R3__R4.d as d FROM (SELECT node1_best.c as c, node1_best.d as d, R4.a as a FROM node1_best JOIN R4 ON node1_best.d = R4.d WHERE node1_best.tag = 1) node3_R3__R4 JOIN node1_R1__R2 ON node1_R1__R2.a = node3_R3__R4.a AND node1_R1__R2.c = node3_R3__R4.c
UNION ALL
SELECT node4_R1__R4.a as a, node5_R2__R3.b as b, node5_R2__R3.c as c, node5_R2__R3.d as d FROM (SELECT R2.b as b, node4_best.c as c, node4_best.d as d FROM node4_best JOIN R2 ON node4_best.c = R2.c WHERE node4_best.tag = 0) node5_R2__R3 JOIN node4_R1__R4 ON node4_R1__R4.b = node5_R2__R3.b AND node4_R1__R4.d = node5_R2__R3.d
UNION ALL
SELECT node6_R1__R2__R3__R4.a as a, node6_R1__R2__R3__R4.b as b, node6_R1__R2__R3__R4.c as c, node6_R1__R2__R3__R4.d as d FROM (SELECT node4_R1__R4.a as a, node4_R1__R4.b as b, node4_best.d as d, node4_best.c as c FROM node4_best JOIN node4_R1__R4 ON node4_best.d = node4_R1__R4.d SEMI JOIN R2 ON R2.b = node4_R1__R4.b AND R2.c = node4_best.c WHERE node4_best.tag = 1) node6_R1__R2__R3__R4
);
```
