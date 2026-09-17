"""Exact variance under independent Rademacher signs, evaluated without sketches.

For Z=(s^T M t)(s^T f)(t^T g), use E[(s.u)^2(s.v)^2]
= ||u||^2 ||v||^2 + 2(u.v)^2 - 2 sum(u_i^2 v_i^2).
The graph M is a binary adjacency matrix (set semantics).
"""
import itertools
import math
from pathlib import Path
from time import perf_counter

import duckdb
import numpy as np

from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def variance_terms(a, b, f, g):
    f2, g2 = f*f, g*g
    F, G = float(f2.sum()), float(g2.sum())
    h = np.bincount(a, weights=g[b], minlength=len(f))  # M g
    l = np.bincount(b, weights=f[a], minlength=len(g))  # M^T f
    rows = np.bincount(a, minlength=len(f))
    cols = np.bincount(b, minlength=len(g))
    K = float(f @ h)
    terms = {
        'n_F_G': len(a)*F*G,
        '2F_norm_Mg_squared': 2*F*float(h @ h),
        '-2F_col_g_squared': -2*F*float(cols @ g2),
        '2G_norm_MT_f_squared': 2*G*float(l @ l),
        '3K_squared': 3*K*K,
        '-4_weighted_MT_f_squared': -4*float(g2 @ (l*l)),
        '-2G_row_f_squared': -2*G*float(rows @ f2),
        '-4_weighted_Mg_squared': -4*float(f2 @ (h*h)),
        '4_edge_f_squared_g_squared': 4*float(np.sum(f2[a]*g2[b])),
    }
    return K, math.fsum(terms.values()), terms


def check_formula():
    rng = np.random.default_rng(7)
    for _ in range(20):
        M = rng.integers(0, 2, size=(3, 3))
        a, b = np.nonzero(M)
        f, g = rng.random(3)*3, rng.random(3)*3
        values = []
        for signs in itertools.product((-1, 1), repeat=6):
            s, t = np.array(signs[:3]), np.array(signs[3:])
            values.append((s @ M @ t)*(s @ f)*(t @ g))
        K, var, _ = variance_terms(a, b, f, g)
        np.testing.assert_allclose(K, np.mean(values), atol=1e-10)
        np.testing.assert_allclose(var, np.var(values), rtol=1e-12, atol=1e-10)


def main():
    check_formula()
    started = perf_counter()
    with duckdb.connect(config={'threads': 4, 'memory_limit': '4GB'}) as con:
        load_graph(con, ROOT/DATASETS['topcats'])
        edges = con.execute('SELECT DISTINCT col0, col1 FROM R').fetchnumpy()
    a, b = edges['col0'], edges['col1']
    n = len(a)
    degree = np.bincount(a, minlength=int(max(a.max(), b.max()))+1)
    f = np.sqrt(degree)
    K, var, terms = variance_terms(a, b, f, f)
    samples = np.loadtxt(ROOT/'log/topcats-1-sign-sketch.samples.tsv', skiprows=1)
    z = samples[:, 4]
    m = len(z)
    out = {
        'formula_check': '20 small matrices, all 64 sign assignments each: passed',
        'n': n, 'K': K, 'F': float(degree.sum()), 'G': float(degree.sum()),
        **terms,
        'exact_variance_Z': var, 'exact_sd_Z': math.sqrt(var),
        'empirical_sd_Z': float(z.std(ddof=1)),
        'empirical_variance_over_exact': float(z.var(ddof=1)/var),
        'm': m, 'estimate_K': float(z.mean()),
        'actual_relative_error': float(z.mean()/K-1),
        'exact_standard_error': math.sqrt(var/m),
        'exact_relative_standard_error': math.sqrt(var/m)/K,
        'observed_error_in_standard_errors': float((z.mean()-K)/math.sqrt(var/m)),
        'negative_sample_fraction': float((z < 0).mean()),
        'm_n_log_n': math.ceil(n/math.log(n)),
        'n_log_n_relative_standard_error': math.sqrt(var/math.ceil(n/math.log(n)))/K,
        'm_for_10pct_relative_RMSE': math.ceil(var/(.1*K)**2),
        'm_for_5pct_relative_RMSE': math.ceil(var/(.05*K)**2),
        'seconds': perf_counter()-started,
    }
    path = ROOT/'log/topcats-1-sign-variance.log'
    with path.open('w') as log:
        for key, val in out.items():
            line = f'{key}\t{val}'
            print(line, flush=True)
            print(line, file=log)


if __name__ == '__main__':
    main()
