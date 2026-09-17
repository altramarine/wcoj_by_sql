"""Topcats triangle weighted-join random sign sketch; standalone experiment."""
import argparse
import math
import struct
import subprocess
import tempfile
from pathlib import Path
from time import perf_counter

import duckdb
import numpy as np

from lp_statistics import load_graph
from run_sql import DATASETS

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', default='topcats', choices=sorted(DATASETS))
    parser.add_argument('--threads', type=int, default=16)
    parser.add_argument('--seed', type=int, default=20260915)
    parser.add_argument('--size-rule', choices=['sqrt', 'n-log-n'], default='sqrt')
    parser.add_argument('--log', type=Path, default=ROOT/'log/topcats-1-sign-sketch.log')
    args = parser.parse_args()
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with args.log.open('w') as log, tempfile.TemporaryDirectory(prefix='sign-sketch-') as tmp:
        def report(s):
            print(s, flush=True)
            print(s, file=log, flush=True)
        report(f'dataset\t{args.dataset}\nquery\tqueries/1.sql\nmethod\tindependent Rademacher signs\nsize_rule\t{args.size_rule}; log means natural logarithm')
        start = perf_counter()
        with duckdb.connect(config={'threads': 4, 'memory_limit': '4GB'}) as con:
            load_graph(con, ROOT / DATASETS[args.dataset])
            cols = con.execute('SELECT DISTINCT col0, col1 FROM R ORDER BY col0, col1').fetchnumpy()
        a, b = cols['col0'], cols['col1']
        if len(a) == 0 or min(a.min(), b.min()) < 0 or max(a.max(), b.max()) >= 2**32:
            raise ValueError('Expected nonempty graph with uint32 vertex IDs')
        edges = np.column_stack((a,b)).astype(np.uint32)
        n = len(edges)
        m = math.isqrt(n)
        m += m*m < n
        if args.size_rule == 'n-log-n':
            if n <= 1:
                raise ValueError('n/log(n) requires n > 1')
            m = math.ceil(n / math.log(n))
        path = Path(tmp)/'edges.bin'
        with path.open('wb') as f:
            f.write(struct.pack('=QQ', n, int(edges.max())+1))
            edges.tofile(f)
        report(f'prepare_seconds\t{perf_counter()-start:.6f}')
        exe = Path(tmp)/'sketch'
        subprocess.run(['g++','-O3','-march=native','-fopenmp','-std=c++17',str(ROOT/'quick_test_sign_sketch.cpp'),'-o',str(exe)], check=True)
        with subprocess.Popen([str(exe),str(path),str(m),str(args.threads),str(args.seed)], stdout=subprocess.PIPE, text=True) as proc:
            for line in proc.stdout:
                report(line.rstrip())
            if proc.wait():
                raise RuntimeError(f'Sketch exited {proc.returncode}')
        samples = args.log.with_suffix('.samples.tsv')
        samples.write_bytes(Path(str(path)+'.samples.tsv').read_bytes())
        report(f'samples\t{samples}\nwarning\tUnbiased estimate, not a deterministic upper bound; empirical SE is diagnostic only.')


if __name__ == '__main__':
    main()
