"""Bound sum(min(outdegree(source),outdegree(destination))) with vertex capacities.

Uses exact counts of vertices with each (outdegree, indegree) pair. These are
vertex statistics, not degree pairs along edges. Cached evaluation uses no edges.
"""

import argparse
import json
from bisect import bisect_right
from collections import defaultdict
from contextlib import nullcontext
from pathlib import Path
from time import perf_counter

import duckdb

from lp_statistics import load_graph
from quick_test_cauchy import ROOT, prepare_profile, exact_candidates
from quick_test_lp import report
from run_sql import DATASETS


class DegreeTotals:
    def __init__(self, values):
        self.values = sorted(set(values))
        self.index = {v: i+1 for i,v in enumerate(self.values)}
        self.counts = [0]*(len(self.values)+1)
        self.sums = [0]*(len(self.values)+1)
        self.total = 0

    def add(self, value, count):
        self.total += count
        i = self.index[value]
        while i < len(self.counts):
            self.counts[i] += count
            self.sums[i] += count*value
            i += i & -i

    def capped_sum(self, cap):
        i = bisect_right(self.values, cap)
        count = value = 0
        while i:
            count += self.counts[i]
            value += self.sums[i]
            i -= i & -i
        return value + cap*(self.total-count)


def capacity_bounds(rows):
    rows = [(int(o),int(i),int(n)) for o,i,n in rows if n]
    if any(min(o,i,n)<0 for o,i,n in rows):
        raise ValueError("Degrees and counts must be nonnegative")
    outgoing = DegreeTotals(o for o,i,n in rows)
    incoming = DegreeTotals(i for o,i,n in rows)
    groups = defaultdict(list)
    for o,i,n in rows:
        if o: groups[o].append((i,n))
    degrees = sorted(groups,reverse=True)
    totals = dict(matching=0, pair_capacity=0, directional_capacity=0)
    nodes = outsum = insum = 0
    for idx,d in enumerate(degrees):
        for indegree,count in groups[d]:
            nodes += count
            outsum += d*count
            insum += indegree*count
            outgoing.add(d,count)
            incoming.add(indegree,count)
        next_degree = degrees[idx+1] if idx+1<len(degrees) else 0
        width = d-next_degree
        totals['matching'] += width*min(outsum,insum)
        totals['pair_capacity'] += width*min(outsum,insum,nodes*nodes)
        totals['directional_capacity'] += width*min(
            outgoing.capped_sum(nodes),incoming.capped_sum(nodes))
    if not totals['directional_capacity']<=totals['pair_capacity']<=totals['matching']:
        raise AssertionError("Capacity constraints increased the bound")
    return totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument('--dataset',choices=sorted(DATASETS),default='topcats')
    source.add_argument('--profile',type=Path)
    parser.add_argument('--save-profile',type=Path)
    parser.add_argument('--exact',action='store_true')
    parser.add_argument('--log',type=Path)
    args=parser.parse_args()
    if args.profile and args.exact:
        parser.error('--exact requires graph input, not --profile')
    if args.log: args.log.parent.mkdir(parents=True,exist_ok=True)
    with (args.log.open('w') if args.log else nullcontext()) as log, \
            (nullcontext() if args.profile else duckdb.connect(config={'memory_limit':'4GB','threads':4})) as con:
        if args.profile:
            started=perf_counter()
            payload=json.loads(args.profile.read_text())
            if payload.get('kind')!='vertex_degree_pairs':
                raise ValueError('Expected a vertex_degree_pairs profile')
            rows=payload['rows'];dataset=payload['dataset']
            report(f'profile_load_seconds\t{perf_counter()-started:.6f}',log)
        else:
            dataset=args.dataset
            started=perf_counter()
            load_graph(con,ROOT/DATASETS[dataset]);prepare_profile(con)
            report(f'load_and_degree_seconds\t{perf_counter()-started:.6f}',log)
            started=perf_counter()
            rows=con.execute('SELECT dout,din,COUNT(*) FROM cauchy_profile GROUP BY dout,din').fetchall()
            report(f'vertex_profile_collection_seconds\t{perf_counter()-started:.6f}',log)
        report(f'dataset\t{dataset}',log)
        report(f'distinct_vertex_degree_pairs\t{len(rows)}',log)
        if args.save_profile:
            args.save_profile.parent.mkdir(parents=True,exist_ok=True)
            args.save_profile.write_text(json.dumps(dict(kind='vertex_degree_pairs',dataset=dataset,rows=rows)))
        started=perf_counter()
        bounds=capacity_bounds(rows)
        report(f'bound_seconds\t{perf_counter()-started:.6f}',log)
        for name,value in bounds.items():report(f'{name}\t{value}',log)
        if args.exact:
            started=perf_counter();actual=exact_candidates(con)
            report(f'exact_sum_min\t{actual}',log)
            report(f'exact_validation_seconds\t{perf_counter()-started:.6f}',log)
            if actual>bounds['directional_capacity']:
                raise AssertionError('Capacity bound is below actual split work')
        report('note\tCached counts of each vertex indegree/outdegree pair; no edge pairings in the cache.',log)


if __name__=='__main__':main()
