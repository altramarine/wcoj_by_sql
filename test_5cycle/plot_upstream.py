"""Plot plan spread and summarize the upstream-configured SQL sweep."""
import argparse
from collections import Counter
import csv
import json
import os
from pathlib import Path
import tempfile

os.environ.setdefault('MPLCONFIGDIR', str(Path(tempfile.gettempdir()) / 'wcoj-matplotlib-cache'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', nargs='?', type=Path, default=Path(__file__).parent / 'upstream_results')
    args = parser.parse_args()
    root = args.directory
    records = [json.loads(line) for line in (root / 'results.jsonl').read_text().splitlines() if line.strip()]
    metadata = json.loads((root / 'run_metadata.json').read_text())
    queries = list(dict.fromkeys(row['query'] for row in records))
    datasets = list(dict.fromkeys(Path(row['dataset_file']).stem for row in records))
    summary = {'records': len(records), 'statuses': dict(Counter(row['status'] for row in records)),
               'direct_cq_mismatches': sum(row['correctness_match'] is False for row in records),
               'historical_only_verified': sum(row['status'] == 'ok' and row['correctness_match'] is None
                                               and row['historical_count_matches'] is True for row in records),
               'historical_counts_checked': sum(row['historical_count_matches'] is not None for row in records),
               'historical_count_mismatches': sum(row['historical_count_matches'] is False for row in records),
               'cells': []}
    fig, axes = plt.subplots(len(datasets), 1, figsize=(15, 4.8 * len(datasets)), squeeze=False)
    colors = {'split': '#3274a1', 'binary': '#e58b32'}
    for axis, dataset in zip(axes[:, 0], datasets):
        for index, query in enumerate(queries):
            rows = [row for row in records if row['query'] == query and Path(row['dataset_file']).stem == dataset]
            if not rows:
                continue
            # A default CQ may time out while translated plans finish. Keep
            # historical-count verification explicit rather than dropping them.
            ok = {row['algorithm']: row for row in rows if row['status'] == 'ok' and
                  (row['correctness_match'] is True or (row['correctness_match'] is None
                   and row['historical_count_matches'] is True))}
            groups = {'split': {name: row for name, row in ok.items() if name.startswith('h')},
                      'binary': {name: row for name, row in ok.items() if name.startswith('b') and name != 'base'}}
            cell = {'dataset': dataset, 'scale': rows[0]['scale'], 'query': query,
                    'completed': len(ok), 'attempted': len(rows),
                    'failed': sum(row['status'] != 'ok' for row in rows),
                    'unverified': sum(row['status'] == 'ok' and row['algorithm'] not in ok for row in rows)}
            for offset, (group, members) in zip((-.17, .17), groups.items()):
                if members:
                    values = [row['elapsed_s_mean'] for row in members.values()]
                    boxes = axis.boxplot([values], positions=[index + offset], widths=.25,
                                         patch_artist=True, manage_ticks=False, showfliers=False, whis=(0, 100),
                                         medianprops={'color': 'black'})
                    boxes['boxes'][0].set(facecolor=colors[group], alpha=.35)
                    axis.scatter([index + offset] * len(values), values, s=13, color=colors[group], alpha=.8, zorder=3)
                    best = min(members, key=lambda name: members[name]['elapsed_s_mean'])
                    cell[f'best_{group}_plan'] = best
                    cell[f'best_{group}_seconds'] = members[best]['elapsed_s_mean']
            for name, marker, color in [('duckdb_default', 'D', 'black'), ('base', '*', '#218838'), ('onerule', 'x', '#9b51a6')]:
                if name in ok:
                    value = ok[name]['elapsed_s_mean']
                    axis.scatter(index, value, marker=marker, color=color, s=55, zorder=5)
                    cell[f'{name}_seconds'] = value
            if cell.get('best_split_seconds') and cell.get('duckdb_default_seconds'):
                cell['default_over_best_split'] = cell['duckdb_default_seconds'] / cell['best_split_seconds']
            failed = [row for row in rows if row['status'] != 'ok']
            if failed:
                transform = axis.get_xaxis_transform()
                axis.scatter(index, .97, transform=transform, marker='^', color='#b62626', s=45)
                label = f'{len(failed)} failed'
                if len(failed) == 1:
                    name = 'default' if failed[0]['algorithm'] == 'duckdb_default' else failed[0]['algorithm']
                    reason = {'memory_out': 'OOM', 'timeout': 'timeout'}.get(failed[0]['status'], 'error')
                    label = f'{name}: {reason}'
                axis.annotate(label, (index, .97), xycoords=transform,
                              xytext=(0, -15), textcoords='offset points', ha='center', fontsize=8, color='#b62626')
            summary['cells'].append(cell)
        axis.set_yscale('log')
        axis.set_ylabel('Execution time (seconds, log scale)')
        axis.set_xticks(range(len(queries)), queries)
        axis.set_xlim(-.55, len(queries) - .45)
        scale_label = 'full graph' if dataset == 'amazon' else '30% vertices; starfish: 15% vertices'
        axis.set_title(f'{dataset}: {scale_label}', loc='left', fontweight='bold')
        axis.grid(axis='y', which='both', alpha=.2)
    handles = [Patch(facecolor=colors['split'], alpha=.4, label='Split/hybrid plans (h*)'),
               Patch(facecolor=colors['binary'], alpha=.4, label='Binary plans (b*)'),
               Line2D([], [], marker='D', color='black', linestyle='', label='Default DuckDB CQ'),
               Line2D([], [], marker='*', color='#218838', linestyle='', markersize=10, label='Translated upstream baseline'),
               Line2D([], [], marker='x', color='#9b51a6', linestyle='', label='Fixed-order one-rule plan')]
    failures = any(row['status'] != 'ok' for row in records)
    if failures:
        handles.append(Line2D([], [], marker='^', color='#b62626', linestyle='', label='Failed run (top marker)'))
    fig.legend(handles=handles, loc='upper center', ncol=3, bbox_to_anchor=(.5, .98))
    fig.suptitle('Upstream hand-selected workload, ordered WITH SQL backend', y=1.01, fontsize=16)
    note = (f"DuckDB {metadata['duckdb']}; {metadata['workers']} threads; seed={metadata['seed']}; "
            f"{metadata['repeat']} execution(s)/plan; timeout={metadata['timeout_seconds']:g}s; "
            f"memory={metadata['memory_limit']}; spilling disabled.\n"
            'Boxes show spread across plans. Input loading excluded; degree/routing included.')
    if failures:
        note += '\nFailure triangles sit at the top of the panel and do not represent a runtime.'
    fig.text(.02, .008, note, fontsize=9)
    fig.tight_layout(rect=(0, .05 if failures else .03, 1, .91))
    fig.savefig(root / 'runtime_spread.png', dpi=180, bbox_inches='tight')
    fig.savefig(root / 'runtime_spread.pdf', bbox_inches='tight')
    plt.close(fig)
    (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    columns = ['dataset', 'scale', 'query', 'completed', 'attempted', 'failed', 'unverified',
               'duckdb_default_seconds', 'base_seconds', 'onerule_seconds',
               'best_split_plan', 'best_split_seconds', 'best_binary_plan', 'best_binary_seconds', 'default_over_best_split']
    with (root / 'summary.tsv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, delimiter='\t', lineterminator='\n', restval='NA')
        writer.writeheader()
        writer.writerows(summary['cells'])
    with (root / 'historical_counts.tsv').open('w') as stream:
        writer = csv.writer(stream, delimiter='\t', lineterminator='\n')
        writer.writerow(['dataset', 'scale', 'query', 'algorithm', 'sql_count', 'historical_flowlog_count',
                         'count_matches', 'sql_seconds', 'historical_flowlog_seconds', 'historical_flowlog_commit'])
        for row in records:
            old = row['historical_flowlog']
            if old:
                writer.writerow([Path(row['dataset_file']).stem, row['scale'], row['query'], row['algorithm'],
                                 row['result_value'], old['result_value'], row['historical_count_matches'],
                                 row['elapsed_s_mean'], old['elapsed_s_mean'], old['flowlog_commit']])
    print(json.dumps({key: value for key, value in summary.items() if key != 'cells'}, indent=2))
    print('Saved', root / 'runtime_spread.png')


if __name__ == '__main__':
    main()
