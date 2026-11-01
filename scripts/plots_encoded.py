#!/usr/bin/env python3
"""Plot the encoded execution measurements."""
import json
import hashlib
from pathlib import Path
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np

HERE = Path(__file__).resolve().parents[1]
PLOTS = HERE / 'work/plots'
PLOTS.mkdir(parents=True,exist_ok=True)
mpl.rcParams.update({'pdf.fonttype': 42, 'ps.fonttype': 42, 'hatch.linewidth': .10})
# Rajan et al., Fig. 9: Times labels, vertical query labels, three colored
# series, fine diagonal background, gray grid, and an upper-right framed legend.
with mpl.rc_context({'font.family': 'serif', 'font.serif': ['Times New Roman'],
                     'font.size': 7.95, 'axes.labelsize': 7, 'axes.labelweight': 'bold',
                     'axes.edgecolor': '#C6C6C6', 'axes.linewidth': .4}):
    data = json.loads((HERE / 'plot_data/phases.json').read_text())
    fig, ax = plt.subplots(figsize=(3.36, 1.54))
    fig.subplots_adjust(left=.11, right=.975, bottom=.38, top=.95)
    ax.patch.set_edgecolor('#D6D6D6')
    ax.patch.set_hatch('x' * 16)
    x = np.arange(len(data))
    width = .20
    for delta, key, name, color in [
            (-width, 'setup_ms', 'Setup', '#4EA72E'),
            (0, 'query_ms', 'Aggregate + result', '#C00000'),
            (width, 'output_write_ms', 'Write', '#0E2841')]:
        ax.bar(x + delta, [r[key] for r in data], width=width, label=name,
               color=color, edgecolor='none', zorder=3)
    ax.set_xticks(x, ['IG/2', 'H/8', 'CMC/14', 'H/10'], rotation=90)
    ax.set_xlim(-.6, 3.6)
    ax.set_ylim(0, .65)
    ax.set_yticks(np.arange(0, .61, .1))
    ax.set_ylabel('Time (ms)', labelpad=2)
    ax.set_xlabel('Original query', labelpad=1)
    ax.tick_params(length=0, pad=2, labelcolor='#595959')
    ax.grid(color='#D9D9D9', linewidth=.45, zorder=1)
    ax.set_axisbelow(True)
    legend = ax.legend(loc='upper right', frameon=True, fancybox=False, framealpha=1,
                       fontsize=6.9, handlelength=.8, handletextpad=.35,
                       borderpad=.25, labelspacing=.18, borderaxespad=.2)
    legend.get_frame().set_edgecolor('#156082')
    legend.get_frame().set_linewidth(.45)
    fig.add_artist(Rectangle((.004, .008), .992, .984, transform=fig.transFigure,
                            fill=False, linewidth=.4, edgecolor='#D9D9D9'))
    fig.savefig(PLOTS / 'fig_stream_phases.svg')
    plt.close(fig)

# FastCompose Fig. 6 (bottom): closed axes, default blue CDF, light grid,
# bold sans-serif axis labels and a framed lower-right legend. No callout badges.
with mpl.rc_context({'font.family': 'DejaVu Sans', 'font.size': 6.2,
                     'axes.labelsize': 6.6, 'axes.labelweight': 'bold',
                     'axes.linewidth': .45, 'xtick.major.width': .45,
                     'ytick.major.width': .45, 'xtick.major.size': 2,
                     'ytick.major.size': 2}):
    grid = json.loads((HERE / 'plot_data/grid_points.json').read_text())
    values = np.sort([r['speedup'] for r in grid])
    fig, ax = plt.subplots(figsize=(3.36, 1.56))
    fig.subplots_adjust(left=.115, right=.978, bottom=.20, top=.975)
    ax.step(np.r_[0, values], np.r_[0, np.arange(1, len(values) + 1) / len(values)],
            where='post', color='#1F77B4', linewidth=1.0, label='BitSliceInverse')
    ax.set_xlabel('Speedup over DuckDB (×)', labelpad=2)
    ax.set_ylabel('Fraction of specs', labelpad=2)
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 1.1)
    ax.set_xticks(np.arange(0, 15, 2))
    ax.set_yticks(np.arange(0, 1.01, .2))
    ax.grid(color='#DEE0E2', linewidth=.55)
    ax.set_axisbelow(True)
    ax.legend(loc='lower right', fontsize=6, prop={'size': 6, 'weight': 'bold'},
              frameon=True, fancybox=False, framealpha=.95,
              edgecolor='#D9D9D9', borderpad=.25, handlelength=1.8)
    fig.savefig(PLOTS / 'fig_shared_grid.svg')
    plt.close(fig)

# Full-width comparison, kept separate from the small mechanism/phase plots.
# Both panels use retained medians and make their different timing scopes clear.
performance = json.loads((HERE / 'plot_data/performance.json').read_text())
styles = [
    ('duckdb_proj2', 'DuckDB', '#C00000'),
    ('arrow_proj', 'Arrow', '#0E2841'),
    ('vortex_proj', 'Vortex', '#5B9BD5'),
    ('generic', 'Native tree', '#A6A6A6'),
    ('stream', 'BitSliceInverse', '#4EA72E'),
]
with mpl.rc_context({'font.family': 'serif', 'font.serif': ['Times New Roman'],
                     'font.size': 8.5, 'axes.labelsize': 9, 'axes.linewidth': .55,
                     'xtick.major.width': .55, 'ytick.major.width': .55}):
    fig = plt.figure(figsize=(7.0, 2.65))
    ax = fig.add_axes([.075, .25, .575, .59])
    worker = fig.add_axes([.755, .25, .23, .59])
    x = np.arange(4)
    width = .145
    for j, (key, label, color) in enumerate(styles):
        values = [row['median_ms'][key] for row in performance['stored_queries']]
        bars = ax.bar(x + (j - 2) * width, values, width=width,
                      label=label, color=color, edgecolor='#333333', linewidth=.3, zorder=3)
        if key == 'stream':
            for bar, value in zip(bars, values):
                ax.text(bar.get_x() + bar.get_width() / 2, value * 1.12,
                        f'{value:.3f}', ha='center', va='bottom', fontsize=7.5,
                        fontweight='bold', color='#28601B')
    ax.set_yscale('log')
    ax.set_ylim(.1, 100)
    ax.set_yticks([.1, 1, 10, 100], ['0.1', '1', '10', '100'])
    ax.set_xticks(x, ['IG/2', 'Hatred/8', 'CityMax/14', 'Hatred/10'])
    ax.set_xlim(-.6, 3.6)
    ax.set_ylabel('Latency (ms, log scale)', labelpad=3)
    ax.grid(axis='y', color='#D9D9D9', linewidth=.45)
    ax.set_axisbelow(True)

    block = performance['complete_worker_ig2']
    keys = ['duckdb_proj2', 'arrow_proj', 'vortex_proj', 'stream']
    colors = {key: color for key, label, color in styles}
    values = [block['median_ms'][key] for key in keys]
    bars = worker.bar(np.arange(4), values, width=.58,
                      color=[colors[key] for key in keys], edgecolor='#333333', linewidth=.3, zorder=3)
    for bar, value, key in zip(bars, values, keys):
        worker.text(bar.get_x() + bar.get_width() / 2, value * 1.12,
                    f'{value:.3f}' if key == 'stream' else f'{value:.1f}',
                    ha='center', va='bottom', fontsize=7.5,
                    fontweight='bold' if key == 'stream' else 'normal')
    worker.set_yscale('log')
    worker.set_ylim(1, 600)
    worker.set_yticks([1, 10, 100], ['1', '10', '100'])
    worker.set_xticks(np.arange(4), [
        'DuckDB\n' + f'{block["control_over_fused"]["duckdb_proj2"]:.1f}×',
        'Arrow\n' + f'{block["control_over_fused"]["arrow_proj"]:.1f}×',
        'Vortex\n' + f'{block["control_over_fused"]["vortex_proj"]:.1f}×',
        'Ours\n1×'], fontsize=8)
    worker.grid(axis='y', color='#D9D9D9', linewidth=.45)
    worker.set_axisbelow(True)
    fig.legend(*ax.get_legend_handles_labels(), loc='upper center',
               bbox_to_anchor=(.51, .995), ncol=5, frameon=False,
               handlelength=1.3, handletextpad=.4, columnspacing=1.3)
    fig.text(.36, .035, '(a) Stored input: four complete queries', ha='center', fontsize=9)
    fig.text(.87, .035, '(b) Complete worker: IG/2', ha='center', fontsize=9)
    fig.savefig(PLOTS / 'fig_performance_overview.svg')
    plt.close(fig)

