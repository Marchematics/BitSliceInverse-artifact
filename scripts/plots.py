#!/usr/bin/env python3
"""Generate scale and parallelism plots from the frozen measurements."""
import hashlib
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import LogLocator, NullFormatter

HERE=Path(__file__).resolve().parents[1]
PLOTS=HERE/'work/plots'
PLOTS.mkdir(parents=True,exist_ok=True)
REC=HERE/'research/real_decoders_20261025/receipts'
SOURCES={}
BLUE='#397CB6';ORANGE='#D88435';PURPLE='#8570AF';GREEN='#4D9C84'

def read(name):
    raw=(REC/name).read_bytes();SOURCES[name]=hashlib.sha256(raw).hexdigest()
    return json.loads(raw)

def style(ax,log=False):
    for sp in ax.spines.values():
        sp.set_visible(True);sp.set_color('#444444');sp.set_linewidth(.55)
    ax.grid(True,which='major',color='#9C9C9C',linestyle=':',linewidth=.38,alpha=.48)
    ax.tick_params(axis='both',which='major',direction='out',length=2.2,width=.5,pad=2.3)
    ax.tick_params(axis='both',which='minor',bottom=False,left=False)
    ax.set_axisbelow(True)
    if log:
        ax.set_yscale('log');ax.yaxis.set_major_locator(LogLocator(base=10,numticks=4));ax.yaxis.set_minor_formatter(NullFormatter())

def line(ax,x,y,color,marker='o',ls='-',label=None):
    return ax.plot(x,y,color=color,marker=marker,linestyle=ls,linewidth=1.15,
        markersize=3.4,markeredgewidth=.6,markerfacecolor='white',label=label)[0]

def legend(fig,handles,labels,ncol):
    lg=fig.legend(handles,labels,loc='upper center',bbox_to_anchor=(.525,.995),
        ncol=ncol,frameon=True,fancybox=False,edgecolor='#B4B4B4',facecolor='white',
        borderpad=.28,labelspacing=.3,columnspacing=1.2,handlelength=1.8,handletextpad=.45)
    lg.get_frame().set_linewidth(.45)
    return lg

def finish(fig,name,axs,lg):
    """Check layout at the exact print size before exporting vector artwork."""
    fig.canvas.draw();r=fig.canvas.get_renderer();issues=[]
    lb=lg.get_window_extent(r)
    for i,ax in enumerate(axs):
        # No legend occupies a panel or its title/x-label area.
        bb=ax.get_tightbbox(r)
        if lb.overlaps(bb):issues.append(f'legend overlaps panel {i}')
        for j,other in enumerate(axs[i+1:],i+1):
            if bb.overlaps(other.get_tightbbox(r)):issues.append(f'panel bounds overlap: {i}/{j}')
        if bb.x0<0 or bb.y0<0 or bb.x1>fig.bbox.x1 or bb.y1>fig.bbox.y1:
            issues.append(f'panel {i} exceeds canvas')
    assert not issues,issues
    fig.savefig(PLOTS/f'{name}.svg')
    fig.savefig(PLOTS/f'{name}.png',dpi=240)
    bounds=[{'panel':i,'bbox_px':list(map(float,ax.get_tightbbox(r).bounds))} for i,ax in enumerate(axs)]
    plt.close(fig)
    return {'panels':len(axs),'layout_issues':issues,'panel_bounds':bounds}

def build():
    plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','DejaVu Serif'],
        'font.size':7.7,'axes.labelsize':7.7,'axes.titlesize':7.7,'xtick.labelsize':7.1,
        'ytick.labelsize':7.1,'legend.fontsize':7.5,'pdf.fonttype':42,'ps.fonttype':42,
        'axes.titlepad':5,'axes.labelpad':3,'savefig.bbox':None})
    scale=read('scale_20261101/summary.json');tiers=read('tiers_20261101/group_tiers.json')
    mt=read('clustered_20261101/clustered_mt.json')
    layout={}
    # 1 x 4: keep domain/row-count results in a single framed strip.
    fig,axs=plt.subplots(1,4,figsize=(7.05,1.94))
    fig.subplots_adjust(left=.065,right=.992,bottom=.23,top=.755,wspace=.35)
    curve=scale['scalecurve_fixedvg'];ns=[r['n_rows']/1e6 for r in curve]
    assert all(r['groups']==101 and r['values']==21084 and r['cube_bytes']==9087844 for r in curve)
    for ax in axs:style(ax)
    h1=line(axs[0],ns,[r['q_fixed_t1_us'] for r in curve],BLUE,'o')
    axs[0].set(title='(a) Query (µs)',xlabel='Rows (millions)',xticks=ns,yticks=[0,4,8,12],ylim=(0,12),xlim=(45,1055))
    line(axs[1],ns,[r['cube_bytes']/1e6 for r in curve],BLUE,'o')
    axs[1].set(title='(b) Index (MB)',xlabel='Rows (millions)',xticks=ns,yticks=[0,4,8,12],ylim=(0,12),xlim=(45,1055))
    gs=[100000,1000000,10000000]
    stats=[tiers['tiers'][f'G{g}']['stats'] for g in gs[:2]]
    qt=[r['emit']['select_median_ms'] for r in stats]+[scale['materialized_10m']['query_and_serialize'][0]['query_median_ms']]
    sizes=[r['bytes']/1e6 for r in stats]+[scale['materialized_10m']['stats_bytes']/1e6]
    for ax in axs[2:]:
        ax.set_xscale('log');style(ax,True)
        ax.set(xlabel='Groups',xticks=gs,xticklabels=['100K','1M','10M'],xlim=(65000,15500000))
        ax.xaxis.set_minor_formatter(NullFormatter())
    h2=line(axs[2],gs,qt,ORANGE,'s')
    axs[2].set(title='(c) Selection (ms)',ylim=(.08,80))
    line(axs[3],gs,sizes,ORANGE,'s')
    axs[3].set(title='(d) State (MB)',ylim=(1,1000))
    lg=legend(fig,[h1,h2],['Rank cube (G = 101, V = 21,084)','Group totals'],2)
    layout['domain_scaling']=finish(fig,'fig_domain_scaling',list(axs),lg)

    # 2 x 4: each column holds one workload; rows share comparisons and phases.
    fig,axs=plt.subplots(2,4,figsize=(7.05,3.54))
    fig.subplots_adjust(left=.065,right=.992,bottom=.115,top=.825,wspace=.36,hspace=.73)
    cases=[('1B_G101','p10','1B: 11.9%'),('1B_G101','p50','1B: 51.0%'),
           ('100M_G1M','p0.1','100M: 0.135%'),('100M_G1M','p50','100M: 51.0%')]
    handles=None
    for j,(config,case,label) in enumerate(cases):
        d=mt['configs'][config]['cases'][case];ts=[1,4,8,16];phase_ts=[4,8,16]
        assert all(x['bit_identical_to_1t'] for x in d['by_threads'].values())
        ours=[d['by_threads'][str(t)]['median_ms'] for t in ts]
        duck=[x*d['x_duck'][str(t)] for t,x in zip(ts,ours)]
        ax=axs[0,j];style(ax,True)
        h1=line(ax,ts,ours,BLUE,'o');h2=line(ax,ts,duck,ORANGE,'s','--')
        ax.set(title=f'({chr(97+j)}) {label}',xlabel='Threads',xticks=ts,xlim=(.4,16.6))
        ax=axs[1,j];style(ax,True)
        h3=line(ax,phase_ts,[d['by_threads'][str(t)]['scan_ms'] for t in phase_ts],PURPLE,'^')
        h4=line(ax,phase_ts,[d['by_threads'][str(t)]['merge_ms'] for t in phase_ts],GREEN,'v','--')
        ax.set(title=f'({chr(101+j)}) Scan and merge',xlabel='Threads',xticks=phase_ts,xlim=(3.3,16.7))
        handles=[h1,h2,h3,h4]
    axs[0,0].set_ylabel('Query (ms)');axs[1,0].set_ylabel('Phase (ms)')
    lg=legend(fig,handles,['BitSliceInverse','DuckDB','Scan','Merge'],4)
    layout['parallel']=finish(fig,'fig_clustered_parallel',list(axs.flat),lg)
    (PLOTS/'layout.json').write_text(json.dumps({
        'files':SOURCES,'curves':'Measured fixed-domain query/index series; group totals bytes/query; matched CLST thread series. DuckDB time reconstructed from same-receipt exact ratios.',
        'phase_note':'Scan and merge fields are available at 4/8/16 threads and are plotted separately, not summed into total time.',
        'style':'Framed 1x4 and 2x4 panels, fixed exterior legends, no inline point labels, publication-size layout checks.',
        'layout_checks':layout},indent=2)+'\n')

if __name__=='__main__':
    build()
    import runpy
    runpy.run_path(str(HERE/'scripts/plots_encoded.py'),run_name='__main__')
