
"""Plot resource performance
"""
from __future__ import print_function
import datetime
import json
import logging
import pandas
import matplotlib.pyplot as plt
import matplotlib.dates as dates
from matplotlib.ticker import FuncFormatter, ScalarFormatter
import seaborn

seaborn.set()
seaborn.set_context("paper")

L = logging.getLogger(__name__)


def to_time(x, pos=None):
    """Convert seconds to time string
    >>> to_time(3600)
    '1:00:00'
    >>> to_time(301)
    '5:01'
    >>> to_time(301.0)
    '5:01'
    """
    res = []
    while x > 0:
        res.append(x % 60)
        x //= 60
    return ":".join("{:02}".format(int(i)) for i in reversed(res)).lstrip("0")


def extract_times(fn, step):
    with open(fn, 'r') as fd:
        data = json.load(fd)
    return dict(data['timing'][-1])[step][1:]


def get_label(data):
    circuits = data.circuit.unique()
    if len(circuits) == 1:
        label = circuits[0]
    else:
        label = ", ".join(circuits)
    try:
        modes = data['mode'].unique()
        if len(modes) == 1 and modes[0] != '':
            label += ', ' + modes[0]
    except KeyError:
        pass
    return label


def generate_timeline_filename(info):
    circuit = info.circuit[0]
    jobid = info.jobid[0]
    cores = info.cores[0]
    cores_node = info.threads[0]
    nodes = cores // cores_node
    return "timeline_{}_{}nodes_{}cores_{}.png".format(circuit, nodes, cores, jobid).lower().replace(" ", "_")


def annotate_plot(fig, info):
    jobid = info.jobid[0]
    start = info.start[0]
    runtime = info.runtime[0]

    start = datetime.datetime.fromtimestamp(float(start)).strftime("%Y-%m-%d")

    circuit = info.circuit[0]
    version = info.version[0]
    cores = info.cores[0]
    cores_node = info.threads[0]
    nodes = cores // cores_node

    fig.text(0.5, 0.9675, circuit,
             fontsize=20, weight='bold', ha='center', va='center')
    fig.text(0.5, 0.945, version,
             fontsize=12, weight='bold', ha='center', va='center')

    fig.text(0.13, 0.98, str(nodes),
             fontsize=10, ha='right', va='top')
    fig.text(0.13, 0.98, " nodes",
             fontsize=10, ha='left', va='top')
    fig.text(0.13, 0.965, str(cores_node),
             fontsize=10, ha='right', va='top')
    fig.text(0.13, 0.965, " cores / node",
             fontsize=10, ha='left', va='top')
    fig.text(0.13, 0.95, str(cores),
             fontsize=10, ha='right', va='top')
    fig.text(0.13, 0.95, " cores total",
             fontsize=10, ha='left', va='top')
    fig.text(0.87, 0.98, "SLURM: ",
             fontsize=10, ha='right', va='top')
    fig.text(0.87, 0.98, str(jobid),
             fontsize=10, ha='left', va='top')
    fig.text(0.87, 0.965, "date: ",
             fontsize=10, ha='right', va='top')
    fig.text(0.87, 0.965, start,
             fontsize=10, ha='left', va='top')
    fig.text(0.87, 0.95, "runtime: ",
             fontsize=10, ha='right', va='top')
    fig.text(0.87, 0.95, to_time(runtime),
             fontsize=10, ha='left', va='top')

    # t = fig.suptitle(u"Circuit {}: SLURM id {}\n"
    #                  u"{} nodes, {} cores/node".format(circuit, jobid, nodes, cores, cores_node),
    #                  color='white')
    # t.set_bbox(dict(facecolor='gray', boxstyle='round', pad=0.6))


def annotate_steps(ax, fn):
    if not fn.endswith(".json"):
        L.warn('no json data present, skipping annotations')
        return
    steps = [("filter_by_rules", 1),
             ("run_reduce_and_cut", 1),
             ("apply_reduce", 0),
             ("export_results", 1)]
    for step, level in steps:
        try:
            conv = datetime.datetime.utcfromtimestamp
            start, end = extract_times(fn, step)
            ymin, ymax = ax.get_ylim()
            y = ymin + (0.7 + level * 0.1) * (ymax - ymin)
            y2 = ymin + (0.73 + level * 0.1) * (ymax - ymin)
            y3 = (ymin + (0.71 + level * 0.1) * (ymax - ymin)) / ymax
            ax.annotate('',
                        xy=(conv(start), y), xycoords='data',
                        xytext=(conv(end), y), textcoords='data',
                        arrowprops=dict(arrowstyle="<->", linewidth=1, color='gray'))
            ax.text(conv(start + 0.5 * (end - start)), y2, step, horizontalalignment='center', fontsize=8)
            ax.axvline(conv(start), ymax=y3, color='gray')
            ax.axvline(conv(end), ymax=y3, color='gray')
        except KeyError as e:
            L.error("no data for %s in %s", e, fn)


plot_setup = [
    {
        'columns': ['cpu'],
        'title': 'CPU usage',
        'unit': 'cores',
        'yscale': 1,
        'ylimit': None,
        'ymax': None,
    },
    {
        'columns': ['mem'],
        'title': 'Memory usage',
        'unit': 'GB',
        'yscale': 1024**3,
        'ylimit': None,
        'ymax': None,
    },
    {
        'columns': ['disk'],
        'title': 'Disk usage',
        'unit': '%',
        'yscale': 1,
        'ylimit': 100,
        'ymax': None,
    },
    {
        'columns': ['network_out', 'network_in'],
        'title': 'Network usage',
        'unit': 'GB/s',
        'yscale': 1024**3,
        'ylimit': None,
        'ymax': None,
    },
]


def save_timeline(data, cfg, ax):
    yscale = cfg.get('yscale', 1.0)

    handels = []
    labels = []
    for col in cfg['columns']:
        if yscale != 1.0:
            data[col + '_avg'] /= yscale
            data[col + '_min'] /= yscale
            data[col + '_max'] /= yscale
        label = col.replace('_', ' ')
        L.info("plotting %s", col)
        (handle,) = ax.plot_date(x=data.index.to_pydatetime(), y=data[col + "_avg"], fmt='o-')
        handels.append(handle)
        labels.append(label)
        ax.fill_between(data.index,
                        data[col + '_min'],
                        data[col + '_max'],
                        alpha=0.3)
    if len(cfg['columns']) > 1:
        ax.legend(handels, labels)
    _, ymax = ax.get_ylim()
    if cfg.get('ymax'):
        ymax = cfg.get('ymax')
    ax.set_ylim(0, 1.5 * ymax)
    _, ymax = ax.get_ylim()
    ax.set_ylabel("{} / {}".format(cfg['title'], cfg['unit']))
    ylimit = cfg.get('ylimit')
    if ylimit and ylimit < ymax:
        ax.axhline(y=ylimit, color='r', alpha=0.2, linewidth=4)


def save_timelines(to_process, opts):
    for cfg in plot_setup:
        want = [c + '_max' for c in cfg['columns']]
        cfg['ymax'] = max(sum((d[want].max().tolist() for (_, _, d) in to_process if d is not None), []))
        cfg['ymax'] /= cfg.get('yscale', 1.0)
    for fn, info, data in to_process:
        if opts.title:
            info.circuit = opts.title
        if opts.subtitle:
            info.version = opts.subtitle
        if data is None:
            continue
        if len(data.index) < opts.min_points:
            L.error("not enough data (>=%d points) for %s", opts.min_points, fn)
            continue
        L.info("saving timeline for %s", fn)
        plot_setup[0]['ylimit'] = info.threads[0]
        try:
            fig, axs = plt.subplots(len(plot_setup), sharex=True, figsize=(7, 12), constrained_layout=True)
            for ax, cfg in zip(axs, plot_setup):
                save_timeline(data, cfg, ax)
                annotate_steps(ax, fn)
            annotate_plot(fig, info)
            axs[-1].xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))
            axs[-1].set_xlabel('Time')
            fig.subplots_adjust(right=0.95, top=0.92, bottom=0.05, hspace=0.05)
            seaborn.despine()
            plt.savefig(generate_timeline_filename(info))
            plt.close()
        except ValueError as e:
            L.exception(e)


def save(df, value, cols, fn, mean=False, title='', legend=True, xcol='cores', xlabel=None, xticks=None):
    if df.size == 0:
        L.error("nothing to plot!")
        return
    ax = None
    handels = []
    labels = []
    ymin = None
    for names, group in df.groupby(cols):
        if not isinstance(names, list) and not isinstance(names, tuple):
            names = [names]
        if mean:
            group = group.groupby(xcol).aggregate(['mean', 'min', 'max'])
            group.columns = ['_'.join([c, f.replace('mean', 'avg')]) for (c, f) in group.columns]
            local_min = group[value + '_min'].min()
        else:
            local_min = group[value].min()
        ymin = min(ymin, local_min) if ymin else local_min
        # print(names, group[xcol])
        label = ", ".join("{} = {}".format(k, v) for k, v in zip(cols, names))
        ax = group.plot(ax=ax, x=group.index, y=[value + "_avg"], style='o-', figsize=(6, 4),
                        label=label)
        hs, _ = ax.get_legend_handles_labels()
        handels.append(hs[-1])
        labels.append(label)
        ax.fill_between(group.index,
                        group[value + '_min'],
                        group[value + '_max'],
                        alpha=0.3)
    if legend:
        ax.legend(handels, labels)
    else:
        ax.legend([], [])
    ax.set_title(title)
    ax.set_xscale('log', basex=2)
    if xcol == 'cores':
        ax.set_xlabel('Number of Cores')
        ax.xaxis.set_major_formatter(ScalarFormatter())
    elif xticks:
        ax.set_xlabel(xlabel)
        ax.xaxis.set_ticks(xticks[0])
        ax.xaxis.set_ticklabels(xticks[1])
    _, ymax = plt.ylim()
    # At least half a minute for the ymin!
    plt.ylim(0.8 * ymin, 1.2 * ymax)
    ax.set_yscale('log', basey=2)
    ax.set_ylabel('Runtime')
    ax.yaxis.set_major_formatter(FuncFormatter(to_time))
    seaborn.despine()
    plt.subplots_adjust(bottom=0.13)
    plt.savefig(fn)
    plt.close()


def save_strong(df):
    for circ in df.circuit.unique():
        data = df[(df.circuit == circ)]
        if data.size == 0:
            continue

        if len(data.version.unique()) > 1:
            L.info("saving strong scaling depending on Spark version")
            save(data, "runtime", ["version"], "strong_scaling_spark_version_{}.png".format(circ).lower(),
                 mean=True, title='Strong Scaling: {}'.format(circ))

            for step in "rules cut export".split():
                L.info("saving runtime for step %s of %s", step, circ)
                save(data, step, ["version"],
                     "strong_scaling_spark_version_{}_step_{}.png".format(circ, step).lower(), mean=True,
                     title='Strong Scaling: {}, runtime for step {}'.format(circ, step), legend=False)

        L.info("saving threads for %s", circ)
        save(data, "runtime", ["threads"], "strong_scaling_{}_threads.png".format(circ).lower(), mean=True,
             title='Strong Scaling: {}, cores used per node'.format(circ))

        L.info("saving runtime for %s", circ)
        save(data, "runtime", ["mode"], "strong_scaling_{}.png".format(circ).lower(), mean=True,
             title='Strong Scaling: {}, total runtime'.format(circ), legend=False)

        for step in "rules cut export".split():
            L.info("saving runtime for step %s of %s", step, circ)
            save(data, step, ["mode"], "strong_scaling_{}_step_{}.png".format(circ, step).lower(), mean=True,
                 title='Strong Scaling: {}, runtime for step {}'.format(circ, step), legend=False)


def save_weak(df, names, sizes, unit, cores=None):
    def index(c):
        return sizes[names.index(c)]
    xticks = (sizes, ['{}\n{:.1f}'.format(n, s) for n, s in zip(names, sizes)])
    xlabel = unit
    df["circuit"] = df.circuit.apply(index)
    if cores:
        df = df[df.cores.isin([int(c) for c in cores.split(',')])]
    data = df[(df.version == 'Spark 2.2.1') & (df['mode'].isin(['nvme', '']))]
    L.info("saving weak scaling")
    save(data, "runtime", ["cores"], "weak_scaling_runtime.png".lower(),
         mean=True, xcol='circuit', xticks=xticks, xlabel=xlabel,
         title='Weak Scaling: total runtime')
    for step in "rules cut export".split():
        L.info("saving weak scaling for step %s", step)
        save(data, step, ["cores"], "weak_scaling_step_{}.png".format(step).lower(),
             mean=True, xcol='circuit', xticks=xticks, xlabel=xlabel,
             title='Weak Scaling: runtime for step {}'.format(step))
