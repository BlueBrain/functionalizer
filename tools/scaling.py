# vim: fileencoding=utf8
from __future__ import print_function
import argparse
import json
import logging
import pandas
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, ScalarFormatter
import os
import re
import requests
from StringIO import StringIO
import seaborn
import subprocess

seaborn.set()
seaborn.set_context("paper")

rack = re.compile(r'r(\d+)')
extract = re.compile(r'([^/.]+)(?:\.v\da)?(?:_\d+)?(?:_(mixed|nvme))?/(\d+)cores_(\d+)nodes_(\d+)execs')
COLUMNS = "fn jobid circuit cores size density mode version rules cut export runtime".split()


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


def maybe_first(o):
    if isinstance(o, list):
        return o[0]
    return o


def extract_ganglia(fn, nodes, start, end):
    fn = fn.replace("json", "pkl")
    get = 'http://bbpvadm.epfl.ch/ganglia/graph.php?c=Rack+{}&h={}&m={}{}&csv=1&cs={}&ce={}'
    reports = "cpu mem load network".split()
    if os.path.exists(fn):
        return pandas.read_pickle(fn)

    datas = []

    hosts = subprocess.check_output('scontrol show hostname {}'.format(nodes).split()).split()
    for host in hosts:
        hostdata = None

        rck = rack.match(host).group(1)
        for report in reports:
            r = requests.get(get.format(rck, host, 'load_one', '&g=' + report + '_report', start, end))
            data = pandas.read_csv(StringIO(r.text))
            data.rename(columns=dict(
                (d, d.lower() if d == 'Timestamp' else '{}_{}'.format(
                    report, d.lower().replace('\\g', '').strip())) for d in data.columns
            ), inplace=True)
            if hostdata is None:
                hostdata = data
            else:
                del data['timestamp']
                hostdata = hostdata.join(data)

        r = requests.get(get.format(rck, host, 'part_max_used', '', start, end))
        data = pandas.read_csv(StringIO(r.text))
        data.rename(columns=dict(zip(data.columns, ['timestamp', 'disk_usage'])), inplace=True)
        hostdata['disk_usage'] = data['disk_usage']
        hostdata['host'] = host
        datas.append(hostdata)
    data = pandas.concat(datas)
    data.to_pickle(fn)
    return data


def extract_data(fns, timeline=False):
    if isinstance(fns, basestring):
        fns = [fns]
    for fn in fns:
        m = extract.search(fn)
        if not m:
            L.error("no match for pattern in %s", fn)
            continue
        try:
            circuit, mode = m.groups()[:2]
            ncores, nodes, execs = (int(n) for n in m.groups()[2:])
            size = ncores // execs
            occupancy = ncores // nodes
            with open(fn, 'r') as fd:
                data = json.load(fd)
            assert(len(data['timing'])) == 1
            timing = dict(data['timing'][0])
            rules = maybe_first(timing['filter_by_rules'])
            cut = maybe_first(timing['run_reduce_and_cut'])
            export = maybe_first(timing['export_results'])
            runtime = maybe_first(data.get('runtime', [[None]])[-1])
            version = data.get('version', data.get('spark', dict()).get('version'))
            slurm = data.get('slurm')
            df = pandas.DataFrame(columns=COLUMNS,
                                  data=[[fn, (slurm or dict()).get('jobid'), circuit,
                                         ncores, size, occupancy, (mode or ''),
                                         version, rules, cut, export, runtime]])

            if not timeline:
                yield df
            if not slurm:
                L.error("no slurm data for %s", fn)
                continue

            start, end = (str(int(float(s))) for s in data['runtime'][-1][1:])
            timedf = extract_ganglia(fn, slurm['nodes'], start, end)
            yield df, timedf
        except Exception as e:
            L.error("could not parse file '%s'", fn)
            L.exception(e)


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
    cores_node = info.density[0]
    nodes = cores // cores_node
    return "timeline_{}_{}nodes_{}cores_{}.png".format(circuit, nodes, cores, jobid)


def annotate_plot(fig, info):
    circuit = info.circuit[0]
    jobid = info.jobid[0]
    cores = info.cores[0]
    cores_node = info.density[0]
    nodes = cores // cores_node

    t = fig.suptitle(u"Circuit {}: SLURM id {}\n"
                     u"{} nodes, {} cores → {} cores/node".format(circuit, jobid, nodes, cores, cores_node),
                     color='white')
    t.set_bbox(dict(facecolor='gray', boxstyle='round', pad=0.6))


def annotate_steps(ax, fn):
    steps = [("filter_by_rules", 1),
             ("run_reduce_and_cut", 1),
             ("apply_reduce", 0),
             ("export_results", 1)]
    for step, level in steps:
        start, end = extract_times(fn, step)
        ymin, ymax = ax.get_ylim()
        y = ymin + (0.7 + level * 0.1) * (ymax - ymin)
        y2 = ymin + (0.73 + level * 0.1) * (ymax - ymin)
        y3 = (ymin + (0.71 + level * 0.1) * (ymax - ymin)) / ymax
        ax.annotate('',
                    xy=(start, y), xycoords='data',
                    xytext=(end, y), textcoords='data',
                    arrowprops=dict(arrowstyle="<->", linewidth=1, color='gray'))
        ax.text(start + 0.5 * (end - start), y2, step, horizontalalignment='center', fontsize=8)
        ax.axvline(start, ymax=y3, color='gray')
        ax.axvline(end, ymax=y3, color='gray')
        # props = dict(boxstyle='darrow')
        # t = ax.text(, step, bbox=props)


plot_setup = [
    {
        'columns': ['cpu_user'],
        'title': 'CPU usage',
        'unit': '%',
        'yscale': 1,
    },
    {
        'columns': ['mem_use'],
        'title': 'Memory usage',
        'unit': 'GB',
        'yscale': 1024**3,
    },
    {
        'columns': ['disk_usage'],
        'title': 'Disk usage',
        'unit': '%',
        'yscale': 1,
    },
    {
        'columns': ['network_out', 'network_in'],
        'title': 'Network usage',
        'unit': 'GB/s',
        'yscale': 1024**3,
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
        ax = data.plot(ax=ax, x=data.index, y=[col + "_avg"], style='o-',
                       label=label)
        hs, _ = ax.get_legend_handles_labels()
        handels.append(hs[-1])
        labels.append(label)
        ax.fill_between(data.index,
                        data[col + '_min'],
                        data[col + '_max'],
                        alpha=0.3)
    if len(cfg['columns']) > 1:
        ax.legend(handels, labels)
    else:
        ax.legend([], [])
    _, ymax = ax.get_ylim()
    ax.set_ylim(0, 1.5 * ymax)
    ax.set_ylabel("{} / {}".format(cfg['title'], cfg['unit']))


def save_timelines(fn):
    for info, data in extract_data(fn, timeline=True):
        break
    else:
        L.error("failed to save timeline for %s", fn)
        return
    L.info("saving timeline for %s", fn)
    data['timestamp'] = pandas.to_datetime(data['timestamp'])
    for time, group in data.groupby('timestamp'):
        cols = set()
        for col, val in group.mean().items():
            cols.add(col)
            data.loc[data.timestamp == time, col + '_avg'] = val
        for col, val in group.min().items():
            if col in cols:
                data.loc[data.timestamp == time, col + '_min'] = val
        for col, val in group.max().items():
            if col in cols:
                data.loc[data.timestamp == time, col + '_max'] = val
    data = data.groupby('timestamp').first()
    fig, axs = plt.subplots(len(plot_setup), sharex=True, figsize=(6, 12), constrained_layout=True)
    for ax, cfg in zip(axs, plot_setup):
        save_timeline(data, cfg, ax)
        annotate_steps(ax, fn)
    annotate_plot(fig, info)
    axs[-1].set_xlabel('Time')
    seaborn.despine()
    plt.savefig(generate_timeline_filename(info))
    plt.close()


def save(df, value, cols, fn, mean=False, title='', legend=True):
    ax = None
    handels = []
    labels = []
    for names, group in df.groupby(cols):
        if not isinstance(names, list) and not isinstance(names, tuple):
            names = [names]
        if mean:
            for n, g in group.groupby("cores"):
                group.loc[group.cores == n, value + "_std"] = g[value].std()
                group.loc[group.cores == n, value + "_avg"] = g[value].mean()
                group.loc[group.cores == n, value + "_min"] = g[value].min()
                group.loc[group.cores == n, value + "_max"] = g[value].max()
        group = group.sort_values('cores')
        label = ", ".join("{} = {}".format(k, v) for k, v in zip(cols, names))
        # if mode == '':
        #     label = "{} cores/node".format(density)
        # else:
        #     label = "{} cores/node, {}".format(density, mode)
        # ax = group.plot(ax=ax, x='cores', y=value, style='o', figsize=(6, 4),
        #                 label=label)
        # ax.plot(x=group.cores, y=group[value + '_avg'])
        # ax.fill_between(group.cores,
        #                 group[value + '_avg'] - group[value + '_std'],
        #                 group[value + '_avg'] + group[value + '_std'],
        #                 alpha=0.5)
        ax = group.plot(ax=ax, x='cores', y=[value + "_avg"], style='o-', figsize=(6, 4),
                        label=label)
        hs, _ = ax.get_legend_handles_labels()
        handels.append(hs[-1])
        labels.append(label)
        ax.fill_between(group.cores,
                        group[value + '_min'],
                        group[value + '_max'],
                        alpha=0.3)
    if legend:
        ax.legend(handels, labels)
    else:
        ax.legend([], [])
    ax.set_title(("Strong scaling: {}".format(title)).strip(': '))
    ax.set_xscale('log', basex=2)
    ax.set_xlabel('Number of Cores')
    _, ymax = plt.ylim()
    plt.ylim(0, 1.2 * ymax)
    # ax.set_yscale('log', basey=2)
    ax.set_ylabel('Runtime')
    ax.xaxis.set_major_formatter(ScalarFormatter())
    ax.yaxis.set_major_formatter(FuncFormatter(to_time))
    seaborn.despine()
    plt.savefig(fn)
    plt.close()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--extract-only', default=False, action='store_true',
                        help='extract timeline data, but do not plot')
    parser.add_argument('--no-scaling', default=False, action='store_true',
                        help='do not produce scaling plots')
    parser.add_argument('--no-timeline', default=False, action='store_true',
                        help='do not produce timeline plots')
    parser.add_argument('filename', nargs='+', help='files to process')
    opts = parser.parse_args()

    if opts.extract_only:
        return

    if not opts.no_timeline:
        for fn in opts.filename:
            save_timelines(fn)

    if opts.no_scaling:
        return

    df = pandas.concat(extract_data(opts.filename))

    L.info("circuits available: %s", ", ".join(df.circuit.unique()))

# O1: Spark version 2.2.1 vs 2.3.0
    data = df[(df.circuit == "O1") & ((df.version == '2.3.0') | ((df.version == '2.2.1') & (df['mode'] == "mixed")))]
    if data.size > 0:
        save(data, "runtime", ["version"], "strong_scaling_O1_spark_version.png", mean=True, title='O1')

    data = df[(df.circuit == "O1") & (df.version == '2.2.1')]
    if data.size > 0:
        save(data, "runtime", ["mode"], "strong_scaling_O1_gpfs_vs_nvme.png", mean=True, title='O1')

    for circ in "10x10 O1 S1".split():
        data = df[(df.circuit == circ) & (df.version == '2.2.1') & (df['mode'].isin(['nvme', '']))]
        save(data, "runtime", ["density"], "strong_scaling_{}_density.png".format(circ), mean=True,
             title='{}, cores used per node'.format(circ))

        data = df[(df.circuit == circ) & (df.version == '2.2.1') & (df['mode'].isin(['nvme', '']))]
        save(data, "runtime", ["mode"], "strong_scaling_{}_runtime.png".format(circ), mean=True,
             title='{}, total runtime'.format(circ), legend=False)

        data = df[(df.circuit == circ) & (df.version == '2.2.1') & (df['mode'].isin(['nvme', '']))]
        for step in "rules cut export".split():
            save(data, step, ["mode"], "strong_scaling_{}_step_{}.png".format(circ, step), mean=True,
                 title='{}, runtime for step {}'.format(circ, step), legend=False)


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s line %(lineno)d: %(message)s', style='{')
    L.setLevel(logging.INFO)
    run()
