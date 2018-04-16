# vim: fileencoding=utf8
"""Plot resource performance
"""
from __future__ import print_function
import argparse
import datetime
import itertools
import json
import logging
import pandas
import matplotlib.pyplot as plt
import matplotlib.dates as dates
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

GANGLIA_SCALE_CPU = 72 / 100.


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


def extract_ganglia(metric, alias, hosts, start, end):
    """Extract data from ganglia

    :param metric: the metric to extract
    :param alias: a list of column names to assign
    :param hosts: a list of hostnames
    :param start: the starting timestamp
    :param end: the end timestamp
    """
    get = 'http://bbpvadm.epfl.ch/ganglia/graph.php?c=Rack+{}&h={}&{}&csv=1&cs={}&ce={}'
    datas = []
    for host in hosts:
        rck = rack.match(host).group(1)
        r = requests.get(get.format(rck, host, metric, start, end))
        data = pandas.read_csv(StringIO(r.text))
        data.rename(columns=dict(zip(data.columns, ['timestamp'] + alias)), inplace=True)
        data['timestamp'] = pandas.to_datetime(data.timestamp, utc=False)
        data['host'] = host
        datas.append(data)
    data = pandas.concat(datas).groupby('timestamp').aggregate(['mean', 'min', 'max'])
    data.columns = ['_'.join([c, f.replace('mean', 'avg')]) for (c, f) in data.columns]
    return data


def process_response(response, columns, collapse=False):
    """Process a response from graphite into a Pandas dataframe

    Data can optionally be collapsed: return mean/min/max per timestamp for
    a single column.

    :param response: a response object
    :param columns: the column names to use
    :param collapse: collapse data by building mean, min, and max columns
    """
    datas = []
    rawdata = json.loads(response.text)
    if collapse:
        columns = columns * len(rawdata)
    for col, data in zip(columns, rawdata):
        df = pandas.DataFrame(columns=[col, 'timestamp'], data=data['datapoints'])
        df['timestamp'] = pandas.to_datetime(df.timestamp, unit='s', utc=False)
        datas.append(df)
    if collapse:
        data = pandas.concat(datas).groupby('timestamp').aggregate(['mean', 'min', 'max'])
        data.columns = ['_'.join([c, f.replace('mean', 'avg')]) for (c, f) in data.columns]
        return data
    data = datas[0]
    for c, d in zip(columns[1:], datas[1:]):
        if not all(data.timestamp == d.timestamp):
            L.error('unreliable timestamp data!')
        data[c] = d[c]
    return data.set_index('timestamp')


def extract_graphite(hosts, start, end):
    """Extract data from graphite (cpu, memory, network usage)

    :param hosts: a list of hostnames
    :param start: the starting timestamp
    :param end: the end timestamp
    """
    fcts = {
        'avg': 'target=averageSeries(bb5.ps.{}_bbp_epfl_ch.memory.memory.used)',
        'min': 'target=minSeries(bb5.ps.{}_bbp_epfl_ch.memory.memory.used)',
        'max': 'target=maxSeries(bb5.ps.{}_bbp_epfl_ch.memory.memory.used)'
    }
    get = 'http://bbpfs43.bbp.epfl.ch/render/?{}&from={}&until={}&format=json'
    start = datetime.datetime.fromtimestamp(float(start)).strftime("%H:%M_%Y%m%d")
    end = datetime.datetime.fromtimestamp(float(end)).strftime("%H:%M_%Y%m%d")

    cpu_metric = 'target=sumSeries(bb5.ps.{}_bbp_epfl_ch.cpu.*.percent.active)'
    cpu_query = get.format('&'.join(cpu_metric.format(h) for h in hosts), start, end)
    cpu_data = process_response(requests.get(cpu_query), ['cpu'], collapse=True)

    in_metric = 'target=perSecond(bb5.ps.{}_bbp_epfl_ch.interface.ib0.if_octets.rx)'
    in_query = get.format('&'.join(in_metric.format(h) for h in hosts), start, end)
    in_data = process_response(requests.get(in_query), ['network_in'], collapse=True)

    out_metric = 'target=perSecond(bb5.ps.{}_bbp_epfl_ch.interface.ib0.if_octets.tx)'
    out_query = get.format('&'.join(out_metric.format(h) for h in hosts), start, end)
    out_data = process_response(requests.get(out_query), ['network_out'], collapse=True)

    hq = '{{{}}}'.format(','.join(hosts))
    mem_query = get.format('&'.join(f.format(hq) for f in fcts.values()), start, end)
    mem_data = process_response(requests.get(mem_query), ['mem_' + f for f in fcts.keys()])

    data = cpu_data.join(in_data).join(out_data).join(mem_data)
    # CPU columns originally in percent per core: convert to overall usage
    # in terms of cores.
    data['cpu_avg'] /= 100
    data['cpu_min'] /= 100
    data['cpu_max'] /= 100

    return data


fallback = [
    ('g=network_report', ['network_in', 'network_out']),
    ('g=cpu_report', ['cpu', 'nice', 'sys', 'wait', 'idle']),
    ('g=mem_report', ['mem', 'share', 'cache', 'buffer', 'total']),
]


def extract_node_data(nodes, start, end):
    hosts = subprocess.check_output('scontrol show hostname {}'.format(nodes).split()).split()

    disk = extract_ganglia('m=part_max_used', ['disk'], hosts, start, end)
    try:
        data = extract_graphite(hosts, start, end)
        disk = disk.append(pandas.DataFrame(index=data.index.copy())) \
                   .interpolate(method='nearest')
        data = data.join(disk.loc[data.index])
        data['cpu_avg'] *= GANGLIA_SCALE_CPU
        data['cpu_min'] *= GANGLIA_SCALE_CPU
        data['cpu_max'] *= GANGLIA_SCALE_CPU
    except ValueError as e:
        L.exception(e)
        L.warn("falling back to ganglia data")
        data = disk
        for m, a in fallback:
            data = data.join(extract_ganglia(m, a, hosts, start, end))
    L.info("data gathered for %s", ", ".join(str(c) for c in data.columns))
    return data


def extract_data(fns, timeline=False):
    if isinstance(fns, basestring):
        fns = [fns]
    for fn in fns:
        m = extract.search(fn)
        if not m:
            L.error("no match for pattern in %s", fn)
            continue
        L.info("processing %s", fn)
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
                continue
            if not slurm:
                L.error("no slurm data for %s", fn)
                yield None, None

            start, end = (str(int(float(s))) for s in data['runtime'][-1][1:])
            pickle = fn.replace(".json", ".pkl")
            if os.path.exists(pickle):
                timedata = pandas.read_pickle(pickle)
            else:
                timedata = extract_node_data(slurm['nodes'], start, end)
                timedata.to_pickle(pickle, protocol=-1)
            yield df, timedata
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

    fig.text(0.5, 0.95, circuit,
             fontsize=20, weight='bold', ha='center', va='center')
    fig.text(0.1, 0.98, str(nodes),
             fontsize=10, ha='right', va='top')
    fig.text(0.1, 0.98, " nodes",
             fontsize=10, ha='left', va='top')
    fig.text(0.1, 0.965, str(cores_node),
             fontsize=10, ha='right', va='top')
    fig.text(0.1, 0.965, " cores / node",
             fontsize=10, ha='left', va='top')
    fig.text(0.1, 0.95, str(cores),
             fontsize=10, ha='right', va='top')
    fig.text(0.1, 0.95, " cores total",
             fontsize=10, ha='left', va='top')
    fig.text(0.9, 0.98, "SLURM: ",
             fontsize=10, ha='right', va='top')
    fig.text(0.9, 0.98, str(jobid),
             fontsize=10, ha='left', va='top')

    # t = fig.suptitle(u"Circuit {}: SLURM id {}\n"
    #                  u"{} nodes, {} cores/node".format(circuit, jobid, nodes, cores, cores_node),
    #                  color='white')
    # t.set_bbox(dict(facecolor='gray', boxstyle='round', pad=0.6))


def annotate_steps(ax, fn):
    steps = [("filter_by_rules", 1),
             ("run_reduce_and_cut", 1),
             ("apply_reduce", 0),
             ("export_results", 1)]
    for step, level in steps:
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
    if cfg.get('ymax'):
        ymax = cfg.get('ymax')
    ax.set_ylim(0, 1.5 * ymax)
    _, ymax = ax.get_ylim()
    ax.set_ylabel("{} / {}".format(cfg['title'], cfg['unit']))
    ylimit = cfg.get('ylimit')
    if ylimit and ylimit < ymax:
        ax.axhline(y=ylimit, color='r', alpha=0.2, linewidth=4)


def save_timelines(fns):
    to_process = [(fn, i, d) for fn in fns for i, d in extract_data(fn, timeline=True) if i is not None]
    for cfg in plot_setup:
        want = [c + '_max' for c in cfg['columns']]
        cfg['ymax'] = max(sum((d[want].max().tolist() for (_, _, d) in to_process), []))
        cfg['ymax'] /= cfg.get('yscale', 1.0)
    for fn, info, data in to_process:
        if len(data.index) < 5:
            L.error("not enough data (>4 points) for %s", fn)
            continue
        L.info("saving timeline for %s", fn)
        plot_setup[0]['ylimit'] = info.density[0]
        try:
            fig, axs = plt.subplots(len(plot_setup), sharex=True, figsize=(7, 12), constrained_layout=True)
            for ax, cfg in zip(axs, plot_setup):
                save_timeline(data, cfg, ax)
                annotate_steps(ax, fn)
            annotate_plot(fig, info)
            fig.subplots_adjust(right=0.95, top=0.92, bottom=0.05, hspace=0.05)
            axs[-1].set_xlabel('Time')
            axs[-1].xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))
            # axs[-1].xaxis.set_minor_locator(dates.MinuteLocator(interval=5))
            # axs[-1].xaxis.set_minor_formatter(dates.DateFormatter(':%M'))
            # axs[-1].xaxis.set_major_locator(dates.HourLocator(interval=1))
            seaborn.despine()
            plt.savefig(generate_timeline_filename(info))
            plt.close()
        except ValueError as e:
            L.exception(e)


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
    parser.add_argument('--timeline', default=False, action='store_true',
                        help='plot timeline data')
    parser.add_argument('--scaling', default=False, action='store_true',
                        help='plot scaling data')
    parser.add_argument('filename', nargs='+', help='files to process')
    opts = parser.parse_args()

    if opts.timeline:
        save_timelines(opts.filename)

    if not opts.scaling:
        return

    df = pandas.concat(extract_data(opts.filename))

    L.info("circuits available: %s", ", ".join(df.circuit.unique()))

    # O1: Spark version 2.2.1 vs 2.3.0
    data = df[(df.circuit == "O1") & ((df.version == '2.3.0') | ((df.version == '2.2.1') & (df['mode'] == "mixed")))]
    if data.size > 0:
        L.info("saving strong scaling depending on Spark version")
        save(data, "runtime", ["version"], "strong_scaling_O1_spark_version.png", mean=True, title='O1')

    data = df[(df.circuit == "O1") & (df.version == '2.2.1')]
    if data.size > 0:
        L.info("saving strong scaling file system")
        save(data, "runtime", ["mode"], "strong_scaling_O1_gpfs_vs_nvme.png", mean=True, title='O1')

    for circ in "10x10 O1 S1".split():
        L.info("saving density for %s", circ)
        data = df[(df.circuit == circ) & (df.version == '2.2.1') & (df['mode'].isin(['nvme', '']))]
        save(data, "runtime", ["density"], "strong_scaling_{}_density.png".format(circ), mean=True,
             title='{}, cores used per node'.format(circ))

        L.info("saving runtime for %s", circ)
        data = df[(df.circuit == circ) & (df.version == '2.2.1') & (df['mode'].isin(['nvme', '']))]
        save(data, "runtime", ["mode"], "strong_scaling_{}_runtime.png".format(circ), mean=True,
             title='{}, total runtime'.format(circ), legend=False)

        data = df[(df.circuit == circ) & (df.version == '2.2.1') & (df['mode'].isin(['nvme', '']))]
        for step in "rules cut export".split():
            L.info("saving runtime for step %s of %s", step, circ)
            save(data, step, ["mode"], "strong_scaling_{}_step_{}.png".format(circ, step), mean=True,
                 title='{}, runtime for step {}'.format(circ, step), legend=False)


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s line %(lineno)d: %(message)s', style='{')
    L.setLevel(logging.INFO)
    run()
