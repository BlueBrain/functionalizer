from __future__ import print_function
import argparse
import json
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
COLUMNS = "fn circuit cores size density mode version rules cut export runtime".split()


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
        return

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


def extract_data(fns):
    for fn in fns:
        m = extract.search(fn)
        if not m:
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
            version = data.get('version', data.get('spark').get('version'))
            yield fn, circuit, ncores, size, occupancy, (mode or ''), version, rules, cut, export, runtime

            slurm = data.get('slurm')
            if not slurm:
                continue

            start, end = (str(int(float(s))) for s in data['runtime'][-1][1:])
            extract_ganglia(fn, slurm['nodes'], start, end)
        except Exception as e:
            print(fn, e)


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


def save_all(df):
    ax = None
    for (version, size), group in df.groupby(['version', 'size']):
        ax = group.plot(ax=ax, x='cores', y='runtime', style='o', figsize=(6, 4),
                        label="Spark {}, {} cores/exe".format(version, size))
    ax.legend()
    label = get_label(df)
    ax.set_title("Strong scaling: {}".format(label))
    ax.set_xscale('log', basex=2)
    ax.set_xlabel('Number of Cores')
    ax.set_yscale('log', basey=2)
    ax.set_ylabel('Runtime')
    ax.xaxis.set_major_formatter(ScalarFormatter())
    ax.yaxis.set_major_formatter(FuncFormatter(to_time))
    seaborn.despine()
    plt.savefig('strong_scaling_{}_all.png'.format(label.replace(", ", "_")))


def save_density(df):
    ax = None
    for (mode, density), group in df.groupby(['mode', 'density']):
        group = group.sort_values('cores')
        if mode == '':
            label = "{} cores/node".format(density)
        else:
            label = "{} cores/node, {}".format(density, mode)
        ax = group.plot(ax=ax, x='cores', y='runtime', style='o', figsize=(6, 4),
                        label=label)
    ax.legend()
    label = get_label(df)
    ax.set_title("Strong scaling: {}".format(label))
    ax.set_xscale('log', basex=2)
    ax.set_xlabel('Number of Cores')
    # ax.set_yscale('log', basey=2)
    ax.set_ylabel('Runtime')
    ax.xaxis.set_major_formatter(ScalarFormatter())
    ax.yaxis.set_major_formatter(FuncFormatter(to_time))
    seaborn.despine()
    plt.savefig('strong_scaling_{}_density.png'.format(label.replace(", ", "_")))


def save_fractions(df):
    label = get_label(df)
    ax = df.plot.area(x='cores', y=['other', 'rules', 'cut', 'export'], figsize=(6, 4))

    ax.set_title("Strong scaling: runtime fractions for {}".format(label))
    ax.set_xscale('log', basex=2)
    ax.set_xlabel('Number of Cores')
    # ax.set_yscale('log', basey=2)
    ax.set_ylabel('Runtime')
    ax.xaxis.set_major_formatter(ScalarFormatter())
    ax.yaxis.set_major_formatter(FuncFormatter(to_time))

    seaborn.despine()
    plt.savefig('strong_scaling_{}_fractions.png'.format(label.replace(", ", "_")))


def annotate_steps(ax, fn):
    steps = [("filter_by_rules", 1),
             ("run_reduce_and_cut", 1),
             ("apply_reduce", 0),
             ("export_results", 1)]
    for step, level in steps:
        start, end = extract_times(fn, step)
        ymin, ymax = plt.ylim()
        y = ymin + (0.7 + level * 0.1) * (ymax - ymin)
        y2 = ymin + (0.73 + level * 0.1) * (ymax - ymin)
        ax.annotate('',
                    xy=(start, y), xycoords='data',
                    xytext=(end, y), textcoords='data',
                    arrowprops=dict(arrowstyle="<->", linewidth=1))
        ax.text(start + 0.5 * (end - start), y2, step, horizontalalignment='center', fontsize=8)
        # props = dict(boxstyle='darrow')
        # t = ax.text(, step, bbox=props)


def save_timeline(data, cfg, name, stub):
    yscale = cfg.get('yscale', 1.0)

    ax = None
    handels = []
    labels = []
    for col in cfg['columns']:
        # import pdb;pdb.set_trace()
        if yscale != 1.0:
            data[col + '_avg'] /= yscale
            data[col + '_min'] /= yscale
            data[col + '_max'] /= yscale
        label = col.replace('_', ' ')
        ax = data.plot(ax=ax, x=data.index, y=[col + "_avg"], style='o-', figsize=(6, 4),
                       label=label)
        hs, _ = ax.get_legend_handles_labels()
        handels.append(hs[-1])
        labels.append(label)
        ax.fill_between(data.index,
                        data[col + '_min'],
                        data[col + '_max'],
                        alpha=0.3)
    if len(cols) > 1:
        ax.legend(handels, labels)
    else:
        ax.legend([], [])
    ax.set_title(cfg['title'])
    # ax.set_xscale('log', basex=2)
    ax.set_xlabel('Time')
    _, ymax = plt.ylim()
    plt.ylim(0, 1.5 * ymax)
    # ax.set_yscale('log', basey=2)
    ax.set_ylabel("{} / {}".format(cfg['title'], cfg['unit']))
    # ax.xaxis.set_major_formatter(ScalarFormatter())
    # ax.yaxis.set_major_formatter(FuncFormatter(to_time))
    seaborn.despine()
    annotate_steps(ax, stub + ".json")
    plt.savefig('{}_{}.png'.format(stub, name))


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


plot_setup = {
    'cpu': {
        'columns': ['cpu_user'],
        'title': 'CPU usage',
        'unit': '%',
        'yscale': 1,
    },
    'mem': {
        'columns': ['mem_use'],
        'title': 'Memory usage',
        'unit': 'GB',
        'yscale': 1024**3,
    },
    'disk': {
        'columns': ['disk_usage'],
        'title': 'Disk usage',
        'unit': '%',
        'yscale': 1,
    },
    'net': {
        'columns': ['network_out', 'network_in'],
        'title': 'Network usage',
        'unit': 'GB/s',
        'yscale': 1024**3,
    },
}


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--extract-only', default=False, action='store_true',
                        help='extract data, but do not plot')
    parser.add_argument('--no-scaling', default=False, action='store_true',
                        help='do not produce scaling plots')
    parser.add_argument('--no-timeline', default=False, action='store_true',
                        help='do not produce timeline plots')
    parser.add_argument('filename', nargs='+', help='files to process')
    opts = parser.parse_args()

    df = pandas.DataFrame(columns=COLUMNS, data=extract_data(opts.filename))

    if opts.extract_only:
        return

    if not opts.no_timeline:
        for fn in opts.filename:
            pfn = fn.replace(".json", ".pkl")
            if not os.path.exists(pfn):
                continue
            print('processing {}'.format(pfn))
            data = pandas.read_pickle(pfn)
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
            stub = fn.replace(".json", "")
            for name, cfg in plot_setup.items():
                save_timeline(data.groupby('timestamp').first(), cfg, name, stub)

    if opts.no_scaling:
        return

    print("circuits available: {}".format(", ".join(df.circuit.unique())))

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

    # data = df[(
    #
    # for circuit in df.circuit.unique():
    #     subset = df[df.circuit == circuit]
    #     save_all(subset)
    #     for mode in subset['mode'].unique():
    #         save_density(subset[(subset.version == '2.2.1') & (subset['mode'] == mode)])
    #
    # df['other'] = df.runtime - df.rules - df.cut - df.export
    # means = df[df['mode'].isin(['', 'nvme'])].groupby(['circuit', 'cores'], as_index=False).mean()
    # for circuit in means.circuit.unique():
    #     subset = means[means.circuit == circuit]
    #     save_fractions(subset)


if __name__ == '__main__':
    run()
