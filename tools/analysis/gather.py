"""Extract data from SLURM and graphite to measure job performance.
"""
from __future__ import print_function
from collections import OrderedDict
import datetime
import json
import logging
import pandas
import os
import re
import requests
from six import StringIO
import subprocess

rack = re.compile(r'r(\d+)')
extract = re.compile(r'(mixed|nvme|hdfs)?/([^/]+)/(\d+)cores_(\d+)nodes_(\d+)execs')
COLUMNS = (
    "fn jobid circuit cores size threads mode version rules cut export runtime start walltime mem disk success"
).split()

SCALE_DISK = 0.02  # 2 TB per node, devided by 100%
SCALE_MEMORY = 1. / 1024**3  # GB for memory consumption
SCALE_RUNTIME = 1. / 60**2  # Runtime in hours

GANGLIA_SCALE_CPU = 72 / 100.

L = logging.getLogger(__name__)


def maybe(o, idx=0):
    """Return an object or an item of a list

    :param o: either an object or a list
    :param idx: index to return if `o` is a list
    """
    if isinstance(o, list):
        return o[idx]
    elif idx == 0:
        return o


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
        if collapse and 'cpu' in data['target']:
            L.info("sum %s for %s: %f", columns[0], data['target'], df[col].sum())
    if collapse:
        data = pandas.concat(datas).groupby('timestamp').aggregate(['mean', 'min', 'max'])
        data.columns = ['_'.join([c, f.replace('mean', 'avg')]) for (c, f) in data.columns]
        return data
    data = datas[0]
    for c, d in zip(columns[1:], datas[1:]):
        if not all(data.timestamp == d.timestamp):
            L.error('unreliable timestamp data!')
        data[c] = d[c]
    return data.groupby('timestamp').first()


def extract_graphite(hosts, start, end):
    """Extract data from graphite (cpu, memory, network usage)

    :param hosts: a list of hostnames
    :param start: the starting timestamp
    :param end: the end timestamp
    """
    fcts = {
        'avg': 'target=averageSeries(bb5.ps.{}_bbp_epfl_ch.{})',
        'min': 'target=minSeries(bb5.ps.{}_bbp_epfl_ch.{})',
        'max': 'target=maxSeries(bb5.ps.{}_bbp_epfl_ch.{})'
    }

    mem_metric = 'memory.memory.used'
    disk_metric = 'df.nvme.df_complex.used'

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
    mem_query = get.format('&'.join(f.format(hq, mem_metric) for f in fcts.values()), start, end)
    mem_data = process_response(requests.get(mem_query), ['mem_' + f for f in fcts.keys()])

    disk_query = get.format('&'.join(f.format(hq, disk_metric) for f in fcts.values()), start, end)
    disk_data = process_response(requests.get(disk_query), ['disk_' + f for f in fcts.keys()])

    data = cpu_data.join(in_data).join(out_data).join(mem_data).join(disk_data)
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


def expand_hosts(nodes):
    """Returns an expanded nodelist via slurm.

    :param nodes: a nodelist in SLURM format
    """
    return subprocess.check_output('scontrol show hostname {}'.format(nodes).split()).split()


def get_slurm_data(jobid):
    """Returns the nodelist and requested cpus for a slurm job id.

    :param jobid: a valid SLURM job id
    """
    def conv(t):
        return datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    output = subprocess.check_output(
        'sacct --noheader -P -o node,ntasks,ncpus,start,end,jobname,state -j {}'.format(jobid).split())
    ntasks = max(line.split('|')[1] for line in output.splitlines())
    statii = OrderedDict([l.split('|')[-2:] for l in output.splitlines()])
    done = statii.get('sm_cluster', statii.values()[0]) == 'COMPLETED'
    nodes, _, cpus, start, end, _, _ = output.splitlines()[-1].split('|')
    return nodes, int(ntasks), int(cpus), conv(start), conv(end), done


def extract_node_data(nodes, start, end):
    """Extract performance data from nodes either via graphite.

    :param nodes: a list of node hostnames
    :param start: when to extract from
    :param end: time to extract up to
    """
    hosts = expand_hosts(nodes)
    data = extract_graphite(hosts, start, end)
    data['cpu_avg'] *= GANGLIA_SCALE_CPU
    data['cpu_min'] *= GANGLIA_SCALE_CPU
    data['cpu_max'] *= GANGLIA_SCALE_CPU
    L.info("data gathered for %s", ", ".join(str(c) for c in data.columns))
    return data


def extract_data_from_json(fn):
    """Extract data from a JSON file that spykfunc writes along with the output

    :param fn: json output from spykfunc
    """
    with open(fn, 'r') as fd:
        data = json.load(fd)

    # assert(len(data['timing']) == 1)
    slurm = data.get('slurm', dict())

    try:
        nodes = len(expand_hosts(slurm['nodes']))
        execs = data['spark']['executors']
        ncores = data['spark']['parallelism']

        circuit = os.path.basename(os.path.dirname(fn))
        mode = None
        if ncores < 10:
            L.warn("small parallelism in %s, falling back to filename matching", fn)
            raise KeyError('ncores')
        m = extract.search(fn)
        if m:
            mode = m.groups()[0]
    except KeyError:
        m = extract.search(fn)
        if not m:
            L.error("no match for pattern in %s", fn)
            return None, None, None, None
        mode, circuit = m.groups()[:2]
        ncores, nodes, execs = (int(n) for n in m.groups()[2:])
    size = ncores // execs
    occupancy = ncores // nodes

    timing = dict(data['timing'][-1])
    rules = maybe(timing['filter_by_rules'])
    cut = maybe(timing['run_reduce_and_cut'])
    export = maybe(timing.get('export_results', [0]))
    runtime = maybe(data.get('runtime', [[None]])[-1])
    done = export is not None and export > 0
    try:
        start, end = (str(int(float(s))) for s in data['runtime'][-1][1:])
    except Exception:
        start = maybe(data.get('runtime', [[None]])[-1], idx=1)
        end = None
    version = 'Spark ' + data.get('version', data.get('spark', dict()).get('version'))
    df = pandas.DataFrame(columns=COLUMNS,
                          data=[[fn, (slurm or dict()).get('jobid'), circuit,
                                 ncores, size, occupancy, (mode or ''),
                                 version, rules, cut, export, runtime, start, -1., -1., -1., done]])
    return df, slurm.get('nodes'), start, end


def extract_data_from_slurm(jobid, circuit, version='C functionalizer', fn=None):
    nodenames, tasks, ncores, start, end, done = get_slurm_data(jobid)
    runtime = int((end - start).total_seconds())

    nodes = len(expand_hosts(nodenames))
    size = ncores // tasks
    occupancy = ncores // nodes
    rules = None
    cut = None
    export = None

    def epic(t):
        epoch = datetime.datetime.fromtimestamp(0)
        return int((t - epoch - datetime.timedelta(hours=1)).total_seconds())

    df = pandas.DataFrame(columns=COLUMNS,
                          data=[[fn, jobid, circuit,
                                 ncores, size, occupancy, '',
                                 version, rules, cut, export, runtime, epic(start), -1., -1., -1., done]])
    return df, nodenames, epic(start), epic(end)


def extract_data(fns, timeline=False):
    """Extract data from log files and monitoring.

    :param fns: a filename or a list of filenames
    :param timeline: if `True`, extract timeline data from monitoring
    """
    if isinstance(fns, basestring):
        fns = [fns]
    for fn in fns:
        L.info("processing %s", fn)
        try:
            jobmatch = re.search(r'[_-](\d+).\w+$', fn)
            if fn.endswith(".json"):
                df, nodes, start, end = extract_data_from_json(fn)
            elif jobmatch:
                jobid = jobmatch.group(1)
                circuit = os.path.basename(os.path.dirname(fn))
                df, nodes, start, end = extract_data_from_slurm(jobid, circuit, fn=fn)
            else:
                L.error("cannot process %s", fn)
                continue

            if df is None:
                continue

            if not timeline:
                yield df, None
                continue

            if nodes is None:
                L.error("no slurm data for %s", fn)
                yield df, None
                continue

            pickle = os.path.splitext(fn)[0] + ".pkl"
            if os.path.exists(pickle):
                timedata = pandas.read_pickle(pickle)
            else:
                timedata = extract_node_data(nodes, start, end)
                timedata.to_pickle(pickle, protocol=-1)
            df['mem'] = df['cores'] / df['threads'] * timedata.mem_avg.max() * SCALE_MEMORY
            df['disk'] = df['cores'] / df['threads'] * timedata.disk_avg.max() * SCALE_DISK
            df['walltime'] = df['cores'] * df['runtime'] * SCALE_RUNTIME
            yield df, timedata
        except Exception as e:
            L.error("could not parse file '%s'", fn)
            L.exception(e)
