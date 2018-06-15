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
import six
import subprocess

from bb5.slurm.resource import ClusterTargets

rack = re.compile(r'r(\d+)')
extract = re.compile(r'(mixed|nvme|hdfs)?/([^/]+)/(\d+)cores_(\d+)nodes_(\d+)execs')

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


def extract_data_from_json(fn):
    """Extract data from a JSON file that spykfunc writes along with the output

    :param fn: json output from spykfunc
    """
    with open(fn, 'r') as fd:
        data = json.load(fd)

    # assert(len(data['timing']) == 1)
    slurm = data.get('slurm', dict())

    circuit = os.path.basename(os.path.dirname(fn))
    mode = None

    m = extract.search(fn)
    if not m:
        nodes = len(expand_hosts(slurm['nodes']))
        execs = data['spark']['executors']
        ncores = data['spark']['parallelism']
    else:
        mode, circuit = m.groups()[:2]
        ncores, nodes, execs = (int(n) for n in m.groups()[2:])

    size = ncores // execs
    occupancy = ncores // nodes

    runtime = maybe(data.get('runtime', [[None]])[-1])
    try:
        start, end = (int(float(s)) for s in data['runtime'][-1][1:])
    except Exception:
        start = maybe(data.get('runtime', [[None]])[-1], idx=1)
        end = None
    version = 'Spark ' + data.get('version', data.get('spark', dict()).get('version'))
    data = pandas.Series(dict(fn=fn, jobid=(slurm or dict()).get('jobid'), circuit=circuit,
                              cores=ncores, size=size, threads=occupancy, mode=(mode or ''),
                              version=version, start=start, runtime=runtime))
    return data, slurm.get('nodes'), start, end


def extract_data_from_slurm(jobid, circuit, version='C functionalizer', fn=None):
    nodenames, tasks, ncores, start, end, done = get_slurm_data(jobid)
    runtime = int((end - start).total_seconds())

    nodes = len(expand_hosts(nodenames))
    size = ncores // tasks
    occupancy = ncores // nodes

    def epic(t):
        epoch = datetime.datetime.fromtimestamp(0)
        return int((t - epoch - datetime.timedelta(hours=1)).total_seconds())

    data = pandas.Series(dict(fn=fn, jobid=jobid, circuit=circuit,
                              cores=ncores, size=size, threads=occupancy, mode='',
                              version=version, start=epic(start), runtime=runtime))
    return data, nodenames, epic(start), epic(end)


def extract_data(fn, res):
    """Extract data from log files and monitoring.

    :param fn: a filename
    :param timeline: if `True`, extract timeline data from monitoring
    """
    L.info("processing %s", fn)
    jobmatch = re.search(r'[_-](\d+).\w+$', fn)
    if fn.endswith(".json"):
        df, nodes, start, end = extract_data_from_json(fn)
    elif jobmatch:
        jobid = jobmatch.group(1)
        circuit = os.path.basename(os.path.dirname(fn))
        df, nodes, start, end = extract_data_from_slurm(jobid, circuit, fn=fn)
    else:
        L.error("cannot process %s", fn)

    if res:
        with open(res) as fd:
            data = json.load(fd)
        timedata = ClusterTargets.to_pandas(data)
        mask = (timedata.index >= datetime.datetime.utcfromtimestamp(start)) \
            & (timedata.index <= datetime.datetime.utcfromtimestamp(end))
        timedata = timedata.loc[mask]
        return df, timedata

    pickle = os.path.splitext(fn)[0] + ".pkl"
    if os.path.exists(pickle):
        timedata = pandas.read_pickle(pickle)
        df['mem'] = df['cores'] / df['threads'] * timedata.mem_avg.max() * SCALE_MEMORY
        df['disk'] = df['cores'] / df['threads'] * timedata.disk_avg.max() * SCALE_DISK
        df['walltime'] = df['cores'] * df['runtime'] * SCALE_RUNTIME
        timedata.columns = [c.replace('_avg', '') for c in timedata.columns]
        for col in timedata.columns:
            if col.startswith('cpu'):
                timedata[col] *= 100
            if col.startswith('disk'):
                timedata[col] *= 2 * 1024**4 / 100
        return df, timedata
