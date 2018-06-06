
"""Plot resource performance
"""
from __future__ import print_function
from collections import namedtuple
from recordtype import recordtype
import abc
import datetime
import json
import logging
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import matplotlib.lines as lines
from matplotlib.ticker import FuncFormatter, ScalarFormatter
import seaborn
import six

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


class AxesSetup(namedtuple("AxesSetup", ["data", "label", "unit", "scale", "limit"])):
    """Specification of data to plot on set of axes.

    :param data: a list of data keys to display
    :param label: the y-axis label
    :param unit: the y-axis unit
    :param scale: the y-axis scale
    :param limit: the upper bound of values on the y-axis
    """
    pass


@six.add_metaclass(abc.ABCMeta)
class BaseResourcePlot(object):
    """Provide an overview of resources used over time.

    :param data:
    """

    SETUP = [
        AxesSetup(['cpu'], 'CPU usage', 'cores', 1, None),
        AxesSetup(['mem'], 'Memory usage', 'GB', 1024**3, None),
        AxesSetup(['network_out', 'network_in'], 'Network usage', 'GB/s', 1024**3, None),
        AxesSetup(['disk'], 'Disk usage', 'TB', 1024**4, 2),
    ]

    def __init__(self, data):
        self.__data = data

    @abc.abstractmethod
    def adjust_axes(self, setup, axes):
        """Alter a single axes object.

        Can be overriden to display custom information.

        :param setup: the :class:`AxesSetup` of `axes`
        :param axes: a matplotlib axes object
        """
        pass

    @abc.abstractmethod
    def annotate_plot(self, fig):
        """Decorate the overall plot.

        :param fig: a matplotlib figure object
        """
        pass

    def save(self, filename):
        """Save the resource plot to a file.

        :param filename: the name of the file
        """
        fig, plots = plt.subplots(len(self.SETUP),
                                  figsize=(7, 12),
                                  sharex=True,
                                  constrained_layout=True)
        fig.subplots_adjust(right=0.95, top=0.95, bottom=0.05, hspace=0.05)
        for axes, setup in zip(plots, self.SETUP):
            self.__timeline(setup, axes)
            self.adjust_axes(setup, axes)
        self.annotate_plot(fig)
        plots[-1].xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))
        plots[-1].set_xlabel('Time')
        seaborn.despine()
        plt.savefig(filename)
        plt.close()

    def __timeline(self, setup, axes):
        """Display a resource usage timeline.

        :param setup: the :class:`AxesSetup` of `axes`
        :param axes: a matplotlib axes object
        """
        handels = []
        labels = []
        for col in setup.data:
            self.__data[col] /= setup.scale
            label = col.replace('_', ' ')
            (handle,) = axes.plot_date(x=self.__data.index.to_pydatetime(),
                                       y=self.__data[col],
                                       fmt='o-')
            handels.append(handle)
            labels.append(label)
            if col + '_min' in self.__data.columns:
                self.__data[col + '_min'] /= setup.scale
                self.__data[col + '_max'] /= setup.scale
                axes.fill_between(self.__data.index,
                                  self.__data[col + '_min'],
                                  self.__data[col + '_max'],
                                  alpha=0.3)
        if setup.limit:
            ymin, ymax = axes.get_ylim()
            axes.axhline(y=setup.limit, color='r', alpha=0.2, linewidth=4)
            axes.set_ylim(ymin, ymax)
        if len(setup.data) > 1:
            axes.legend(handels, labels)
        axes.set_ylabel("{} / {}".format(setup.label, setup.unit))


class ResourcePlot(BaseResourcePlot):
    """Basic implementation
    """

    def adjust_axes(self, setup, axes):
        """Alter a single axes object.

        Can be overriden to display custom information.

        :param setup: the :class:`AxesSetup` of `axes`
        :param axes: a matplotlib axes object
        """
        pass

    def annotate_plot(self, fig):
        """Decorate the overall plot.

        :param fig: a matplotlib figure object
        """
        pass


class SparkAxesSetup(recordtype('SparkAxesSetup', list(AxesSetup._fields) + ['ymax'])):
    pass


class SparkResourcePlot(BaseResourcePlot):
    """Provide annotations for the default plot setup.

    :param data: resource usage data to display
    :param info: supplementary information about the job configuration
    :param filename: file to extract step information from
    """
    SETUP = [
        SparkAxesSetup(*s, ymax=None) for s in BaseResourcePlot.SETUP
    ]

    def __init__(self, data, info, filename):
        super(SparkResourcePlot, self).__init__(data)
        self.__filename = filename
        self.__info = info


    def annotate_plot(self, fig):
        """Display information about the Spark setup.
        """
        fig.subplots_adjust(right=0.95, top=0.92, bottom=0.05, hspace=0.05)

        jobid = self.__info.jobid[0]
        start = self.__info.start[0]
        runtime = self.__info.runtime[0]

        start = datetime.datetime.fromtimestamp(float(start)).strftime("%Y-%m-%d")

        circuit = self.__info.circuit[0]
        version = self.__info.version[0]
        cores = self.__info.cores[0]
        cores_node = self.__info.threads[0]
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

    def adjust_axes(self, setup, ax):
        """Annotate all the plots with information about the steps.
        """
        if not self.__filename.endswith(".json"):
            L.warn('no json data present, skipping annotations')
            return
        steps = [("filter_by_rules", 1),
                 ("run_reduce_and_cut", 1),
                 ("apply_reduce", 0),
                 ("export_results", 1)]
        ymin, ymax = ax.get_ylim()
        ymax *= 1.5  # make room for labels
        ax.set_ylim(ymin, ymax)
        for step, level in steps:
            try:
                conv = datetime.datetime.utcfromtimestamp
                start, end = extract_times(self.__filename, step)
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
                L.error("no data for %s in %s", e, self.__filename)


def save_timelines(to_process, opts):
    for setup in SparkResourcePlot.SETUP:
        want = [c + '_max' for c in setup.data]
        setup.ymax = max(sum((d[want].max().tolist() for (_, _, d) in to_process if d is not None), []))
        setup.ymax /= setup.scale
    for filename, info, data in to_process:
        if data is None:
            continue
        if len(data.index) < opts.min_points:
            L.error("not enough data (>=%d points) for %s", opts.min_points, filename)
            continue
        if opts.title:
            info.circuit = opts.title
        if opts.subtitle:
            info.version = opts.subtitle
        data.columns = [c.replace('_avg', '') for c in data.columns]
        SparkResourcePlot(data, info, filename).save('timeline.png')


weak_labels = {
    "walltime": "Wall time / hours",
    "runtime": "Runtime",
    "cut": "Runtime for cut step",
    "rules": "Runtime for rule step",
    "export": "Runtime for export step",
    "mem": "Peak memory consumption / GB",
    "disk": " Peak disk space consumption / TB"
}


def save(df, value, fn, split_by=None, title='', mean=False, scatter=False,
         xcol='cores', xlabel=None, xticks=None, logbase=2):
    if df.size == 0:
        L.error("nothing to plot!")
        return
    ax = None
    handels = []
    labels = []
    ymin = None

    if split_by:
        split_by = [c for c in split_by if len(df[c].unique()) > 1]
        if len(split_by) > 0:
            data = df.groupby(split_by)
        else:
            split_by = None
            data = [(None, df)]
    else:
        data = [(None, df)]

    for names, super_group in data:
        if not isinstance(names, list) and not isinstance(names, tuple):
            names = [names]
        if mean:
            group = super_group.groupby(xcol).aggregate(['mean', 'min', 'max'])
            group.columns = ['_'.join([c, f.replace('mean', 'avg')]) for (c, f) in group.columns]
            local_min = group[value + '_min'].min()
        else:
            group = super_group
            local_min = group[value].min()
        ymin = min(ymin, local_min) if ymin else local_min
        # print(names, group[xcol])
        label = None
        if split_by:
            label = ", ".join("{}: {}".format(k, v or 'default') for k, v in zip(split_by, names))
        if ax:
            ax.legend([], [])
        ax = group.plot(ax=ax, y=value + "_avg", style='o-', figsize=(6, 4), label=label)
        handle = [h for h in ax.get_legend_handles_labels()[0] if isinstance(h, lines.Line2D)][-1]
        handels.append(handle)
        labels.append(label)
        ax.fill_between(group.index, group[value + '_min'], group[value + '_max'], alpha=0.3)
        if scatter:
            ax.scatter(super_group[xcol], super_group[value], alpha=0.25, color=handle.get_color())
    if split_by:
        ax.legend(handels, labels)
    else:
        ax.legend([], [])
    ax.set_title(title)
    ax.set_xscale('log', basex=logbase)
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
    ax.set_yscale('log', basey=logbase)
    ax.set_ylabel(weak_labels[value])
    if value == 'runtime':
        ax.yaxis.set_major_formatter(FuncFormatter(to_time))
    seaborn.set_style(rc={'ytick.minor.size': 3.0, 'ytick.direction': 'in', 'ytick.color': '.5'})
    seaborn.despine()
    plt.subplots_adjust(bottom=0.13)
    for tick, _, label in ax.xaxis.iter_ticks():
        if 'dev-11M' not in label:
            continue
        tick.set_pad(21.5)
    plt.savefig(fn)
    plt.close()


def save_strong(df):
    for circ in df.circuit.unique():
        data = df[(df.circuit == circ)]
        if data.size == 0:
            continue

        if len(data.version.unique()) > 1:
            L.info("saving strong scaling depending on Spark version")
            save(data, "runtime", "strong_scaling_spark_version_{}.png".format(circ).lower(),
                 split_by=["mode", "version"], mean=True, title='Strong Scaling: {}'.format(circ))

            for step in "rules cut export".split():
                L.info("saving runtime for step %s of %s", step, circ)
                save(data, step,
                     "strong_scaling_spark_version_{}_step_{}.png".format(circ, step).lower(),
                     split_by=["mode", "version"], mean=True,
                     title='Strong Scaling: {}, runtime for step {}'.format(circ, step))

        L.info("saving threads for %s", circ)
        save(data, "runtime", "strong_scaling_{}_threads.png".format(circ).lower(),
             split_by=["threads"], mean=True,
             title='Strong Scaling: {}, cores used per node'.format(circ))

        L.info("saving runtime for %s", circ)
        save(data, "runtime", "strong_scaling_{}.png".format(circ).lower(),
             split_by=["mode", "version"], mean=True,
             title='Strong Scaling: {}, total runtime'.format(circ))

        for step in "rules cut export".split():
            L.info("saving runtime for step %s of %s", step, circ)
            save(data, step, "strong_scaling_{}_step_{}.png".format(circ, step).lower(),
                 split_by=["mode", "version"], mean=True,
                 title='Strong Scaling: {}, runtime for step {}'.format(circ, step))


def save_weak(df, names, sizes, unit, cores=None):
    def index(c):
        return sizes[names.index(c)]
    xticks = (sizes, ['{}\n{:.1f}'.format(n, s) for n, s in zip(names, sizes)])
    xlabel = unit
    df["name"] = df.circuit
    df["circuit"] = df.circuit.apply(index)
    data = df[(df.version == 'Spark 2.2.1') & ~df['mode'].isin(['mixed'])]
    data[data.mem > 0][[
        'name', 'version', 'cores', 'runtime', 'walltime', 'mem', 'disk', 'mode'
    ]].to_csv('weak_scaling_data.csv')
    L.info("saving weak scaling")
    for measure in "walltime mem disk".split():
        d = data[data[measure] > 0]
        save(d, measure, "weak_scaling_{}.png".format(measure).lower(),
             xcol='circuit', xticks=xticks, xlabel=xlabel, title='Weak Scaling',
             split_by=["mode"], mean=True, scatter=True, logbase=10)
    if cores:
        data = df[df.cores.isin([int(c) for c in cores.split(',')])]
    save(data, "runtime", "weak_scaling_runtime.png".lower(),
         split_by=["cores", "mode"], mean=True, xcol='circuit', xticks=xticks, xlabel=xlabel,
         title='Weak Scaling: runtime')
    for step in "rules cut export".split():
        L.info("saving weak scaling for step %s", step)
        save(data, step, "weak_scaling_step_{}.png".format(step).lower(),
             split_by=["cores", "mode"], mean=True, xcol='circuit', xticks=xticks, xlabel=xlabel,
             title='Weak Scaling: runtime for step {}'.format(step))
