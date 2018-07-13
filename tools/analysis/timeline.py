"""Generate customized resource consumption plots.
"""
import datetime
import json
import logging
import six

from bb5.slurm.visualize import AxesSetup, BaseResourcePlot

L = logging.getLogger(__name__)


def _to_time(x, pos=None):
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


def _extract_times(fn, step):
    """Get timing information for `step` from `fn`.
    """
    with open(fn, 'r') as fd:
        data = json.load(fd)
    return dict(data['timing'][-1])[step][1:]


class SparkAxesSetup(object):
    __slots__ = list(AxesSetup._fields) + ['ymax']

    def __init__(self, *args, **kwargs):
        for s, v in zip(self.__slots__, args):
            setattr(self, s, v)
        for k, v in six.iteritems(kwargs):
            setattr(self, k, v)


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
        self._filename = filename
        self._info = info

    def annotate_plot(self, fig):
        """Display information about the Spark setup.
        """
        fig.suptitle("Circuit: {}, {} cores".format(self._info.circuit[0],
                                                    self._info.cores[0]))

    def adjust_axes(self, setup, ax):
        """Annotate all the plots with information about the steps.
        """
        pass


class FancySparkResourcePlot(SparkResourcePlot):
    def annotate_plot(self, fig):
        """Display information about the Spark setup.
        """
        fig.subplots_adjust(top=0.92)

        jobid = self._info.jobid
        start = self._info.start
        runtime = self._info.runtime

        start = datetime.datetime.fromtimestamp(float(start)).strftime("%Y-%m-%d")

        circuit = self._info.circuit
        version = self._info.version
        cores = self._info.cores
        cores_node = self._info.threads
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
        fig.text(0.87, 0.95, _to_time(runtime),
                 fontsize=10, ha='left', va='top')

    def adjust_axes(self, setup, ax):
        """Annotate all the plots with information about the steps.
        """
        if not self._filename.endswith(".json"):
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
                start, end = _extract_times(self._filename, step)
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
                L.error("no data for %s in %s", e, self._filename)


def _generate_timeline_filename(info):
    """Generate a filename from circuit and execution information.
    """
    nodes = info.cores // info.threads
    return "timeline_{i.circuit}_{n}nodes_{i.cores}cores_{i.jobid}.{{}}" \
           .format(i=info, n=nodes) \
           .lower().replace(" ", "_")


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
        fn = _generate_timeline_filename(info).format(opts.filetype)
        if opts.simple:
            SparkResourcePlot(data, info, filename).save(fn)
        else:
            FancySparkResourcePlot(data, info, filename).save(fn)
