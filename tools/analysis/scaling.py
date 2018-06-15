
"""Plot resource performance
"""
from __future__ import print_function
import abc
import logging
import six

import matplotlib.pyplot as plt
import matplotlib.lines as lines
from matplotlib.ticker import ScalarFormatter
import seaborn

L = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class ScalingPlot(object):
    """Abstract base class to produce scaling plots.

    :param data: a Pandas DataFrame with plotting data.
    """
    def __init__(self, data):
        self.__data = data

    @abc.abstractmethod
    def format_axes(self, ax):
        """Apply specialization to plotting axes.

        :param ax: the matplotlib Axes object to decorate/refine.
        """
        pass

    def __handle_split(self, split_by):
        """Process column splitting:
        """
        if not split_by:
            return [(None, self.__data)], split_by
        split_by = [c for c in split_by if len(self.__data[c].unique()) > 1]
        if len(split_by) > 0:
            return self.__data.groupby(split_by), split_by
        return [(None, self.__data)], None

    def save(self, value, fn, split_by=None, scatter=False, xcol='cores'):
        """Save the scaling plot to a file.

        Multiple y-values per x-axis entry will be grouped, and a band
        between min and max displayed, overlayed by a line showing the
        average.

        :param value: what to plot on the y-axis
        :param fn: the filename to write to
        :param split_by: plot some columns differently
        :type split_by: None or list[Str]
        :param scatter: also display datapoints as scatter
        :param xcol: the column to use for the x-axis
        """
        data, split_by = self.__handle_split(split_by)
        fig, ax = plt.subplots(figsize=(6, 4), constrained_layout=True)
        handels = []
        labels = []
        ymin = None

        for index, super_group in data:
            if index is None or isinstance(index, six.string_types):
                label = index
            else:
                label = ", ".join("{}: {}".format(k, v or 'default') for k, v in zip(split_by, index))

            group = super_group.groupby(xcol).aggregate(['mean', 'min', 'max'])
            group.columns = ['_'.join([c, f.replace('mean', 'avg')]) for (c, f) in group.columns]
            local_min = group[value + '_min'].min()

            ymin = min(ymin, local_min) if ymin else local_min
            # print(names, group[xcol])
            ax.legend([], [])
            group.plot(ax=ax, y=value + "_avg", style='o-', label=label)
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
        self.format_axes(ax)
        _, ymax = plt.ylim()
        plt.ylim(0.8 * ymin, 1.2 * ymax)
        seaborn.set_style(rc={'ytick.minor.size': 3.0, 'ytick.direction': 'in', 'ytick.color': '.5'})
        seaborn.despine()
        # plt.subplots_adjust(bottom=0.13)
        fig.savefig(fn)


class WeakScalingPlot(ScalingPlot):
    """Weak scaling plot

    :param data: a Pandas DataFrame with plotting data.
    :param labels: circuit labels for the x-axis
    :param sizes: circuit sizes to be used on the x-axis
    """
    def __init__(self, data, labels, sizes):
        super(WeakScalingPlot, self).__init__(data)
        self.__circuit_labels = labels
        self.__circuit_sizes = sizes
        self.__title = ''
        self.__ylabel = ''

    def format_axes(self, ax):
        """Apply specialization to plotting axes.

        :param ax: the matplotlib Axes object to decorate/refine.
        """
        ax.set_title(self.__title)
        ax.set_xscale('log', basex=10)
        ax.set_yscale('log', basex=10)
        ax.set_xlabel('Touches (in billions)')
        ax.set_ylabel(self.__ylabel)
        ax.xaxis.set_ticks(self.__circuit_sizes)
        ax.xaxis.set_ticklabels(self.__circuit_labels)

    def save(self, column, title, ylabel, filename):
        """Save plot to a file

        :param column: data to display on the y-axis
        :param title: title of the plot
        :param ylabel: label of the y-axis
        :param filename: file to save to
        """
        self.__title = title
        self.__ylabel = ylabel
        super(WeakScalingPlot, self).save(column, filename,
                                          scatter=True,
                                          split_by=['mode'], xcol='circuit')


class StrongScalingPlot(ScalingPlot):
    """Weak scaling plot

    :param data: a Pandas DataFrame with plotting data.
    """
    def __init__(self, data):
        super(StrongScalingPlot, self).__init__(data)
        self.__title = ''

    def format_axes(self, ax):
        """Apply specialization to plotting axes.

        :param ax: the matplotlib Axes object to decorate/refine.
        """
        ax.set_title(self.__title)
        ax.set_xlabel('Cores')
        ax.set_ylabel('Minutes')
        ax.set_xscale('log', basex=2)
        ax.set_yscale('log', basey=2)
        ax.xaxis.set_major_formatter(ScalarFormatter())
        ax.yaxis.set_major_formatter(ScalarFormatter())

    def save(self, column, split_by, title, filename):
        """Save plot to a file

        :param column: data to display on the y-axis
        :param title: title of the plot
        :param ylabel: label of the y-axis
        :param filename: file to save to
        """
        self.__title = title
        if split_by and isinstance(split_by, six.string_types):
            split_by = split_by.split(',')
        super(StrongScalingPlot, self).save(column, filename,
                                            split_by=split_by)


def save_strong(df, opts):
    """Small wrapper around StrongScalingPlot
    """
    if opts.filter:
        for (k, v) in (s.split('=') for s in opts.filter.split(',')):
            df = df[df[k] == v]
    if df.size == 0:
        L.error('no data left to plot after filtering!')
        return
    plot = StrongScalingPlot(df)
    plot.save(opts.column,
              opts.split_by,
              opts.title,
              "strong_{o.column}.{o.filetype}".format(o=opts))


def save_weak(df, opts):
    """Small wrapper around WeakScalingPlot
    """
    names = [c for c in opts.circuit_order.split(',') if c in df.circuit.unique()]
    sizes = [float(n) for n in opts.circuit_sizes.split(',')][:len(names)]
    labels = ['{}\n{:.1f}'.format(n, s) for n, s in zip(names, sizes)]

    def index(c):
        return sizes[names.index(c)]

    df["name"] = df.circuit
    df["circuit"] = df.circuit.apply(index)

    plot = WeakScalingPlot(df, labels, sizes)
    plot.save(opts.column, opts.title, opts.ylabel, 'weak_{o.column}.{o.filetype}'.format(o=opts))
