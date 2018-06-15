# vim: fileencoding=utf8
"""Plot resource performance
"""
from __future__ import print_function
import argparse
import logging

import matplotlib
matplotlib.use('agg')  # noqa
import matplotlib.pyplot as plt
import pandas
import seaborn

from .analysis.gather import extract_data
from .analysis.scaling import save_strong, save_weak
from .analysis.timeline import save_timelines

logging.basicConfig(format='%(levelname)s line %(lineno)d: %(message)s', style='{')
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


def run():
    """Entry point for command line tool.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--context', default="paper",
                        help='seaborn style to use for plots (default: paper)')
    parser.add_argument('--filetype', default="png",
                        help='filetype to save plots (default: png)')
    parser.add_argument('--xkcd', default=False, action='store_true',
                        help='use xkcd drawing stype.')
    subparsers = parser.add_subparsers(dest="command")
    timeline = subparsers.add_parser('timeline')
    timeline.add_argument('--simple', default=False, action='store_true',
                          help='display less information')
    timeline.add_argument('--title', default=None,
                          help='title to use (default: circuit name)')
    timeline.add_argument('--subtitle', default=None,
                          help='subtitle to use (default: version)')
    timeline.add_argument('--min-points', default=10, type=int,
                          help='minimum amount of data points required for plotting (default: 10)')
    timeline.add_argument('--resources', nargs='+', help='resource files to process')
    timeline.add_argument('filename', nargs='+', help='files to process')
    strong = subparsers.add_parser('strong')
    strong.add_argument('--column', default='time2solution',
                        help='the column to plot')
    strong.add_argument('--title', default='Time to Solution',
                        help='the title to display')
    strong.add_argument('--split-by', default='circuit,mode',
                        help='column(s) to split data on (comma separated)')
    strong.add_argument('--filter', default=None,
                        help='filter data as comma separated column=value pairs')
    strong.add_argument('filename', help='file to process')
    weak = subparsers.add_parser('weak')
    weak.add_argument('--column', default='walltime',
                      help='the column to plot')
    weak.add_argument('--title', default='Core-Hours',
                      help='the title to display')
    weak.add_argument('--ylabel', default='Hours',
                      help='the ylabel to display')
    weak.add_argument('--circuit-order', default='O1.v6a,S1.v6a,10x10,dev-11M,4.10x10,10.10x10',
                      help='comma separated order of circuits')
    weak.add_argument('--circuit-sizes', default='4.6,57.7,124.4,439.2,497.7,1244.3',
                      help='comma separated sizes of circuits')
    weak.add_argument('--cores',
                      help='comma separated list of core counts to include')
    weak.add_argument('filename', help='file to process')
    opts = parser.parse_args()

    seaborn.set(context=opts.context)
    if opts.xkcd:
        import tools.analysis.scaling as s
        s.plt.xkcd()
        plt.xkcd()

    if opts.command == 'timeline':
        if not opts.resources:
            opts.resources = [None] * len(opts.filename)
        to_process = [[f] + list(extract_data(f, g))
                      for f, g in zip(opts.filename, opts.resources)]
        save_timelines(to_process, opts)
        return

    df = pandas.read_csv(opts.filename)
    if opts.command == 'strong':
        save_strong(df, opts)
    if opts.command == 'weak':
        save_weak(df, opts)


if __name__ == '__main__':
    run()
