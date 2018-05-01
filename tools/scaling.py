# vim: fileencoding=utf8
"""Plot resource performance
"""
from __future__ import print_function
import argparse
import logging
import pandas

from .analysis.gather import extract_data
from .analysis.plot import save_timelines, save_strong, save_weak

logging.basicConfig(format='%(levelname)s line %(lineno)d: %(message)s', style='{')
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


def run():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    timeline = subparsers.add_parser('timeline')
    timeline.add_argument('--title', default=None,
                          help='title to use (default: circuit name)')
    timeline.add_argument('--subtitle', default=None,
                          help='subtitle to use (default: version)')
    timeline.add_argument('--min-points', default=10, type=int,
                          help='minimum amount of data points required for plotting (default: 10)')
    timeline.add_argument('filename', nargs='+', help='files to process')
    strong = subparsers.add_parser('strong')
    strong.add_argument('filename', nargs='+', help='files to process')
    weak = subparsers.add_parser('weak')
    weak.add_argument('--circuit-order', default='O1.v6a,S1.v6a,10x10,4.10x10,10.10x10',
                      help='comma separated order of circuits')
    weak.add_argument('filename', nargs='+', help='files to process')
    opts = parser.parse_args()

    to_process = [(fn, i, d) for fn in opts.filename for i, d in extract_data(fn, timeline=True)]
    df = pandas.concat(d for _, d, _ in to_process)

    if opts.command == 'timeline':
        save_timelines(to_process, opts)
        return
    L.info("circuits available: %s", ", ".join(df.circuit.unique()))
    if opts.command == 'strong':
        save_strong(df)
    if opts.command == 'weak':
        save_weak(df, opts.circuit_order.split(','))


if __name__ == '__main__':
    run()
