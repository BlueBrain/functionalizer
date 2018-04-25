# vim: fileencoding=utf8
"""Plot resource performance
"""
from __future__ import print_function
import argparse
import logging
import pandas

from .analysis.gather import extract_data
from .analysis.plot import save_timelines, save_strong, save_weak

L = logging.getLogger(__name__)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--timeline', default=False, action='store_true',
                        help='plot timeline data')
    parser.add_argument('--strong', default=False, action='store_true',
                        help='plot strong scaling data')
    parser.add_argument('--weak', default=False, action='store_true',
                        help='plot weak scaling data')
    parser.add_argument('--circuit-order', default='O1,S1,10x10',
                        help='comma separated order of circuits')
    parser.add_argument('filename', nargs='+', help='files to process')
    opts = parser.parse_args()

    to_process = [(fn, i, d) for fn in opts.filename for i, d in extract_data(fn, timeline=True)]
    df = pandas.concat(d for _, d, _ in to_process)

    if opts.timeline:
        save_timelines(to_process)
    if opts.strong:
        L.info("circuits available: %s", ", ".join(df.circuit.unique()))
        save_strong(df)
    if opts.weak:
        L.info("circuits available: %s", ", ".join(df.circuit.unique()))
        save_weak(df, opts.circuit_order.split(','))


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s line %(lineno)d: %(message)s', style='{')
    L.setLevel(logging.INFO)
    run()
