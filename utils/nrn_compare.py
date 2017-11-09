"""
nrn_compare: takes two outputs of functionalizer and analyzes their similarity

Usage:
  nrn_compare summary <file1> <file2>
  nrn_compare -h

Options:
  -h                Show help
  -vv               Verbose mode (-v for info, -vv for debug)
"""

from __future__ import print_function
import h5py
import numpy
from docopt import docopt
import logging
logging.basicConfig(level=logging.INFO)
from pprint import pformat


def check_summary(summary1, summary2):
    f1 = h5py.File(summary1, mode="r")
    f2 = h5py.File(summary2, mode="r")

    f1_keyset = set(f1.keys())
    f2_keyset = set(f2.keys())

    diff1 = f1_keyset - f2_keyset
    if diff1: logging.error("File {} datasets not in {}: {}".format(summary1, summary2, diff1))
    diff2 = f2_keyset - f1_keyset
    if diff2: logging.error("File {} datasets not in {}: {}".format(summary2, summary1, diff2))

    ds_count = len(f1_keyset)
    progress_each = ds_count // min(100, round(ds_count / 500.0, 0))
    probs = []

    for i, ds_name in enumerate(f1_keyset - {"info"}):
        logging.debug("Dataset: %s", ds_name)
        ds1 = f1[ds_name][:]
        ds2 = f2[ds_name][:]
        if not numpy.array_equal(ds1, ds2):
            prob_index = numpy.nonzero(ds1-ds2)[0]
            probs.extend(zip([ds_name]*len(prob_index),
                             prob_index.tolist(),
                             ds1[prob_index].tolist(),
                             ds2[prob_index].tolist()))
        if i % progress_each == 0:
            logging.info("Progress: %4d /%4d", i, ds_count)

    if probs:
        print("Problematic datasets:\n")
        format = "| %10s | %10s | %15s | %15s |"
        print(format % ("Dataset", "Index", "Left val", "Right val"))
        print("|" + '=' * 61 + "|")
        for p in probs:
            print(format % p)
    else:
        print("No differences found!")


if __name__ == "__main__":
    args = docopt(__doc__)
    if args["summary"]:
        check_summary(args["<file1>"], args["<file2>"])
