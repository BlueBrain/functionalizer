"""
nrn_compare: takes two outputs of functionalizer and analyzes their similarity

Usage:
  nrn_compare summary <file1> <file2> [-d=<ds1> ...] [-vv]
  nrn_compare -h

Options:
  -h                Show help
  -d=<ds1>          Only compare selected datasets
  -vv               Verbose mode (-v for info, -vv for debug)
"""

from __future__ import print_function
import h5py
import numpy
from docopt import docopt
import logging
logging.basicConfig(level=logging.INFO)


def check_summary(summary1, summary2, dataset_filter=None, verbosity=0):
    f1 = h5py.File(summary1, mode="r")
    f2 = h5py.File(summary2, mode="r")

    f1_keyset = set(f1.keys())
    f2_keyset = set(f2.keys())

    diff1 = f1_keyset - f2_keyset
    if diff1: logging.error("File {} datasets not in {}: {}".format(summary1, summary2, diff1))
    diff2 = f2_keyset - f1_keyset
    if diff2: logging.error("File {} datasets not in {}: {}".format(summary2, summary1, diff2))

    ds_to_verify = dataset_filter or (f1_keyset - {"info"})
    ds_count = len(ds_to_verify)
    progress_each = ds_count // (min(100, round(ds_count / 500.0, 0)) or 1)
    probs = []  # Problem list
    total_records = 0
    wrong_records = 0

    for i, ds_name in enumerate(ds_to_verify):
        logging.debug("Dataset: %s", ds_name)
        ds1 = f1[ds_name][:]
        ds2 = f2[ds_name][:]
        ds1_len = len(ds1)
        total_records += ds1_len

        if not numpy.array_equal(ds1, ds2):
            ds2_len = len(ds2)
            # If different lengths, compare with N/A and trim for next phase
            if ds1_len > ds2_len:
                array_remain = ds1[ds2_len:]
                diff_len = len(array_remain)
                wrong_records += diff_len
                probs.extend(zip([ds_name]*diff_len, range(ds2_len, ds1_len), ["<N/A>"]*diff_len, array_remain.tolist()))
                ds1 = ds2[:ds1_len]
            elif ds2_len > ds1_len:
                array_remain = ds2[ds1_len:]
                diff_len = len(array_remain)
                wrong_records += diff_len
                probs.extend(zip([ds_name]*diff_len, range(ds1_len, ds2_len), array_remain.tolist(), ["<N/A>"] * diff_len))
                ds2 = ds1[:ds2_len]

            prob_index = numpy.nonzero(ds1-ds2)[0]
            diff_len = len(prob_index)
            probs.extend(zip([ds_name]*diff_len,
                             prob_index.tolist(),
                             ds1[prob_index].tolist(),
                             ds2[prob_index].tolist()))
            wrong_records += diff_len

        if i % progress_each == 0:
            logging.info("Progress: %4d /%4d", i, ds_count)

    if wrong_records:
        print("Found {} mismatching records out of {}. ({:.1f}%)".format(wrong_records, total_records, float(wrong_records) * 100 / total_records))

        if verbosity:
            print("Problematic datasets:")
            format = "| %10s | %10s | %15s | %15s |"
            print(format % ("Dataset", "Index", "Left val", "Right val"))
            print("|" + '=' * 61 + "|")
            for p in probs:
                print(format % p)
    else:
        print("No differences found!")
    logging.info("Done comparison.")


if __name__ == "__main__":
    args = docopt(__doc__)
    if args["summary"]:
        check_summary(args["<file1>"], args["<file2>"], args["-d"], verbosity=args["-v"])
