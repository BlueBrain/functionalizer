"""Compare two parquet output directories of Spykfunc
"""
from __future__ import print_function

import argparse

import sparkmanager as sm
from spykfunc import utils
from pyspark.sql.functions import col


def compare(a, b):
    only_a = a.subtract(b)
    only_b = b.subtract(a)
    na = only_a.count()
    nb = only_b.count()
    if na > 0:
        print("> Rows unique to baseline found!")
        only_a.show()
    if nb > 0:
        print("> Rows unique to comparison found!")
        only_b.show()
    return na == nb


def connections(df):
    keys = ["pre_gid", "post_gid"]
    return df.select(*keys).distinct().cache()


def compare_connection(row, ba, co):
    a = ba.where((col("pre_gid") == row.pre_gid) & (col("post_gid") == row.post_gid))
    b = co.where((col("pre_gid") == row.pre_gid) & (col("post_gid") == row.post_gid))
    return compare(a, b)


class _ConfDumpAction(argparse._HelpAction):
    """Dummy class to list default configuration and exit, just like `--help`.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        from spykfunc.utils import Configuration
        kwargs = dict(overrides=namespace.overrides)
        if namespace.configuration:
            kwargs['configuration'] = namespace.configuration
        Configuration(namespace.output_dir, **kwargs).dump()
        parser.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", help="the output directory to compare to")
    parser.add_argument("compare", help="the output directory to compare with")
    parser.add_argument("-n", default=100, type=int,
                        help="How many connections to compare")
    parser.add_argument("-c", "--configuration",
                        help="A configuration file to use. See `--dump-defaults` for default settings")
    parser.add_argument("-p", "--property", dest='overrides', action='append', default=[],
                        help="Override single properties of the configuration, i.e.,"
                             "`-p spark.master=spark://1.2.3.4:7077`. May be specified multiple times.")
    parser.add_argument("--dump-configuration", action=_ConfDumpAction,
                        help="Show the configuration including modifications via options prior to this "
                             "flag and exit")
    args = parser.parse_args()

    properties = utils.Configuration("",
                                     filename=args.configuration,
                                     overrides=[s.split('=', 1) for s in args.overrides])

    sm.create("comparison", properties("spark"))

    base = sm.read.load(args.baseline)
    comp = sm.read.load(args.compare)

    print(">> Comparing global connections")
    base_conn = connections(base)
    compare(base_conn, connections(comp))

    f = float(args.n) / base_conn.count()
    unequal = []
    print(">> Comparing local connections")
    for row in base_conn.sample(withReplacement=False, fraction=f).collect():
        if not compare_connection(row, base, comp):
            unequal.append((row.pre_gid, row.post_gid))
    for pre, post in unequal:
        print("> Differences in {} -> {}".format(pre, post))
