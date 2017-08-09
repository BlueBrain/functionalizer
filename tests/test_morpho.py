import pytest
morphotool = pytest.importorskip("morphotool")
from morphotool import MorphologyDB
from collections import defaultdict
from spykfunc import data_loader
import os.path
import json
from pyspark.sql import SparkSession


def has_problematic_radius(morpho_entry):
    if isinstance(morpho_entry, (tuple, list)):
        morpho_name, morpho_obj = morpho_entry
    else:
        morpho_name, morpho_obj = "morphology", morpho_entry

    radius_zero = 0
    problematic_segm = defaultdict(list)

    for node in morpho_obj.all_nodes:
        if not hasattr(node, 'number_points') or not node.number_points:
            continue
        prev = node.radius[0]
        cur = 0
        for rad in node.radius:
            if rad < 0.001:
                radius_zero += 1
            elif prev > .0 and rad > prev * 10:
                problematic_segm[node.index].append(cur)
            prev = rad
            cur += 1
    if problematic_segm or radius_zero:
        return morpho_name, problematic_segm, radius_zero
    return None


def find_morpho_names_dat(datfile):
    return [MorphologyDB.split_line_to_details(fline)[0] for fline in open(datfile, "r") if fline.strip()]


def find_morpho_names_mvd2(mvdfile):
    start_moto = "Neurons Loaded"
    end_moto = "MiniColumnsPosition"
    list_names = []
    with open(mvdfile) as f:
        line_iter = iter(f)
        line = ""
        while line != start_moto:
            line = next(line_iter).strip()
        line = next(line_iter).strip()
        for line in line_iter:
            line = line.strip()
            if line == end_moto:
                break
            list_names.append(line.split()[0])

    return list_names


def write_results(list, outstream):
    outstream.write("[")
    for item in list:
        json.dump(item, outstream)
        outstream.write(",\n")
    outstream.write("]")


def run_test(morpho_dir, names, spark, outstream):
    fdata = data_loader.FuzerData(data_loader.MVD_Morpho_Loader("", morpho_dir), spark)
    fdata._load_h5_morphologies(names, total_parts=1000)
    morpho_prob = fdata.morphologyRDD.map(has_problematic_radius).filter(lambda x: x is not None)
    all_probs = morpho_prob.collect()
    all_probs = map(lambda a: (a[0], sum(len(vals) for vals in a[1].values()), a[1], a[2]), all_probs)
    all_probs.sort(key=lambda a: a[1], reverse=True)
    write_results(all_probs, outstream)

    probs_count = reduce(lambda a, b: (a[0] + (1 if b[1] else 0), a[1] + b[1], a[2] + (1 if b[3] else 0), a[3] + b[3]), all_probs,
                         (0, 0, 0, 0))
    print("Problem count for " + morpho_dir + """:
        10x+ radius on subsequent segments: %d morphologies affected (%d points)
        empty points: %d morphologies affected (%d points)""" % probs_count)


# ###############
# Specific tests
# ###############
disable_when_not_viz = pytest.mark.skipif(True, reason="Runs only in a cluster")


@disable_when_not_viz
def test_2012_05_31():
    spark = SparkSession.builder.getOrCreate()
    morpho_dir = "/gpfs/bbp.cscs.ch/release/l2/data/morphologies/31.05.12/v1"
    morpho_db = "/gpfs/bbp.cscs.ch/release/l2/data/morphologies/31.05.12/neuronDB.dat"
    morpho_names = find_morpho_names_dat(morpho_db)
    run_test(morpho_dir, morpho_names, spark, open("test_31.05.12.json", "w"))


@disable_when_not_viz
def test_2017_01_05():
    spark = SparkSession.builder.getOrCreate()
    morpho_dir = "/gpfs/bbp.cscs.ch/project/proj59/entities/morphologies/2017.01.05/v1"
    morpho_db = "/gpfs/bbp.cscs.ch/project/proj59/entities/morphologies/2017.01.05/neurondb.dat"
    morpho_names = find_morpho_names_dat(morpho_db)
    run_test(morpho_dir, morpho_names, spark, open("test_2017_01_05.json", "w"))


@disable_when_not_viz
def test_SomatosensoryCxS1_v5_O1():
    spark = SparkSession.builder.getOrCreate()
    base_dir = "/gpfs/bbp.cscs.ch/project/proj1/circuits/SomatosensoryCxS1-v5.r0/O1/merged_circuit"
    morpho_dir = "/gpfs/bbp.cscs.ch/project/proj1/entities/morphologies/2012.07.23/h5"
    morpho_db = os.path.join(base_dir, "circuit.mvd2")
    morpho_names = find_morpho_names_mvd2(morpho_db)
    run_test(morpho_dir, morpho_names, spark, open("test_SomatosensoryCxS1_v5_O1.json", "w"))


@disable_when_not_viz
def test_SomatosensoryCxS1_v5_1x7():
    spark = SparkSession.builder.getOrCreate()
    base_dir = "/gpfs/bbp.cscs.ch/project/proj1/circuits/SomatosensoryCxS1-v5.r0/1x7/merged_circuit"
    morpho_dir = "/gpfs/bbp.cscs.ch/project/proj1/entities/morphologies/2012.07.23/h5"
    morpho_db = os.path.join(base_dir, "circuit.mvd2")
    morpho_names = find_morpho_names_mvd2(morpho_db)
    run_test(morpho_dir, morpho_names, spark, open("test_SomatosensoryCxS1_v5_1x7.json", "w"))


@disable_when_not_viz
def test_SomatosensoryCxS1_v6_O1():
    spark = SparkSession.builder.getOrCreate()
    base_dir = "/gpfs/bbp.cscs.ch/project/proj59/circuits/SomatosensoryCxS1-v6.r1/O1_invivo/merged_circuit"
    morpho_dir = "/gpfs/bbp.cscs.ch/project/proj59/entities/morphologies/2017.01.05/v1"
    morpho_db = os.path.join(base_dir, "circuit.mvd2")
    morpho_names = find_morpho_names_mvd2(morpho_db)
    run_test(morpho_dir, morpho_names, spark, open("test_SomatosensoryCxS1_v6_O1.json", "w"))


if __name__ == "__main__":
    if disable_when_not_viz:
        test_2012_05_31()
        test_2017_01_05()
        test_SomatosensoryCxS1_v5_O1()
        test_SomatosensoryCxS1_v5_1x7()
        test_SomatosensoryCxS1_v6_O1()
