from __future__ import print_function

import os
import spykfunc
from spykfunc.synapse_properties import compute_additional_h5_fields
import tempfile


DATADIR = os.path.join(os.path.dirname(__file__), '..', 'tests', 'circuit_1000n')

ARGS = (
    os.path.join(DATADIR, "builderRecipeAllPathways.xml"),
    os.path.join(DATADIR, "nodes.h5"),
    os.path.join(DATADIR, "touches/*.parquet")
)

tmpdir = tempfile.mkdtemp()
cdir = os.path.join(tmpdir, 'check')
odir = os.path.join(tmpdir, 'out')
kwargs = {
    'checkpoint-dir': cdir,
    'output-dir': odir,
    'overrides': [
        'spark.master=local[4]',
        'spark.driver.memory=4g',
        'spark.executor.cores=4',
        'spark.executor.memory=20g'
    ]
}

fz = spykfunc.session(*ARGS, **kwargs)
fz.process_filters()


def run():
    data = compute_additional_h5_fields(fz.circuit, fz._circuit.synapse_class_matrix, fz._circuit.synapse_class_properties)
    data.count()

if __name__ == '__main__':
    import timeit
    print(timeit.repeat('run()', setup='from __main__ import run', repeat=7, number=5))
