from __future__ import print_function

import os
import spykfunc
import spykfunc.filters
import tempfile


# DATADIR = os.path.join(os.path.dirname(__file__), '..', 'tests', 'circuit_1000n')
DATADIR = '/home/matwolf/Work/Playpen/O1.v6a'

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
    'output-dir': odir
}

fz = spykfunc.session(*ARGS, **kwargs)
fl = spykfunc.filters.TouchRulesFilter(fz._circuit.touch_rules)

import pdb; pdb.set_trace()

def run():
    data = fl.apply(fz.circuit)
    print(data.count())

if __name__ == '__main__':
    import timeit
    print(timeit.repeat('run()', setup='from __main__ import run', repeat=5, number=1))
