Functionalizer
==============

Functionalizer is a tool for filtering the output of a touch detector (the "touches")
according to morphological models, given in in the form of recipe prescription as
described in the `SONATA extension`_.

To process the large quantities of data optimally, this software uses PySpark.

Installation
------------

The easiest way to install `functionalizer` is via ``pip install functionalizer``.

.. note::
   Due to a dependency on ``mpi4py``, a MPI implementation needs to be installed on the
   system used.  On Ubuntu, this can be achieved with:
   .. code-block:: console
      apt-get install -y libopenmpi-dev

For manual installation from sources via ``pip``, a compiler handling C++17 will be
necessary.  Furthermore, all ``git`` submodules should be checked out:

.. code-block:: console

   gh repo clone BlueBrain/functionalizer -- --recursive --shallow-submodules
   cd functionalizer
   pip install .

Spark and Hadoop should be installed and set up as runtime dependencies.

Usage
-----

Basic usage follows the pattern::

    functionalizer --s2f --circuit-config=circuit_config.json --recipe=recipe.json edges.h5

Where the final argument `edges.h5` may also be a directory of Parquet files.  When
running on a cluster with multiple nodes, care should be taken that every rank occupies a
whole node, Spark will then spread out across each node.

Acknowledgment
--------------
The development of this software was supported by funding to the Blue Brain Project,
a research center of the École polytechnique fédérale de Lausanne (EPFL),
from the Swiss government's ETH Board of the Swiss Federal Institutes of Technology.

Copyright (c) 2017-2024 Blue Brain Project/EPFL

.. _SONATA extension: https://sonata-extension.readthedocs.io
