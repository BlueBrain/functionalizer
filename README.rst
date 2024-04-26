Functionalizer
==============

Functionalizer is a tool for filtering the output of a touch detector (the "touches")
according to morphological models, given in in the form of recipe prescription as
described in the `SONATA extension`_.

To process the large quantities of data optimally, this software uses PySpark.

Installation
------------

For manual installation via `pip`, a compiler handling C++17 will be necessary.
Otherwise, all `git` submodules should be checked out and `cmake` as well as `ninja` be
installed.

Spark and Hadoop should be installed and set up as runtime dependencies.

Usage
-----

Basic usage follows the pattern::

    functionalizer --s2f --circuit-config=circuit_config.json --recipe=recipe.json edges.h5

Where the final argument `edges.h5` may also be a directory of Parquet files.  When
running on a cluster with multiple nodes, care should be taken that every rank occupies a
whole node, Spark will then spread out across each node.

.. _SONATA extension: https://sonata-extension.readthedocs.io
