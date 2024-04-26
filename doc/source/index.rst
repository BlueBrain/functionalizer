=================================
Welcome to Spykfunc documentation
=================================

Spykfunc is a re-implementation of the legacy Functionalizer on top of the
Python bindings of Spark.

Spykfunc processes the output of TouchDetector_, which detects all touches
between cells in a volume.
Through various filters, the distributions of touches between cells can be
adjusted to follow biological models.
Before writing data back to disk, various synaptic properties are
optionally generated to turn touches into proper synapses.
This latter step can be run on its own, to i.e. augment projections with
the right synapse properties.

As `Apache Spark`_ interacts best with the Parquet_ format, Spykfunc
preferably reads and writes Parquet for the edge files, while nodes may be
provided in the SONATA format.
An alternative input mode for SONATA edges exists, but should be treated as
an experimental feature at this time.

Format conversion can be done via the software provided by ParquetConverters_.

.. toctree::
   :maxdepth: 3

   Getting Started <usage>
   Filters <filters>
   Debugging <debugging>

   Spykfunc API <api>

   Changelog <changes>

   License <license>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _ParquetConverters: https://bbpteam.epfl.ch/documentation/p.html#parquetconverters
.. _TouchDetector: https://bbpteam.epfl.ch/documentation/t.html#touchdetector
.. _Parquet: https://parquet.apache.org/
.. _Apache Spark: https://spark.apache.org/
