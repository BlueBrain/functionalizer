===========================================
Welcome to the functionalizer documentation
===========================================

To generate a fully functional connectome, `functionalizer` takes the output of cell
apposition of either `touchdetector`_ or `connectome-manipulator`_, and reduces the
distribution of touches to follow biological models. Following this optional reduction,
`functionalizer` generates synaptic properties to output a fully functional connectome
that may then be simulated.

The reduction operations and synaptic property generation use the `fz-td-recipe`_ package
to parse a recipe description as described by the `SONATA extension`_.

As `Apache Spark`_ interacts best with the Parquet_ format, the utilities in
`parquet-converters`_ may be used to convert binary `touchdetector`_ output into Parquet
and to convert the `functionalizer` Parquet output to SONATA.

.. toctree::
   :maxdepth: 3

   Getting Started <usage>
   Filters <filters>
   Debugging <debugging>

   Functionalizer API <api>

   Changelog <changes>

   License <license>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _SONATA extension: https://sonata-extension.readthedocs.io/en/latest/
.. _fz-td-recipe: https://github.com/BlueBrain/fz-td-recipe
.. _connectome-manipulator: https://github.com/BlueBrain/connectome-manipulator
.. _parquet-converters: https://github.com/BlueBrain/parquet-converters
.. _touchdetector: https://github.com/BlueBrain/touchdetector
.. _Parquet: https://parquet.apache.org/
.. _Apache Spark: https://spark.apache.org/
