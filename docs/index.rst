=================================
Welcome to Spykfunc documentation
=================================

Spykfunc is a new Functionalizer implementation on top of Spark (PySpark).

Functionalizer goal is to filter out Synapse touches, produced by Touch Detector, according to rules
defined in the recipe. The resulting touches can then be exported to a circuit format as expected
by the simulator, typically nrn_*.h5.


Example
-------

A couple of examples, both single node and using a cluster, can be found in the examples directory in the source.
In the most simple case you just want to run spykfunc from the command line, which will
apply the same filters as functionalizer.

.. code:: bash

   $ spykfunc
   usage: spykfunc [-h] [--s2s] [--no-hdf5] [--output-dir OUTPUT_DIR]
                   [--spark-opts SPARK_OPTS]
                   recipe_file mvd_file morpho_dir touch_files

**Alternatively you can use the Python API**

   >>> import spykfunc
   >>> spykfunc.__version__
   '0.5.1'

   >>> spykfunc.session?
   Signature: spykfunc.session(recipe, mvd_file, first_touch, spark_opts=None)
   Docstring: Creates and Initializes a Functionalizer session
   :returns: A :py:class:`~spykfunc.Functionalizer` instance


Start by initializing a functionalizer session. This helper creates the Functionalizer object and loads the data.

   >>> fz = spykfunc.session(
   ...    "tests/circuit_1000n/builderRecipeAllPathways.xml",
   ...    "tests/circuit_1000n/circuit.mvd3",
   ...    "tests/circuit_1000n/touches/touchesData.*.parquet")
   [INFO] spykfunc.data_loader: Loading global Neuron data...
   [INFO] spykfunc.data_loader: Loading parquets...


On the filtering phase you have multiple options

   >>> # Run all the filters
   >>> fz.process_filters()

   >>> # Alternatively run the ones you select
   >>> fz.filter_by_soma_axon_distance()
   [INFO] spykfunc.functionalizer: Filtering by soma-axon distance...
   >>> fz.run_reduce_and_cut()
   [INFO] spykfunc.functionalizer: Applying Reduce and Cut...
   [INFO] spykfunc.filters: Computing Pathway stats...
   [INFO] spykfunc.filters: Applying Reduce step...

Beware that applying the filters does NOT necessarily run them immediately.
They are executed as you request data results out of them (e.g. export or check stats)

To check the current filtered touches:

.. code-block:: python

   >>> # Inspect the touches, or the graph
   >>> fz.circuit
   DataFrame[src: int, dst: int, pre_section: smallint, pre_segment: smallint, post_section: ...]

   >>> fz.neuron_stats
   <spykfunc.stats.NeuronStats at 0x451d890>

   >>> fz.dataQ.
                fz.dataQ.apply     fz.dataQ.get_stats    fz.dataQ.show
                fz.dataQ.count     fz.dataQ.groupBy
                fz.dataQ.filter    fz.dataQ.select


In the end Export your touches, which will incur computing their properties

   >>> # Note: By default it exports to NRN format in Hdf5. You can optionally export to parquet
   >>> fz.export_results()
   [INFO] spykfunc.functionalizer: Computing touch synaptical properties
   [INFO] spykfunc.functionalizer: Exporting touches...
   [INFO] spykfunc.functionalizer: Done exporting.


Contents
--------

.. toctree::
   :maxdepth: 2

   License <license>
   Authors <authors>
   Changelog <changes>
   Module Reference <api>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
