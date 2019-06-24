API
===

Overview and Examples
---------------------

.. code::

   >>> import spykfunc
   >>> spykfunc.__version__
   '0.9.0'

   >>> spykfunc.session?
   Signature: spykfunc.session(recipe, mvd_file, first_touch, properties=None, …)
   Docstring: Creates and Initializes a Functionalizer session
   :returns: A :py:class:`~spykfunc.Functionalizer` instance


Start by initializing a functionalizer session. This helper creates the Functionalizer object and loads the data.

   >>> fz = spykfunc.session(
   ...    "tests/circuit_1000n/builderRecipeAllPathways.xml",
   ...    "tests/circuit_1000n/circuit.mvd3",
   ...    "tests/circuit_1000n/morphologies/h5",
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

In the end Export your touches, which will incur computing their properties

   >>> fz.export_results()
   [INFO] spykfunc.functionalizer: Computing touch synaptical properties
   [INFO] spykfunc.functionalizer: Exporting touches...
   [INFO] spykfunc.functionalizer: Done exporting.

Spykfunc module
```````````````

.. automodule:: spykfunc
    :members:


Functionalizer
``````````````

.. automodule:: spykfunc.functionalizer
    :members: Functionalizer, session


Synapse Filters
```````````````

.. automodule:: spykfunc.filters
    :members: SomaDistanceFilter, GapJunctionFilter, BoutonDistanceFilter, TouchRulesFilter, ReduceAndCut, SynapseProperties
