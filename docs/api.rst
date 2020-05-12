Code Structure
==============

The underlying operating basis of the Spark Functionalizer is the
subsequent application of several filters to a circuit representation
encompassing synapses and cells to obtain a realistic set of synapses
representing the connectome of a brain region.

In this implementation, a central :class:`.Functionalizer` instance is used
to configure the Apache Spark setup, then load the appropriate cell data,
scientific recipe, and touches between cells.  Internally, the brain
circuit is then represented by the :class:`.Circuit` class.
A sequence of filters inheriting from the :class:`.DatasetOperation` class
process the touches, which can be subsequently written to disk.

Entry Point
```````````

For most uses, the :class:`.Circuit` is constructed by the
:class:`.Functionalizer` class based on user parameters passed through.
The latter handles also user parameters and the setup of the Apache Spark
infrastructure, including memory settings and storage paths.

.. autoclass:: spykfunc.functionalizer.Functionalizer
   :members: init_data, export_results, process_filters

Data Handling
`````````````

The :class:`.NeuronData` class is used to read both nodes and edges from
binary storage or Parquet.  Nodes are customarily stored in either the
SONATA_ or MVD3_ format based on HDF5, and :class:`.NeuronData` will
internally cache them in Parquet format for faster future access.

.. autoclass:: spykfunc.circuit.Circuit

.. autoclass:: spykfunc.data_loader.NeuronData
   :members: load_neurons, load_touch_parquet, load_touch_sonata

Filtering
`````````

A detailed overview of the scientific filter implementations available in
`Spykfunc` can be found in :ref:`Synapse Filters <filters>`.

.. autoclass:: spykfunc.filters.DatasetOperation
   :members:
   :private-members:

.. _SONATA: https://bbpteam.epfl.ch/documentation/projects/Circuit%20Documentation/latest/sonata.html
.. _MVD3: https://bbpteam.epfl.ch/documentation/projects/Circuit%20Documentation/latest/mvd3.html
