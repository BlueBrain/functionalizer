`functionalizer` API
====================

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

.. autoclass:: functionalizer.core.Functionalizer
   :members: init_data, export_results, process_filters

Data Handling
`````````````

The :class:`.NodeData` class is used to read both nodes and edges from
binary storage or Parquet.  Nodes are customarily stored in SONATA_ format
based on HDF5, and :class:`.NodeData` will internally cache them in
Parquet format for faster future access.

.. autoclass:: functionalizer.circuit.Circuit

.. autoclass:: functionalizer.io.NodeData
   :members: load_neurons, load_touch_parquet, load_touch_sonata

Filtering
`````````

A detailed overview of the scientific filter implementations available in
``functionalizer`` can be found in :ref:`Synapse Filters <filters>`.

.. autoclass:: functionalizer.filters.DatasetOperation
   :members:
   :private-members:

.. _SONATA: https://sonata-extension.readthedocs.io/en/latest/
