.. _filters:

Synapse Filters
===============

The following filters are accepted by `Spykfunc`'s ``--filters`` command
line option.
To use any of the filters, remove the `Filter` suffix if present, e.g.,
:class:`~BoutonDistanceFilter` becomes ``BoutonDistance``.

Parametrized Synapse Reduction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: spykfunc.filters.implementations.bouton_distance.BoutonDistanceFilter
.. autoclass:: spykfunc.filters.implementations.soma_distance.SomaDistanceFilter
.. autoclass:: spykfunc.filters.implementations.touch.TouchReductionFilter
.. autoclass:: spykfunc.filters.implementations.touch.TouchRulesFilter

Synapse Identification
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: spykfunc.filters.implementations.synapse_id.AddIDFilter
.. autoclass:: spykfunc.filters.implementations.synapse_id.DenseIDFilter

Generating Properties for Gap-Junctions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: spykfunc.filters.implementations.gap_junction.GapJunctionFilter
.. autoclass:: spykfunc.filters.implementations.gap_junction.GapJunctionProperties

Sampled Reduction
~~~~~~~~~~~~~~~~~

.. autoclass:: spykfunc.filters.implementations.spine_length.SpineLengthFilter

.. autoclass:: spykfunc.filters.implementations.reduce_and_cut.ReduceAndCut

Generating Properties of Chemical Synapses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: spykfunc.filters.implementations.synapse_reposition.SynapseReposition
.. autoclass:: spykfunc.filters.implementations.synapse_properties.SynapseProperties
