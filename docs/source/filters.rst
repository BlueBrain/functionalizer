.. _filters:

Synapse Filters
===============

The following filters are accepted by ``functionalizer``'s ``--filters`` command
line option.
To use any of the filters, remove the `Filter` suffix if present, e.g.,
:class:`~BoutonDistanceFilter` becomes ``BoutonDistance``.

Parametrized Synapse Reduction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: functionalizer.filters.implementations.bouton_distance.BoutonDistanceFilter
.. autoclass:: functionalizer.filters.implementations.soma_distance.SomaDistanceFilter
.. autoclass:: functionalizer.filters.implementations.touch.TouchReductionFilter
.. autoclass:: functionalizer.filters.implementations.touch.TouchRulesFilter

Synapse Identification
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: functionalizer.filters.implementations.synapse_id.AddIDFilter
.. autoclass:: functionalizer.filters.implementations.synapse_id.DenseIDFilter

Generating Properties for Gap-Junctions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: functionalizer.filters.implementations.gap_junction.GapJunctionFilter
.. autoclass:: functionalizer.filters.implementations.gap_junction.GapJunctionProperties

Sampled Reduction
~~~~~~~~~~~~~~~~~

.. autoclass:: functionalizer.filters.implementations.spine_length.SpineLengthFilter

.. autoclass:: functionalizer.filters.implementations.reduce_and_cut.ReduceAndCut

Generating Properties of Chemical Synapses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: functionalizer.filters.implementations.synapse_reposition.SynapseReposition
.. autoclass:: functionalizer.filters.implementations.synapse_properties.SynapseProperties
