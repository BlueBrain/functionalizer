=========
Changelog
=========

Version 0.16.0
==============

Changes:
  - Fix a bug where the afferent section type of too many sections was
    changed.  See also FUNCZ-269_.
  - Factor some recipe reading code out into its own module. See also
    FUNCZ-183_.
  - Sort within each output partition to have completely reproducible
    output. See also FUNCZ-262_.
  - Change the input parameters to require ``--from <circuit_file> <population>``
    and ``--to <circuit_file> <population>``. Both source and target parameters
    can differ, allowing to specify different circuit files and/or populations.
    Note that the ``--circuit <circuit_file>`` is replaced by this feature.
  - Add support for NodeSets with ``--from-nodeset <nodesets_file> <nodeset>``
    and ``--to-nodeset <nodesets_file> <nodeset>``, filtering the populations
    specified by the ``--from``/``--to`` parameters. Both source and target
    parameters can differ, allowing different nodesets files and/or nodesets.
  - Change: Refactoring to introduce support for SONATA files natively through
    Libsonata. Note that MVD and/or other legacy files are no longer supported.
    See also FUNCZ-263_.

Version 0.15.9
==============

Changes:
  - Shuffle the data loading order to perform almost all I/O after recipe
    parsing and setup.
    Added an option ``--dry-run`` to read minimal data and verify the
    recipe.
    See also FUNCZ-248_.


Version 0.15.7
==============

Fixes:
  - The `SynapseReposition` filter did not parse the recipe correctly. See
    also FUNCZ-257_.
  - The `nrrp` parameter to synapse generation is read as a floating point
    value again. See also FUNCZ-258_.

Changes:
  - The SONATA input will now create the field `synapse_id`, hence
    deprecating the `AddID` filter.
  - The plotting utilities have been removed as our ability to obtain
    performance data has been crippled. See also FUNCZ-244_.

Version 0.15.6
==============

Fixes:
  - The parameter `nrrp` was off by one.

Version 0.15.5
==============

Changes:
  - Added a `AddID` filter to be able to process SONATA without the
    `synapse_id` field.  Also skip the generating the `axonal_delay` field
    if `distance_soma` is not present in the input.  See also FUNCZ-212_.

Fixes:
  - Multi-population support had source and target populations swapped

Version 0.15.4
==============

Changes:
  - Added `p_A` and `pMu_A` to allowed parameters in `mTypeRule`.  See
    FUNCZ-242_.
  - Added support for additional positions in the TouchDetector output.  See
    FUNCZ-236_.

Fixes:
  - More robust filter loading

Version 0.15.3
==============

Changes:
  - Process `uHillCoefficient` and `gsynSRSF` attributes of
    `SynapseClassification`.  See FUNCZ-238_.
  - Added filters `DenseID` to compress the ids of gap junctions (to be run
    before `GapJunction`, and `GapJunctionProperties` to set the
    conductance of gap junctions.  These filters are active by default when
    running with `--gap-junctions`.

Version 0.15.2
==============

Changes:
  - Split of repositioning of synapses into a separate filter. See
    FUNCZ-226_.
  - Fix branch type matching in `TouchRules`. Allow `axon` to be matched,
    and do no longer match `axon` values when using the `dendrite` value.
    This should not have a user impact, as the default `TouchDetector`
    touch space is axon-dendrite connections. See also FUNCZ-216_.
  - Activate spine length filtering if recipe component is present.

Version 0.15.1
==============

Changes:
  - Improved the determination of fields to write to the output

Version 0.15.0
==============

Changes:
  - Warn if entries in the classification matrix don't cover values. Also
    adds option ``--strict`` to abort execution if any warnings are issued.
    See FUNCZ-86_.
  - Use MorphIO/MorphoKit to read in morphologies. See FUNCZ-199_.
  - Add additional output columns to gap-junction runs. See FUNCZ-211_.
  - Fix executions for circuits with only one synapse class. See FUNCZ-218_.
  - Add preliminary SONATA support. See FUNCZ-217_.
  - Add support for ``{from,to}BranchType`` in `TouchRules`. See FUNCZ-223_.

Version 0.14.3
==============

Changes:
  - Warn when synapse classification does not cover all values. See
    FUNCZ-209_.

Version 0.14.2
==============

Changes:
  - Display intermittent touch count after checkpoints. See also
    FUNCZ-201_.

Version 0.14.1
==============

Changes:
  - Add the fractional position along sections to the output.

Version 0.14.0
==============

Changes:
  - Allow touch rules to filter for more than soma, !soma. The following
    values are valid in the `TouchRule` XML nodes (for the attribute
    `type`):

    - `*` accepts everything
    - `soma` matches soma branches (type 0)
    - `dendrite` matches everything that is not a soma (this reproduces the
      old behavior. Since TouchDetector does not consider touches towards
      axons in normal operation, this matches dendrites only normally)
    - `basal` matches branches of type 2 (basal dendrites)
    - `apical` matches branches of type 3 (apical dendrites)

    Note that the notations correspond to the convention used for
    morphologies saved as H5.
  - Output touch positions: contour for efferent, center position for
    afferent side.
  - Output section type for the afferent side of touches.
  - Output spine length
  - Compare statistical properties of the resulting circuits in the CI
  - Added a `--debug` command line flag to produce additional output

Version 0.13.2
==============

Changes:
  - Ensure that properties drawn from a truncated gaussian are always
    positive: truncate the normal distribution at ±1σ and 0.

Version 0.13.1
==============

Changes:
  - Fix random number generation for determining active connections

==============

Changes:
  - Support post- and pre- neuron ordering of the output.
  - Reordering of the command line options and help

Version 0.12.1
==============

Changes:
  - Fix the morphology output to use floats consistently
  - Add ability to process morphologies stored in nested directories

Version 0.12.0
==============

Changes:
  - Switched to new unique seeding for random numbers: **breaks
    backwards-compatibility on a bitwise comparison**
  - Improved `gap-junctions` support:
    * unique junction ID ready to consume by Neurodamus
    * added bi-directionality to dendro-somatic touches

Version 0.11.0
==============

Changes:
  - Initial support for gap-junctions
  - Control filters run with `--filters` command-line option
  - One of `--structural`, `--functional`, or `--gap-junctions` has to be
    passed to the executable to define filters
  - Save neuron ids as 64 bit integers in the final export
  - Add the following information to `report.json`:
    * the largest shuffle size
    * the number of rows seen last
    * the largest number of rows seen
  - Documented filters

Version 0.10.3
==============

Changes:
  - Read the layers from circuit files rather than inferring them from
    morphologies

Version 0.10.2
==============

Changes:
  - Save `_mvd` directory in the output directory by default
  - Save checkpoints in HDFS automatically
  - Documentation improvements
  - Drop Python 2 support

Version 0.10.1
==============

Changes:
  - Add `parquet-compare` to compare output
  - Add missing package directory

Version 0.10.0
==============

Changes:
  - Circuits are now reproducible by using the seed specified in the recipe
    for sampling and filtering of touches
  - The default output has been renamed from `nrn.parquet` to
    `circuit.parquet`

Version 0.9.1
=============

Changes:
  - Allow to build both `py2` and `py3` versions from the source tree with
    nix
  - Make the synapse repositioning in the recipe optional

Version 0.9
===========

Changes include, but are not limited to:
  - Proper seeding of random numbers to guarantee reproducibility

Version 0.8
===========

Changes include, but are not limited to:
  - Provide a module to run the software
  - Perform synapse shifts

Version 0.1
===========

First working version with 3 base filters:
  - BoutonDistance
  - TouchRules
  - ReduceAndCut

.. _FUNCZ-86: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-86
.. _FUNCZ-183: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-183
.. _FUNCZ-199: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-199
.. _FUNCZ-201: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-201
.. _FUNCZ-209: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-209
.. _FUNCZ-211: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-211
.. _FUNCZ-212: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-212
.. _FUNCZ-216: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-216
.. _FUNCZ-217: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-217
.. _FUNCZ-218: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-218
.. _FUNCZ-223: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-223
.. _FUNCZ-226: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-226
.. _FUNCZ-236: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-236
.. _FUNCZ-238: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-238
.. _FUNCZ-242: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-242
.. _FUNCZ-244: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-244
.. _FUNCZ-248: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-248
.. _FUNCZ-257: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-257
.. _FUNCZ-258: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-258
.. _FUNCZ-262: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-262
.. _FUNCZ-263: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-263
.. _FUNCZ-269: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-269
