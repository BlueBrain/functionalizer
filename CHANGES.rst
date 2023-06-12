=========
Changelog
=========

Version 0.18.0
==============

Changes:
  - Better detection if section types are 0-based or 1-based.
    See also FUNCZ-333_.
  - Join node dataframe to edges only once.
    See also FUNCZ-309_.

Version 0.17.5
==============

Changes:
  - Do not require the recipe by default.
    See also FUNCZ-318_.

Version 0.17.4
==============

Fixes:
  - Load metadata correctly for Parquet input.
  - Do not encode filenames when passing to ``libsonata``.

Version 0.17.3
==============

.. warning:: This release deprecates the ``sm_*`` commands, please switch to the new
             ``functionalizer`` command.

Changes:
  - Introduce a new ``functionalizer`` command, where ``srun functionalizer`` replaces
    ``sm_run spykfunc``.  The new ``functionalizer`` command accepts all options from
    Spykfunc, too.
    See also FUNCZ-325_.

Version 0.17.2
==============

.. warning:: Changes in this release may require an additional ``--`` after the
             ``--morphologies`` flag for proper command line argument parsing.

Changes:
  - Add a new filter that allows to assign morphologies to spines.
    See also FUNCZ-301_.
  - Add the column ``edge_type_id`` to gap-junction output to conform with
    the SONATA specification.
  - Load all node attributes and make them conditional at the same time.
    Should avoid having to specify bogus attributes when using projections.
    See also FUNCZ-307_.

Version 0.17.1
==============

Breaking Changes:
  - Write section types out with the correct
    `MorphIO convention`_.
    See also FUNCZ-289_.

Changes:
  - Allow unsigned integer types for SONATA input.  Restricted to a maximum
    of 32 bits.
    See also FUNCZ-291_.

Version 0.17.0
==============

.. warning:: Changes in this release will yield different results compared
             to previous versions when re-running circuits.

Breaking Changes:
  - Replace the random number generating library. Switch to using the tried
    and tested Random123 library.  See also HPCTM-1294_.
  - Run the `TouchRules` filter by default in structural mode.  See also
    FUNCZ-255_.

Changes:
  - Fix a regression that led to duplicated gap-junction ids.  See also
    FUNCZ-287_.

Version 0.16.99
===============

Breaking Changes:
  - Drop the `SynapseProperties` filter by default in structural mode.
    Space savings are small, but time savings seem worthwhile.  See
    also FUNCZ-265_.

Changes:
  - Produce `debug` output by default.  See also FUNCZ-281_.
  - Reduce the output of the cluster startup script to display important
    information more prominently.  Spykfunc will not require the flag
    ``-p spark.master=…`` any longer when launched with ``sm_run``.
    See also FUNCZ-275_.
  - Generalize :class:`~recipe.parts.touch_connections.ConnectionRule`,
    where ``<mTypeRule to="…" …`` is now superseded by ``<rule toMType="…" …``
    and additional selection criteria may be specified.
  - Store metadata about previous tool invocations and add recipe, filters
    used.  See also HPCTM-1425_.
  - Track touches dropped and raise an exception if touches are removed
    filters like synaptic property generation.  See also FUNCZ-274_.
  - Rework user interface to require the flags ``--recipe`` and
    ``--morphologies``, previously positional arguments, when using
    filters.  Drop ``--parquet`` and ``--touches`` and use positional
    arguments instead, auto-detecting the input file type.
  - Allow to not specify any filters or use the ``--merge`` flag to process
    several inputs of **non-overlapping** edge populations.  See also
    FUNCZ-279_.

    .. note:: If a source—target connection appears in more than one input,
              synapses for this input may not be sorted in a reproducible
              way.

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
.. _FUNCZ-255: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-255
.. _FUNCZ-257: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-257
.. _FUNCZ-258: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-258
.. _FUNCZ-262: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-262
.. _FUNCZ-263: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-263
.. _FUNCZ-265: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-265
.. _FUNCZ-269: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-269
.. _FUNCZ-274: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-274
.. _FUNCZ-275: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-275
.. _FUNCZ-277: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-277
.. _FUNCZ-279: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-279
.. _FUNCZ-281: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-281
.. _FUNCZ-287: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-287
.. _FUNCZ-289: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-289
.. _FUNCZ-291: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-291
.. _FUNCZ-301: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-301
.. _FUNCZ-307: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-307
.. _FUNCZ-309: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-309
.. _FUNCZ-318: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-318
.. _FUNCZ-325: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-325
.. _FUNCZ-333: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-333
.. _HPCTM-1294: https://bbpteam.epfl.ch/project/issues/browse/HPCTM-1294
.. _HPCTM-1425: https://bbpteam.epfl.ch/project/issues/browse/HPCTM-1425

.. _MorphIO convention: https://github.com/BlueBrain/MorphIO/blob/dea3ce8/include/morphio/enums.h#L61-L95
