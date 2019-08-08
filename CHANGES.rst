=========
Changelog
=========

Version 0.15.0
==============

Changes:
  - Use MorphIO/MorphoKit to read in morphologies. See FUNCZ-199_.
  - Add additional output columns to gap-junction runs. See FUNCZ-211_.
  - Fix executions for circuits with only one synapse class. See FUNCZ-218_.
  - Add preliminary SONATA support. See FUNCZ-217_.

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

.. _FUNCZ-199: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-199
.. _FUNCZ-201: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-201
.. _FUNCZ-209: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-209
.. _FUNCZ-211: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-211
.. _FUNCZ-217: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-217
.. _FUNCZ-218: https://bbpteam.epfl.ch/project/issues/browse/FUNCZ-218
