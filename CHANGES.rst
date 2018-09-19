=========
Changelog
=========

Version 0.11.0
==============

Changes:

  - Initial support for gap-junctions
  - Control filters run with `--filters` command-line option
  - One of `--structural`, `--functional`, or `--gap-junctions` has to be
    passed to the executable to define filters

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

