=========
Changelog
=========

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

