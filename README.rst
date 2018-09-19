========
spykfunc
========

A pySpark implementation of BBP Functionalizer


Description
===========

Functionalizer is a tool for filtering the output of touch detector (the "touches") 
according to morphological models, given in in the form of "recipe" xml files.

This software leverages Spark to perform the filtering in a distributed manner.


Note
====


Installation on ``bbp*viz``
---------------------------

The following modules should be loaded on the cluster to allow the python
extensions to be compiled:
.. code::

   module load nix/python  # for python2.7
   module load nix/boost
   module load nix/hdf5
   module load nix/hpc/functionalizer
   module load nix/hpc/highfive
   module load nix/hpc/mvd-tool

Procedure for building:
.. code::

   virtualenv testenv
   . testenv/bin/activate
   pip install --process-dependency-links .


Usage
-----

To run the functionalizer over all touches in a directory, use one of the
two forms below:

.. code:: shell

   spykfunc builderRecipeAllPathways.xml circuit.mvd3 morphologies touches/*.parquet
   spykfunc builderRecipeAllPathways.xml circuit.mvd3 morphologies "touches/*.parquet"

Or run on only one or several files with:

.. code:: shell

   spykfunc builderRecipeAllPathways.xml circuit.mvd3 morphologies touches/touchesData.0.parquet
   spykfunc builderRecipeAllPathways.xml circuit.mvd3 morphologies touches/touchesData.{0,1,2}.parquet
