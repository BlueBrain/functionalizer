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
