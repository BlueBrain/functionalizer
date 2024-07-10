.. _debugging:

Debugging
=========

Debug flags
-----------

For additional output containing information about the parameters for
reduce and cut steps, run `Functionalizer` with the ``--debug`` flag.

.. warning::

   As activating this flag will lead to `Functionalizer` consuming a lot of
   memory to gather the required information for the debug output, it is
   not advised to activate this flag for large executions or by default.

Following a live execution
--------------------------

While a `Functionalizer` process is running, progress can be followed using the
web-interface of the first node executing `Functionalizer`, given that
``${SLURM_JOBID}`` points to a valid SLURM job:

.. code-block:: console

   $ echo "http://$(echo $(sacct -j ${SLURM_JOBID} -n -o nodelist|head -n 1)):4040"

Above command will print the URL of the Apache Spark web interface, which
can be used to track some progress.

Similarly, if a Hadoop cluster is instantiated alongside the Spark cluster,
using the following will yield a web interface to check on, e.g., disk
usage:

.. code-block:: console

   $ echo "http://$(echo $(sacct -j ${SLURM_JOBID} -n -o nodelist|head -n 1)):50070

To gauge other resource usage, follow the node list given by

.. code-block:: console

  $ sacct -j ${SLURM_JOBID} -n -o nodelist

With the BBP internal system monitoring, CPU utilization and memory usage
can be displayed for each of the nodes listed by the command above.
Use the `BB5 System monitoring`_ dashboard directly, and search for the
fully qualified domain name of the node above (including the
``_bbp_epfl_ch`` suffix).
In case the URL has changed, look for the corresponding dashboard in the
`BlueBrain Grafana instance`_.

Post execution analysis
-----------------------

Logs of past executions can be analyzed if the logs in a directory called
`eventlog` have been conserved.
To find this directory, use the following in the output directory of
`Functionalizer`:

.. code-block:: console

   $ find . -name eventlog
   $ export LOGDIR=$(find . -name eventlog)

Then an Apache Spark history server can be started as follows:

.. code-block:: console

   $ module load functionalizer
   $ ${SPARK_HOME}/bin/spark-class \
       -Dspark.history.fs.logDirectory=${LOGDIR} \
       -Dspark.daemon.memory=30g \
       -Dspark.daemon.cores=8 \
       org.apache.spark.deploy.history.HistoryServer

The history server then will be active on port 18080 of the machine it was
started on, i.e., if ``bbpv1`` is used, navigate to ``http://bbpv1:18080``.
There will be a list of past executions of `Functionalizer`, and the history
server will take a small while to process one when opened.

The page of the execution will display job and stage status (a stage 

.. _BB5 System monitoring: https://bbpmonitoring.epfl.ch/metrics/d/000000101
.. _BlueBrain Grafana instance: https://bbpmonitoring.epfl.ch/metrics
