"""Module doing the actual Spark management
"""
from contextlib import contextmanager
from functools import update_wrapper
from pyspark.sql import SparkSession, SQLContext
from six import iteritems

import atexit
import json
import os
import time

from .eventlog import EventLog


class SparkReport(object):
    """Save time differences to a file
    """
    def __init__(self, filename, manager):
        """Create a new instance

        :param filename: filename to store data in
        :param manager: spark manager to query for additional data
        """
        self.__filename = filename
        self.__report = {
            'runtime': [],
            'timing': [[]],
            'spark': {
                'version': manager.spark.version,
                'parallelism': manager.defaultParallelism,
                'executors': manager._jsc.sc().getExecutorMemoryStatus().size(),
            },
            'app': {},
            'slurm': {
                'jobid': os.environ.get('SLURM_JOBID', ''),
                'nodes': os.environ.get('SLURM_NODELIST', ''),
            }
        }
        self.__start = time.time()
        self.__eventlog = None
        logdir = manager.getConf().get('spark.eventLog.dir')
        if logdir:
            self.__eventlog = os.path.join(logdir, manager.getConf().get('spark.app.id') + '.inprogress')
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        elif os.path.exists(filename):
            with open(filename, 'r') as fd:
                data = json.load(fd)
                self.__report['timing'] = data.get('timing', []) + [[]]
                self.__report['runtime'] = data.get('runtime', [])

        def finish():
            """Save the final runtime upon object deletion
            """
            now = time.time()
            self.__report['runtime'].append((now - self.__start, self.__start, now))
            if os.path.exists(self.__eventlog):
                try:
                    log = EventLog(self.__eventlog)
                    self.__report['spark']['shuffle'] = log.shuffle_size
                    self.__report['spark']['rows_max'] = log.max_rows
                    self.__report['spark']['rows_last'] = log.last_rows
                except Exception as e:
                    self.__report['spark']['processing_error'] = str(e)
                    self.__report['spark']['shuffle'] = float('nan')
                    self.__report['spark']['rows_max'] = float('nan')
                    self.__report['spark']['rows_last'] = float('nan')
            with open(self.__filename, 'w') as fd:
                json.dump(self.__report, fd)
        atexit.register(finish)

    def add_info(self, data):
        """Add additional information to the report

        :param data: a dictionary to store under the key `app`
        """
        self.__report['app'].update(data)

    def __call__(self, name, start, end):
        """Update stored information

        :param name: key to use
        :param start: beginning timestamp
        :param end: end timestamp
        """
        self.__report['timing'][-1].append((name, (end - start, start, end)))
        with open(self.__filename, 'w') as fd:
            json.dump(self.__report, fd)


class SparkManager(object):
    """Manage Spark with a singular object
    """
    def __init__(self):
        self.__session = None
        self.__context = None
        self.__sqlcontext = None

        self.__allowed = None
        self.__overlap = None

        self.__gstack = [(None, None)]

        self.__cleaning = False
        self.__report = None

    @property
    def spark(self):
        """:property: the Spark session
        """
        return self.__session

    @property
    def sc(self):
        """:property: the Spark context
        """
        return self.__context

    @property
    def sqlContext(self):
        """:property:
        """
        if not self.__sqlcontext:
            self.__sqlcontext = SQLContext.getOrCreate(self.sc)
        return self.__sqlcontext

    def __getattr__(self, attr):
        """Provide convenient access to Spark functions
        """
        if attr in self.__dict__:
            return self.__dict__[attr]
        if self.__overlap is None:
            raise ValueError("Spark has not been initialized yet!")
        if attr in self.__overlap:
            raise AttributeError("Cannot resolve attribute unambiguously!")
        if attr not in self.__allowed:
            raise AttributeError("Cannot resolve attribute '{}'! Allowed attributes: {}".format(
                attr, ", ".join(sorted(self.__allowed))))
        try:
            return getattr(self.__session, attr)
        except AttributeError:
            return getattr(self.__context, attr)

    def create(self, name=None, config=None, options=None, report=None, reset=False):
        """Create a new Spark session if needed

        Will use the name and configuration options provided to create a new
        spark session and populate the global module variables.

        :param name: the name of the spark application
        :param config: configuration parameters to be applied before
                       building the spark session
        :type config:  a list of key, value pairs
        :param options: environment options for launching the spark session
        :param report: filename to save a timing report
        :type report: str
        :param reset: create a new Spark session
        :type reset: bool
        """
        if self.__session and not reset:
            return self.__session

        # TODO auto-generate name?
        if not name:
            raise ValueError("need a name for a new spark session")

        if options:
            os.environ['PYSPARK_SUBMIT_ARGS'] = options + ' pyspark-shell'

        session = SparkSession.builder.appName(name)

        if config:
            for k, v in config:
                session.config(k, v)

        self.__session = session.getOrCreate()
        self.__context = self.__session.sparkContext

        s_attr = set(dir(self.__session))
        c_attr = set(dir(self.__context))

        self.__allowed = s_attr | c_attr
        self.__overlap = s_attr & c_attr

        identical = set(i for i in self.__overlap
                        if getattr(self.__session, i) is getattr(self.__context, i))
        self.__overlap -= identical
        self.__allowed |= identical

        if report:
            self.__report = SparkReport(report, self)

        return self.__session

    def record(self, data):
        """Pass application data through to the report (if enabled)

        :param data: a dictionary to save in the report JSON
        """
        if self.__report:
            self.__report.add_info(data)

    def register_java_functions(self, fcts):
        """Register java functions with the SQL context of Spark

        :param fcts: a list of tuples containing function alias and java class
        """
        for alias, name in fcts:
            self.sqlContext.registerJavaFunction(alias, name)

    def assign_to_jobgroup(self, f):
        """Assign a spark job group to the jobs started within the decorated
        function

        The job group will be named after the function, with the docstring as
        description.

        :param f: function to decorate
        """
        n = f.__name__
        d = f.__doc__.strip() if f.__doc__ else ''

        def new_f(*args, **kwargs):
            with self.jobgroup(n, d):
                return f(*args, **kwargs)
        return update_wrapper(new_f, f)

    @contextmanager
    def benchmark(self):
        """Create a setup for benchmarking

        Performs a little warmup procedure.

        .. warning::

           Will clear the cache when running!
        """
        try:
            self.reset_cache()
            # Warm-up
            df = self.spark.range(1000)
            df.count()
            yield
        finally:
            pass

    @contextmanager
    def clean_cache(self):
        """Clean the rdd cache

        .. warning::

           May not preserve Dataframes correctly!
        """
        if self.__cleaning:
            msg = "Nested cleaning of temporary RDDs is not supported!"
            raise NotImplementedError(msg)
        self.__cleaning = True
        pre = set(rdd.id() for _, rdd in iteritems(self.sc._jsc.getPersistentRDDs()))
        try:
            yield
        finally:
            post = set(rdd.id() for _, rdd in iteritems(self.sc._jsc.getPersistentRDDs()))
            by_id = {r.id(): r for r in self.sc._jsc.getPersistentRDDs().values()}
            for rdd in post - pre:
                by_id[rdd].unpersist()
            self.__cleaning = False

    @contextmanager
    def jobgroup(self, name, desc=""):
        """Temporarily assign a job group to spark jobs within the context

        :param name: the name of the spark group to use
        :param desc: a longer description of the job group
        """
        self.__context.setJobGroup(name, desc)
        self.__gstack.append((name, desc))
        try:
            start = time.time()
            yield
        finally:
            if self.__report:
                self.__report(name, start, time.time())
            self.__gstack.pop()
            self.__context.setJobGroup(*self.__gstack[-1])

    def reset_cache(self):
        """Clear all caches
        """
        for _, rdd in iteritems(self.sc._jsc.getPersistentRDDs()):
            rdd.unpersist()
        self.catalog.clearCache()
