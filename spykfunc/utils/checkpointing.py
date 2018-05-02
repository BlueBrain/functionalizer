from __future__ import absolute_import

from os import path as osp
from collections import namedtuple
from funcsigs import signature
from functools import wraps
from pyspark.sql.column import _to_seq
from pyspark.sql import DataFrame
import sparkmanager as sm
from . import get_logger


class CheckpointStatus:
    """A CheckpointStatus object shall be passed to the decorator in order to retrieve information
    Note: A state of error doesnt invalidate the dataframe, and resume is attempted
    """
    INVALID = 0             # Not initialized
    RESTORED_PARQUET = 1    # Not computed, restored from parquet
    RESTORED_TABLE = 2      # Not computed, restored from a Table
    COMPUTED = 3            # Computed, no checkpoint available
    ERROR = -1              # CheckPoint not available and error creating/loading it

    def __init__(self):
        self.state = self.INVALID   # property: The Checkpointing end state
        self.error = None           # property: The Exception if any thrown during checkpointing"""


class CheckpointHandler:
    """A Handler for a checkpoint.
    A list of such handlers can be passed to CheckpointResume for any of the enumerated events.
    """
    BEFORE_LOAD = 0
    BEFORE_SAVE = 1
    POST_RESUME = 2
    POST_COMPUTE = 3

    def __init__(self, handler_type, handker_f):
        self.type = handler_type
        self.f = handker_f

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)

    @classmethod
    def apply_all(cls, df, handlers, handler_type):
        """Recursively applies all handlers matching a given type to the dataframe"""
        for h in cls.filter(handlers, handler_type):  # type: CheckpointHandler
            df = h(df)
        return df

    @classmethod
    def run_all(cls, handlers, handler_type):
        """Runs all the handlers which match a given type"""
        for h in cls.filter(handlers, handler_type):  # type: CheckpointHandler
            h()

    @classmethod
    def filter(cls, handlers, handler_type):
        """Returns the subset of handlers which match the given type"""
        return [h for h in handlers if h.type == handler_type]

    # Helpers
    before_load = classmethod(lambda cls, f: cls(cls.BEFORE_LOAD, f))
    before_save = classmethod(lambda cls, f: cls(cls.BEFORE_SAVE, f))
    post_resume = classmethod(lambda cls, f: cls(cls.POST_RESUME, f))
    post_compute = classmethod(lambda cls, f: cls(cls.POST_COMPUTE, f))


class CheckpointResume:
    """ Class implementing checkpointing and restore, to parquet and parquet-based tables
    """

    class _RunParams:
        """ Parameters for a single checkpoint call, since the object is shared"""
        dest = None
        overwrite = False
        break_exec_plan = True
        bucket_cols = False
        n_buckets = True
        handlers = ()
        logger = get_logger("spykfunc.checkpoint")
        filename_suffix = ""
        status = None
        # Runtime
        table_name = None
        table_path = None
        parquet_file_path = None

        def __init__(self, **opts):
            for name, value in opts.items():
                if value is not None and hasattr(self, name):
                    setattr(self, name, value)

    _Runnable = namedtuple("_Runnable", ("f", "args", "kw"))

    # =========
    def __init__(self, directory=None, overwrite=False):
        self.directory = directory
        self.overwrite = overwrite
        self.last_status = None

    def __call__(self, name, dest=None, overwrite=False, break_exec_plan=True,
                 bucket_cols=False, n_buckets=True,  # True -> Same nr partitions
                 handlers=None, filename_suffix="",
                 logger=None, status=None):
        """ Decorator for checkpointing_resume routines

        :param name: The name of the checkpoint, preferably no whitespaces
        :param dest: The destination path for data files
        :param overwrite: If True will not attempt to resume and forces reevaluating df. Default: False
        :param break_exec_plan: If True (default) will reload the saved data to break the execution plan
        :param bucket_cols: A tuple defining the columns to which partition the data.
            NOTE: This option activates storing as Table (default: False)
        :param n_buckets: The number of partition buckets. Default: True, which uses the df number of partitions
            NOTE: The number of buckets will multiply the number of output files if the df is not properly
            partitioned. Use this option (and bucket_cols) with caution, consider repartition() before
        :param handlers: A list of CheckpointHandler functions to run on respective Checkpointing phases
        :param filename_suffix: A suffix to be appended to the checkpoint data file. This might be used to distinguish
            among checkpoints produced in slightly different conditions
        :param logger: A logger object. Defaults to spykfunc master logger
        :param status: A CheckPointStatus object can be passed if checkpointing process information is desirable
        :return: The checkpointed dataframe, built from the created files unless break_exec_plan was set False
        """

        _dec_kw = {k: v for k, v in locals().items() if v is not None}
        _dec_kw.pop('self')
        _params = self._RunParams(**_dec_kw)
        if _params.status is None:
            _params.status = CheckpointStatus()

        def decorator(f):
            if isinstance(f, DataFrame):
                # Support for checkpointing a dataframe directly
                _params.overwrite = _dec_kw.get('overwrite', self.overwrite)
                _params.dest = _dec_kw.get('dest', self.directory)

                # When overwrite is True all subsequent checkpoints shall be overwritten
                if _params.overwrite:
                    self.overwrite = True

                return self._run(f, name, _params)

            # Create the decorated method
            @wraps(f)
            def new_f(*args, **kw):
                # Decorated function params might override behavior
                # locals() gets all params as keywords, inc positional
                all_args = dict(zip(signature(f).parameters.keys(), args))
                all_args.update(kw)
                # The code commented below allows and removes options that didn't make part of the signature
                # However if the signature has kwargs it will introduce an unexpected behavior
                # mode = all_kw['mode'] if 'mode' in signature(f).parameters \
                #    else kw.pop('mode', None)
                if 'mode' in all_args:
                    _params.filename_suffix = all_args['mode']

                _params.overwrite = all_args.get('overwrite', self.overwrite)
                # If True then change the global default, so subsequent steps are recomputed
                if _params.overwrite:
                    self.overwrite = True

                _params.dest = _dec_kw.get('dest', self.directory)  # If None in decorator, use the current
                if _params.dest is None:
                    _params.logger.error("Checkpoints dir has not been set. Assuming _checkpoints.")
                    _params.dest = "_checkpoints"

                return self._run(self._Runnable(f, args, kw), name, _params)

            return new_f
        return decorator

    # ---
    @classmethod
    def _run(cls, df, name, params):
        # type: (object, str, CheckpointResume._RunConfig) -> DataFrame
        """ Checkpoints a dataframe (internal)

        :param df: The df or the tuple with the calling info to create it
            Note: We avoid creating the DF before since it might have intermediate implications
        :param name: The logical name of the dataframe checkpoint
        :param params: The params of the current checkpoint_resume run
        """
        params.table_name = name.lower()
        basename = osp.join(params.dest, params.table_name)
        if params.filename_suffix:
            basename += '_' + str(params.filename_suffix)
        params.table_path = basename + ".ptable"
        params.parquet_file_path = basename + ".parquet"

        # Attempt to load, unless overwrite is set to True
        if params.overwrite:
            if osp.exists(params.parquet_file_path) or osp.exists(params.table_path):
                params.logger.info("[OVERWRITE %s] Checkpoint found. Overwriting...", name)
        else:
            restored_df = cls._try_restore(name, params)
            if restored_df is not None:
                return restored_df

        # Apply transformations
        if isinstance(df, cls._Runnable):
            df = df.f(*df.args, **df.kw)

        df = CheckpointHandler.apply_all(df, params.handlers, CheckpointHandler.POST_COMPUTE)

        try:
            df = cls._do_checkpoint(df, name, params)
            if params.break_exec_plan:
                df = cls._try_restore(name, params)
            params.status.state = CheckpointStatus.COMPUTED

        except Exception as e:
            params.status.state = CheckpointStatus.ERROR
            params.status.error = e
            params.logger.error("Checkpointing failed. Error: " + str(e))
            params.logger.warning("Attempting to continue without checkpoint")

        return df

    # --
    @staticmethod
    def _try_restore(name, params):
        """ Tries to restore a dataframe, from table or raw parquet, according to the params object

        :param name: the name of the stage
        :param params: The checkpoint_restore session params
        """
        df = None

        def try_except_restore(restore_f, source):
            CheckpointHandler.run_all(params.handlers, CheckpointHandler.BEFORE_LOAD)
            try:
                params.logger.info("[SKIP %s] Checkpoint found. Restoring state...", name)
                df = restore_f(source)
                params.status.error = None
                return df

            except Exception as e:
                params.logger.warning("Could not load checkpoint from table. Reason: %s", str(e))
                params.status.state = CheckpointStatus.ERROR
                params.status.error = e
                return None

        # Attempting from table
        if params.bucket_cols and osp.isdir(params.table_path):
            df = try_except_restore(sm.read.table, params.table_name)
            if df is not None:
                params.status.state = CheckpointStatus.RESTORED_TABLE

        # If no table, or error, try with direct parquet
        if df is None and osp.exists(params.parquet_file_path):
            df = try_except_restore(sm.read.parquet, params.parquet_file_path)
            if df is not None:
                params.status.state = CheckpointStatus.RESTORED_PARQUET

        # All good? Run post handlers
        if df is not None:
            df = CheckpointHandler.apply_all(df, params.handlers, CheckpointHandler.POST_RESUME)
        return df

    # --
    @staticmethod
    def _do_checkpoint(df, name, params):
        table_name = params.table_name
        bucket_cols = params.bucket_cols

        df = CheckpointHandler.apply_all(df, params.handlers, CheckpointHandler.BEFORE_SAVE)

        if params.bucket_cols:
            params.logger.debug("Checkpointing to TABLE %s...", table_name)
            with sm.jobgroup("checkpointing {} to TABLE".format(table_name)):
                # For the moment limited support exists, we need intermediate Hive tables
                if isinstance(bucket_cols, (tuple, list)):
                    col1 = bucket_cols[0]
                    other_cols = bucket_cols[1:]
                else:
                    col1 = bucket_cols
                    other_cols = []

                num_buckets = df.rdd.getNumPartitions() if params.n_buckets is True \
                    else params.n_buckets

                (df.write.mode("overwrite").option("path", params.table_path)._jwrite
                 .bucketBy(num_buckets, col1, _to_seq(sm.sc, other_cols))
                 .sortBy(col1, _to_seq(sm.sc, other_cols))
                 .saveAsTable(table_name))
        else:
            params.logger.debug("Checkpointing to PARQUET %s...", name.lower())
            with sm.jobgroup("checkpointing {} to PARQUET".format(name.lower())):
                df.write.parquet(params.parquet_file_path, mode="overwrite")

        params.logger.debug("Checkpoint Finished")

        return df


checkpoint_resume = CheckpointResume()
"""A singleton checkpoint-resume object to be used throughout a spark session"""
