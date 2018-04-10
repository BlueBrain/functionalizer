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
    """ A CheckpointStatus object shall be passed to the decorator in order to retrieve information
    """
    INVALID = 0
    RESTORED_PARQUET = 1
    RESTORED_TABLE = 2
    COMPUTED = 3
    ERROR = -1

    def __init__(self):
        self.status = self.INVALID
        self.error = None


class CheckpointHandler:
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
        for h in cls.filter(handlers, handler_type):  # type: CheckpointHandler
            df = h(df)
        return df

    @classmethod
    def run_all(cls, handlers, handler_type):
        for h in cls.filter(handlers, handler_type):  # type: CheckpointHandler
            h()

    @classmethod
    def filter(cls, handlers, handler_type):
        return [h for h in handlers if h.type == handler_type]


class CheckpointResume:
    """ Class implementing checkpointing and restore, to parquet and parquet-based tables
    """

    class _RunParams:
        """ Parameters for a single checkpoint call, since the object is shared
        """
        dest = None
        overwrite = False
        break_exec_plan = True
        bucket_cols = False
        n_buckets = True
        handlers = None
        logger = get_logger("spykfunc.checkpoint")
        filename_suffix = ""
        status_obj = None

        def __init__(self, **opts):
            for name, value in opts.items():
                if value is not None:
                    setattr(self, name, value)

    _Runnable = namedtuple("_Runnable", ("f", "args", "kw"))

    # =========
    def __init__(self, directory=None, overwrite=False):
        self.directory = directory
        self.overwrite = overwrite
        self.last_status = None

    # decorator API definition
    def __decorator(self,
                    name,
                    dest=None,
                    overwrite=False,
                    break_exec_plan=True,
                    bucket_cols=False,
                    n_buckets=True,  # True -> Same nr partitions
                    handlers=None,
                    logger=None,
                    filename_suffix="",
                    status_obj=None):
        """Decorator for checkpointing_resume routines"""
        pass

    @wraps(__decorator)
    def __call__(self, name, **_kw):
        _params = self._RunParams(_kw)
        _params.dest = _kw.get('dest', self.directory)
        _params._status_obj = _kw.get('status', CheckpointStatus())

        def decorator(f):
            if isinstance(f, DataFrame):
                _params.overwrite = _kw.get('overwrite', self.overwrite)
                # When overwrite is True all subsequent checkpoints shall be overwritten
                if _params.overwrite:
                    self.overwrite = True

                return self._run(f, name, _params)

            @wraps(f)
            def new_f(*args, **kw):
                # Decorated function params might override behavior
                mode = kw['mode'] if 'mode' in signature(f).parameters \
                    else kw.pop('mode', None)
                if mode is not None:
                    _params.filename_suffix = mode

                _params.overwrite = kw['overwrite'] if 'overwrite' in signature(f).parameters \
                    else kw.pop('overwrite', self.overwrite)
                if _params.overwrite:
                    self.overwrite = True

                self._run(self._Runnable(f, args, kw), name, _params)

            return new_f
        return decorator

    # ---
    @classmethod
    def _run(cls, df, name, params):
        # type: (object, str, CheckpointResume._RunConfig) -> DataFrame
        """ Checkpoints a dataframe (internal)

        :param df: The df or the tuple with the calling info to create it
                   Note: We avoid creating the DF before since it might have intermediate implications
        :param name: The logical name of the dataframe
        :param params: The params of the current checkpoint_resume run
        """
        params.table_name = name.lower()
        params.table_path = osp.join(params.dest, params.table_name)
        if params.filename_suffix:
            params.table_path += '_' + str(params.filename_suffix) + ".ptable"
        params.parquet_file_path = params.table_path + ".parquet"

        # Attempt to load, unless overwrite is set to True
        if params.overwrite:
            if osp.exists(params.parquet_file_path) or osp.exists(params.table_path):
                params.logger.info("[OVERWRITE %s] Checkpoint found. Overwriting...", name)
        else:
            df = cls._try_restore(name, params)
            if df is not None:
                return df

        # Apply transformations
        if isinstance(df, cls._Runnable):
            df = df.f(*df.args, **df.kw)
            if df is not None:
                params.status_obj.status = CheckpointStatus.COMPUTED

        df = CheckpointHandler.apply_all(df, params.handlers, CheckpointHandler.POST_COMPUTE)

        try:
            df = cls._do_checkpoint(df, name, params)
        except Exception as e:
            params.status_obj.status = CheckpointStatus.ERROR
            params.status_obj.error = e
            params.logger.error("Checkpointing failed. Error: " + str(e))
            params.logger.warning("Attempting to continue without checkpoint")
        else:
            if params.break_exec_plan:
                cls._try_restore(name, params)

        return df

    # --
    @staticmethod
    def _try_restore(name, params):
        """ Tries to restore a dataframe, from table or raw parquet, according to the params object
        """
        df = None

        def try_except_restore(restore_f, source):
            CheckpointHandler.run_all(params.handlers, CheckpointHandler.BEFORE_LOAD)
            try:
                params.logger.info("[SKIP %s] Checkpoint found. Restoring state...", name)
                df = restore_f(source)
                params.status_obj.error = None
                return df
            except Exception as e:
                params.logger.warning("Could not load checkpoint from table. Reason: %s", str(e))
                params.status_obj.status = CheckpointStatus.ERROR
                params.status_obj.error = e
                return None

        # Attempting from table
        if params.bucket_cols and osp.isdir(params.table_path):
            df = try_except_restore(sm.read.table, params.table_name)
            if df is not None:
                params.status_obj.status = CheckpointStatus.RESTORED_TABLE

        # If no table, or error, try with direct parquet
        if df is None and osp.exists(params.parquet_file_path):
            df = try_except_restore(sm.read.parquet, params.parquet_file_path)
            if df is not None:
                params.status_obj.status = CheckpointStatus.RESTORED_PARQUET

        # All good? Run post handlers
        if df is not None:
            return CheckpointHandler.apply_all(df, params.handlers, CheckpointHandler.POST_RESUME)

        return df

    # --
    @staticmethod
    def _do_checkpoint(df, name, params):
        table_name = params.table_name
        bucket_cols = params.bucket_cols

        CheckpointHandler.apply_all(df, params.handlers, CheckpointHandler.BEFORE_SAVE)

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
