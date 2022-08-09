"""Checkpoint handling to save intermediate calculations."""
from collections import namedtuple
from functools import wraps
from funcsigs import signature
import pyspark.sql
from pyspark.sql.column import _to_seq
import sparkmanager as sm
from . import get_logger
from .filesystem import exists, isdir, size


class CheckpointStatus:
    """Status holding object.

    A CheckpointStatus object shall be passed to the decorator in order to retrieve information.

    Note: A state of error doesnt invalidate the dataframe, and resume is attempted.
    """

    INVALID = 0  # Not initialized
    RESTORED_PARQUET = 1  # Not computed, restored from parquet
    RESTORED_TABLE = 2  # Not computed, restored from a Table
    COMPUTED = 3  # Computed, no checkpoint available
    ERROR = -1  # CheckPoint not available and error creating/loading it

    def __init__(self):
        """Initialize the checkpoint status."""
        self.state = self.INVALID  # property: The Checkpointing end state
        self.error = None  # property: The Exception if any thrown during checkpointing"""


class CheckpointHandler:
    """A Handler for a checkpoint.

    A list of such handlers can be passed to CheckpointResume for any of the enumerated
    events.
    """

    BEFORE_LOAD = 0
    BEFORE_SAVE = 1
    POST_RESUME = 2
    POST_COMPUTE = 3

    def __init__(self, handler_type, handler_f):
        """Initialize the checkpoint handler.

        Args:
            handler_type: type of the handler.
            handler_f: function to apply.
        """
        self.type = handler_type
        self.f = handler_f

    def __call__(self, *args, **kwargs):
        """Apply the stored function."""
        return self.f(*args, **kwargs)

    @classmethod
    def apply_all(cls, df, handlers, handler_type):
        """Recursively applies all handlers matching a given type to the dataframe."""
        for h in cls.filter(handlers, handler_type):  # type: CheckpointHandler
            df = h(df)
        return df

    @classmethod
    def run_all(cls, handlers, handler_type):
        """Runs all the handlers which match a given type."""
        for h in cls.filter(handlers, handler_type):  # type: CheckpointHandler
            h()

    @classmethod
    def filter(cls, handlers, handler_type):
        """Returns the subset of handlers which match the given type."""
        return [h for h in handlers if h.type == handler_type]

    # Helpers
    before_load = classmethod(lambda cls, f: cls(cls.BEFORE_LOAD, f))
    before_save = classmethod(lambda cls, f: cls(cls.BEFORE_SAVE, f))
    post_resume = classmethod(lambda cls, f: cls(cls.POST_RESUME, f))
    post_compute = classmethod(lambda cls, f: cls(cls.POST_COMPUTE, f))


class CheckpointResume:
    """Class implementing checkpointing and restore, to parquet and parquet-based tables."""

    class _RunParams:
        """Parameters for a single checkpoint call, since the object is shared."""

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
        """Create a checkpoint wrapper.

        Args:
            directory: path to store checkpoints under
            overwrite: write over existing older checkpoints
        """
        self.directory = directory
        self.overwrite = overwrite
        self.last_status = None

    def __call__(
        self,
        name,
        dest=None,
        overwrite=False,
        break_exec_plan=True,
        bucket_cols=False,
        n_buckets=True,  # True -> Same nr partitions
        handlers=None,
        filename_suffix="",
        logger=None,
        status=None,
        child=None,
    ):
        """Decorator for checkpointing_resume routines.

        Args:
            name: The name of the checkpoint
            child: Will look for the attribute `_checkpoint_name` of the first argument
                and prepend it to name
            dest: The destination path for data files
            overwrite: If ``True`` will not attempt to resume and forces reevaluating `df`.
                Default: ``False``
            break_exec_plan: If ``True`` (default) will reload the saved data to break the
                execution plan
            bucket_cols: A tuple defining the columns to which partition the data.
                NOTE: This option activates storing as Table (default: ``False``)
            n_buckets: The number of partition buckets. Default: ``True``, which uses the
                `df` number of partitions NOTE: The number of buckets will multiply the
                number of output files if the df is not properly partitioned. Use this
                option (and `bucket_cols`) with caution, consider ``repartition()`` before
                handlers: A list of `CheckpointHandler` functions to run on respective
                checkpointing phases
            logger: A logger object. Defaults to the spykfunc default logger
            status: A `CheckPointStatus` object can be passed if checkpointing process
                information is desirable

        Returns:
            The checkpointed dataframe, built from the created files unless
            `break_exec_plan` was set ``False``
        """
        _dec_kw = {k: v for k, v in locals().items() if v is not None}
        _dec_kw.pop("self")
        _params = self._RunParams(**_dec_kw)
        if _params.status is None:
            _params.status = CheckpointStatus()

        def decorator(f):
            if isinstance(f, pyspark.sql.DataFrame):
                # Support for checkpointing a dataframe directly
                _params.overwrite = _dec_kw.get("overwrite", self.overwrite)
                _params.dest = _dec_kw.get("dest", self.directory)

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

                _params.overwrite = all_args.get("overwrite", self.overwrite)
                # If True then change the global default, so subsequent steps are recomputed
                if _params.overwrite:
                    self.overwrite = True

                _params.dest = _dec_kw.get(
                    "dest", self.directory
                )  # If None in decorator, use the current
                if _params.dest is None:
                    _params.logger.error("Checkpoints dir has not been set. Assuming _checkpoints.")
                    _params.dest = "_checkpoints"

                if child and hasattr(args[0], "_checkpoint_name"):
                    new_name = f"{args[0]._checkpoint_name}_{name}"
                else:
                    new_name = name

                return self._run(self._Runnable(f, args, kw), new_name, _params)

            return new_f

        return decorator

    # ---
    @classmethod
    def _run(cls, df, name, params):
        # type: (object, str, CheckpointResume._RunConfig) -> DataFrame
        """Checkpoints a dataframe (internal).

        :param df: The df or the tuple with the calling info to create it
            Note: We avoid creating the DF before since it might have intermediate implications
        :param name: The logical name of the dataframe checkpoint
        :param params: The params of the current checkpoint_resume run
        """
        params.table_name = name.lower()
        basename = "/".join([params.dest, params.table_name])
        if params.filename_suffix:
            basename += "_" + str(params.filename_suffix)
        params.table_path = basename + ".ptable"
        params.parquet_file_path = basename + ".parquet"

        # Attempt to load, unless overwrite is set to True
        if params.overwrite:
            if exists(params.parquet_file_path) or exists(params.table_path):
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
            sm.record({"checkpoints_size": size(params.dest)})
            if params.break_exec_plan:
                df = cls._try_restore(name, params, info=False)
            params.status.state = CheckpointStatus.COMPUTED

        except Exception as e:
            params.status.state = CheckpointStatus.ERROR
            params.status.error = e
            params.logger.error("Checkpointing failed. Error: " + str(e))
            params.logger.warning("Attempting to continue without checkpoint")

        return df

    # --
    @staticmethod
    def _try_restore(name, params, info=True):
        """Tries to restore a dataframe, from table or raw parquet, according to the params object.

        :param name: the name of the stage
        :param params: the checkpoint_restore session params
        :param bool info: log a message when restoring
        """
        df = None

        def try_except_restore(restore_f, source):
            CheckpointHandler.run_all(params.handlers, CheckpointHandler.BEFORE_LOAD)
            try:
                if info:
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
        if params.bucket_cols and isdir(params.table_path):
            df = try_except_restore(sm.read.table, params.table_name)
            if df is not None:
                params.status.state = CheckpointStatus.RESTORED_TABLE

        # If no table, or error, try with direct parquet
        if df is None and exists(params.parquet_file_path):
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
            with sm.jobgroup(f"checkpointing {table_name} to TABLE"):
                # For the moment limited support exists, we need intermediate Hive tables
                if isinstance(bucket_cols, (tuple, list)):
                    col1 = bucket_cols[0]
                    other_cols = bucket_cols[1:]
                else:
                    col1 = bucket_cols
                    other_cols = []

                num_buckets = (
                    df.rdd.getNumPartitions() if params.n_buckets is True else params.n_buckets
                )

                (
                    df.write.mode("overwrite")
                    .option("path", params.table_path)
                    ._jwrite.bucketBy(num_buckets, col1, _to_seq(sm.sc, other_cols))
                    .sortBy(col1, _to_seq(sm.sc, other_cols))
                    .saveAsTable(table_name)
                )
        else:
            params.logger.debug("Checkpointing to PARQUET %s...", name.lower())
            with sm.jobgroup(f"checkpointing {name.lower()} to PARQUET"):
                df.write.parquet(params.parquet_file_path, mode="overwrite")

        params.logger.debug("Checkpoint Finished")

        return df


checkpoint_resume = CheckpointResume()
"""A singleton checkpoint-resume object to be used throughout a spark session"""
