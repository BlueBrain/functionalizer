from os import path as osp
from collections import namedtuple
from contextlib import contextmanager
from functools import update_wrapper
from pyspark.sql.column import _to_seq
from . import get_logger

import sparkmanager as sm

class defaults(object):
    directory = None
    """:property: to be set by the main executing branch"""

    overwrite = False
    """:property: do not restore from checkpoints"""


# -----------------------------------------------
# Conditional execution decorator
# -----------------------------------------------
def checkpoint_resume(name,
                      dest=None,
                      break_exec_plan=True,
                      logger=get_logger("spykfunc.checkpoint"),
                      before_load_handler=None,
                      before_save_handler=None,
                      post_resume_handler=None,
                      post_compute_handler=None,
                      bucket_cols=False,
                      n_buckets=True):
    """Checkpoint a table in the execution flow

    :param before_save_handler: transformation to be applied to the
                                dataframe before saving to disk
    """
    def decorator(f):
        def new_f(*args, **kw):
            mode = kw.pop("mode", None)
            table_path = osp.join(dest or defaults.directory, name.lower())
            if mode:
                table_path += '_' + mode
            parquet_file_path = table_path + ".parquet"
            table_name = name.lower()


            # Attempt to load, unless overwrite is set to True
            if defaults.overwrite or kw.pop('overwrite', False):
                if osp.exists(parquet_file_path) or osp.exists(table_path):
                    logger.info("[OVERWRITE %s] Checkpoint found. Overwriting...", name)
            else:
                if osp.exists(parquet_file_path):
                    try:
                        if before_load_handler:
                            before_load_handler()
                        df = sm.read.parquet(parquet_file_path)
                        logger.info("[SKIP %s] Checkpoint found. Restoring state...", name)
                        if post_resume_handler:
                            return post_resume_handler(df)
                        return df
                    except Exception as e:
                        logger.warning("Could not load checkpoint. Reason: %s", str(e))

                # Attempting from table
                if bucket_cols and osp.isdir(table_path):
                    try:
                        df = sm.read.table(table_name)
                        logger.info("[SKIP %s] Checkpoint found. Restoring state...", name)
                        if post_resume_handler:
                            return post_resume_handler(df)
                        return df
                    except Exception as e:
                        logger.warning("Could not load checkpoint from table. Reason: %s", str(e))

            # Apply Tranformations
            df = f(*args, **kw)
            df_to_save = before_save_handler(df) if before_save_handler else df

            # We changed data, the rest of the pipeline should overwrite
            # any other saved checkpoints!
            defaults.overwrite = True

            if bucket_cols:
                logger.debug("Checkpointing to TABLE %s...", table_name)
                with sm.jobgroup("checkpointing {} to TABLE".format(table_name)):
                    # For the moment limited support exists, we need intermediate Hive tables
                    if isinstance(bucket_cols, (tuple, list)):
                        col1 = bucket_cols[0]
                        other_cols = bucket_cols[1:]
                    else:
                        col1 = bucket_cols
                        other_cols = []

                    if n_buckets is True:
                        num_buckets = df.rdd.getNumPartitions()
                    else:
                        num_buckets = n_buckets

                    (df_to_save
                     .write.mode("overwrite").option("path", table_path)._jwrite
                     .bucketBy(num_buckets, col1, _to_seq(sm.sc, other_cols))
                     .sortBy(col1, _to_seq(sm.sc, other_cols))
                     .saveAsTable(table_name))
            else:
                logger.debug("Checkpointing to PARQUET %s...", name.lower())
                with sm.jobgroup("checkpointing {} to PARQUET".format(name.lower())):
                    df_to_save.write.parquet(parquet_file_path, mode="overwrite")

            logger.debug("Checkpoint Finished")

            if break_exec_plan:
                if before_load_handler: before_load_handler()
                with sm.jobgroup("restoring checkpoint " + name.lower()):
                    if bucket_cols:
                        df = sm.read.table(table_name)
                    else:
                        df = sm.read.parquet(parquet_file_path)
                
            if post_compute_handler:
                return post_compute_handler(df)

            return df

        return update_wrapper(new_f, f)
    return decorator


@contextmanager
def number_shuffle_partitions(np):
    previous_np = int(sm.conf.get("spark.sql.shuffle.partitions"))
    sm.conf.set("spark.sql.shuffle.partitions", np)
    yield
    sm.conf.set("spark.sql.shuffle.partitions", previous_np)
