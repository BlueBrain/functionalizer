from os import path as osp
from functools import update_wrapper
from pyspark.sql import SparkSession
from pyspark.sql.column import _to_seq
from . import get_logger

spark = None


# -----------------------------------------------
# Conditional execution decorator
# -----------------------------------------------
def checkpoint_resume(name,
                      dest="_checkpoints",
                      break_exec_plan=True, 
                      logger=get_logger("spykfunc.checkpoint"),
                      before_load_handler=None,
                      post_resume_handler=None,
                      post_compute_handler=None,
                      bucket_cols=False,
                      n_buckets=True,
                      bucket_sorting=True):
    table_path = osp.join(dest, name.lower())
    parquet_file_path = table_path + ".parquet"
    table_name = name.lower()

    def decorator(f):
        def new_f(*args, **kw):
            global spark
            if spark is None:
                spark = SparkSession.builder.getOrCreate()

            # Attempt loading from checkpoint
            if not kw.pop("overwrite", False):
                if osp.exists(parquet_file_path):
                    logger.info("[SKIP %s] Checkpoint found. Restoring state...", name)
                    try:
                        if before_load_handler: before_load_handler()
                        df = spark.read.parquet(parquet_file_path)
                        if post_resume_handler:
                            return post_resume_handler(df)
                        return df
                    except Exception as e:
                        logger.warning("Could not load checkpoint. Reason: %s", str(e))

                # Attempting from table
                if bucket_cols and osp.isdir(table_path):
                    try:
                        df = spark.read.table(table_name)
                        if post_resume_handler:
                            return post_resume_handler(df)
                        return df
                    except Exception as e:
                        logger.warning("Could not load checkpoint from table. Reason: %s", str(e))
            
            # Apply Tranformations
            df = f(*args, **kw)
            
            if bucket_cols:
                logger.debug("Checkpointing to TABLE %s...", table_name)
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

                bucketed_jdf = (df
                 .write.mode("overwrite").option("path", table_path)._jwrite
                 .bucketBy(num_buckets, col1, _to_seq(spark._sc, other_cols))
                )
                if bucket_sorting:
                    bucketed_jdf.sortBy(col1, _to_seq(spark._sc, other_cols)).saveAsTable(table_name)
                else:
                    bucketed_jdf.saveAsTable(table_name)
            else:
                logger.debug("Checkpointing to PARQUET %s...", name.lower())
                df.write.parquet(parquet_file_path, mode="overwrite")

            logger.debug("Checkpoint Finished")

            if break_exec_plan:
                if before_load_handler: before_load_handler()
                if bucket_cols:
                    df = spark.read.table(table_name)
                else:
                    df = spark.read.parquet(parquet_file_path)
                
            if post_compute_handler:
                return post_compute_handler(df)

            return df

        return update_wrapper(new_f, f)
    return decorator


def reduce_number_shuffle_partitions(df, factor, min_=100, max_=1000):
    cur_n_parts = df.rdd.getNumPartitions()
    cur_n_parts = ((cur_n_parts-1) // 100 + 1) * 100  # Avoid strange numbers
    return df.coalesce(max(min_, min(max_, cur_n_parts//factor)))

