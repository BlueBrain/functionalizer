import os
from functools import update_wrapper
from pyspark.sql import SparkSession
from . import get_logger

spark = None


# -----------------------------------------------
# Conditional execution decorator
# -----------------------------------------------
def checkpoint_resume(filename, step_name, dest="_checkpoints", 
                      break_exec_plan=True, 
                      logger=get_logger("spykfunc.checkpoint"),
                      before_load_handler=None,
                      post_resume_handler=None,
                      post_compute_handler=None):
    file_path = os.path.join(dest, filename)
    
    def decorator(f):
        def new_f(*args, **kw):
            global spark
            if spark is None:
                spark = SparkSession.builder.getOrCreate()

            if not kw.pop("overwrite", False):
                if os.path.exists(file_path):
                    logger.info("[SKIP %s] Checkpoint found. Restoring state...", step_name)
                    try:
                        if before_load_handler: before_load_handler()
                        df = spark.read.parquet(file_path)
                        if post_resume_handler:
                            return post_resume_handler(df)
                        return df
                    except Exception as e:
                        logger.warning("Could not load checkpoint. Reason: %s", str(e))                        
            
            # Apply Tranformations
            df = f(*args, **kw)
            
            logger.debug("Checkpointing to %s...", filename)
            df.write.parquet(file_path, mode="overwrite")
            logger.debug("Checkpoint Finished")

                       
            if break_exec_plan:
                if before_load_handler: before_load_handler()
                df = spark.read.parquet(file_path)
                
            if post_compute_handler:
                return post_compute_handler(df)

            return df

        return update_wrapper(new_f, f)
    return decorator
