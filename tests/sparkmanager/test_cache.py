"""Small test to make sure that RDDs are deleted right.
"""
import sparkmanager as sm

from pyspark.sql import SQLContext


def test_unpersist():
    """Make sure that cached RDDs are unpersisted
    """
    sm.create("test")

    sql = SQLContext.getOrCreate(sm.sc)

    rdd1 = sm.parallelize(range(10000)).cache()
    rdd1.count()
    df1 = sql.createDataFrame([('Foo', 1)]).cache()
    df1.count()

    before = set(r.id() for r in sm.sc._jsc.getPersistentRDDs().values())

    with sm.clean_cache():
        rdd2 = sm.parallelize(range(0, 10000, 2))
        rdd2.cache()
        df2 = sql.createDataFrame([('Bar', 2)])
        df2.cache()

    assert before == set(r.id() for r in sm.sc._jsc.getPersistentRDDs().values())

    assert rdd1.getStorageLevel().useMemory is True
    assert rdd2.getStorageLevel().useMemory is False

    # FIXME Does not currently work!
    # assert df1.rdd.getStorageLevel().useMemory is True
    assert df2.rdd.getStorageLevel().useMemory is False


def test_reset():
    """Make sure that all caches are reset
    """
    sm.create("test")
    sm.reset_cache()

    sql = SQLContext.getOrCreate(sm.sc)

    assert len(sm.sc._jsc.getPersistentRDDs()) == 0

    rdd1 = sm.parallelize(range(10000))
    rdd1.count()
    rdd1.persist()
    df1 = sql.createDataFrame([('Foo', 1)])
    df1.count()
    df1.persist()

    assert len(sm.sc._jsc.getPersistentRDDs()) > 0

    sm.reset_cache()

    assert len(sm.sc._jsc.getPersistentRDDs()) == 0
