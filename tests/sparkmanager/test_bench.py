"""Test benchmark functionality
"""
import sparkmanager as sm


def test_bench():
    """Test the benchmarking
    """
    sm.create("test")

    with sm.benchmark():
        rdd = sm.parallelize(range(10000))
        rdd.count()
