"""Test job group setting interface only
"""
import sparkmanager as sm


def test_deco():
    """Test the decorator
    """
    sm.create("test")

    @sm.assign_to_jobgroup
    def some_function():
        rdd = sm.parallelize(range(10000))
        rdd.count()
    some_function()
