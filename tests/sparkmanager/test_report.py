"""Test timing report
"""
import json
import sparkmanager as sm


def test_report(tmpdir):
    """Test the decorator
    """
    filename = tmpdir.join("report")
    sm.create("test", report=str(filename), reset=True)

    @sm.assign_to_jobgroup
    def some_function():
        rdd = sm.parallelize(range(10000))
        rdd.count()
    some_function()

    with open(str(filename), 'r') as fd:
        data = json.load(fd)
    assert len(data['timing']) == 1


def test_report_resume(tmpdir):
    """Test the decorator
    """
    filename = tmpdir.join("report")
    with open(str(filename), 'w') as fd:
        json.dump({
            "timing": [[('foo', -1)], []]
        }, fd)

    sm.create("test", report=str(filename), reset=True)

    @sm.assign_to_jobgroup
    def some_function():
        rdd = sm.parallelize(range(10000))
        rdd.count()
    some_function()

    with open(str(filename), 'r') as fd:
        data = json.load(fd)
    assert len(data['timing']) == 3
