"""Test the auto-detection of HDFS presence.
"""
import mock
import os
try:
    from pathlib2 import Path
except Exception:
    from pathlib import Path

from spykfunc.utils.filesystem import AutoClient


CONFIG = """<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>{}</name>
    <value>myhost:myport</value>
  </property>
</configuration>
"""


def test_read_conf(tmpdir):
    """Verify reading configuration and getting the hostname
    """
    conf = Path(tmpdir) / 'conf' / 'hdfs-site.xml'
    conf.parent.mkdir(parents=True, exist_ok=True)
    with conf.open('w') as fd:
        fd.write(CONFIG.format("dfs.namenode.http-address"))
    with mock.patch.dict(os.environ, {'HADOOP_HOME': str(tmpdir)}):
        assert AutoClient._find_host() == 'myhost:myport'


def test_read_conf_default(tmpdir):
    """Verify reading a basic configuration returns HDFS default settings
    """
    conf = tmpdir / 'hdfs-site.xml'
    with conf.open('w') as fd:
        fd.write(CONFIG.format("dfs.namenode.http-address-fake"))
    with mock.patch.dict(os.environ, {'HADOOP_CONF_DIR': str(tmpdir)}):
        assert AutoClient._find_host() == 'localhost:50070'
