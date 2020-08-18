"""Convenience methods for a homogeneous Spark setup
"""
from __future__ import absolute_import

import sys

from .manager import SparkManager

# All imports of `sparkmanager` will point to one instance
sys.modules[__name__] = SparkManager()
