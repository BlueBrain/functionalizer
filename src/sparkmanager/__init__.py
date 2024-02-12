"""Convenience methods for a homogeneous Spark setup."""

import sys

from .manager import SparkManager

# All imports of `sparkmanager` will point to one instance
sys.modules[__name__] = SparkManager()
