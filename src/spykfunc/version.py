"""Versioning shim."""
from importlib.metadata import version as get_version

# `get_version` in a Spack development environment returns None
__version__ = version = get_version("spykfunc") or "unknown"
