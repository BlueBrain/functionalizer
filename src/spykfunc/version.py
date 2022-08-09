"""Versioning shim."""
from importlib.metadata import version as get_version

__version__ = version = get_version("spykfunc")
