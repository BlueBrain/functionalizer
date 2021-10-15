# -*- coding: utf-8 -*-
import sys, os
from pkg_resources import get_distribution

# -- General configuration -----------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.ifconfig",
    "sphinx.ext.napoleon",
]

autodoc_default_options = {
    "ignore-module-all": True,
    "members": True,
    "show-inheritance": True,
    "special-members": "__call__",
}
autodoc_member_order = "groupwise"
autodoc_mock_imports = [
    "funcsigs",
    "h5py",
    "hdfs",
    "jprops",
    "lxml",
    "morphokit",
    "libsonata",
    "numpy",
    "pandas",
    "pyarrow",
    "pyspark",
    "pyspark.sql",
    "spykfunc.utils.checkpointing",  # avoid type confusion with pyspark
]

# source_suffix = ".rst"

# master_doc = "index"

project = "Spykfunc"

version = get_distribution("spykfunc").version
release = version

exclude_patterns = []

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = False

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []

html_theme = "sphinx-bluebrain-theme"
html_theme_options = {"metadata_distribution": "spykfunc"}
html_title = "Spykfunc"
html_show_sourcelink = False
