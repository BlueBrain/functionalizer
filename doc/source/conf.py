# -*- coding: utf-8 -*-
from importlib.metadata import version as get_version

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
    "h5py",
    "hdfs",
    "jprops",
    "lxml",
    "libsonata",
    "numpy",
    "pandas",
    "pyarrow",
    "pyspark",
    "pyspark.sql",
]

# source_suffix = ".rst"

# master_doc = "index"

project = "functionalizer"

version = get_version(project)
release = version

exclude_patterns = []

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = False

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []

html_theme = "sphinx-bluebrain-theme"
html_theme_options = {"metadata_distribution": "functionalizer"}
html_title = "functionalizer"
html_show_sourcelink = False
html_static_path = ['_static']
