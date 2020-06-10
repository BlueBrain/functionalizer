# -*- coding: utf-8 -*-
#
import sys, os

sys.path.insert(0, os.path.abspath('..'))

# -- General configuration -----------------------------------------------------
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx', 'sphinx.ext.todo',
              'sphinx.ext.autosummary', 'sphinx.ext.viewcode', 'sphinx.ext.coverage',
              'sphinx.ext.doctest', 'sphinx.ext.ifconfig',
              'sphinx.ext.napoleon']

autodoc_mock_imports = ['pyspark', 'pyspark.sql', 'sparkmanager', 'spykfunc.schema', 'spykfunc.utils.checkpointing']

source_suffix = '.rst'

master_doc = 'index'

# General information about the project.
project = u'Spykfunc'

version = ''  # Is set by calling `setup.py docs`
release = ''  # Is set by calling `setup.py docs`

exclude_patterns = ['_build']

# The reST default role (used for this markup: `text`) to use for all documents.
# default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
# add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []

html_theme = 'sphinx-bluebrain-theme'
html_title = "Spykfunc"

try:
    from spykfunc import __version__ as version
except ImportError:
    pass
else:
    release = version

html_static_path = ['_static']
htmlhelp_basename = 'spykfunc-doc'

# -- External mapping ------------------------------------------------------------
python_version = '.'.join(map(str, sys.version_info[0:2]))
intersphinx_mapping = {
    'sphinx': ('http://sphinx.pocoo.org', None),
    'python': ('http://docs.python.org/' + python_version, None),
    'matplotlib': ('http://matplotlib.sourceforge.net', None),
    'numpy': ('http://docs.scipy.org/doc/numpy', None),
    'sklearn': ('http://scikit-learn.org/stable', None),
    'pandas': ('http://pandas.pydata.org/pandas-docs/stable', None),
    'scipy': ('http://docs.scipy.org/doc/scipy/reference/', None),
}
