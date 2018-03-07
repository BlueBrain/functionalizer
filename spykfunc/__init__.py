################################################################################
# Copyright (C) 2017 EPFL - Blue Brain Project
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
################################################################################

""" Spykfunc - An implementation of Functionalizer in PySpark
"""

from __future__ import absolute_import as _, print_function as _, division as _
import pkg_resources as _pkg
import logging as _log

try:
    __version__ = _pkg.get_distribution(__name__).version
except Exception:
    __version__ = 'unknown'


# General logging
_log.basicConfig(level=_log.WARN)


class config:
    log_level = _log.DEBUG


# ####################################################################################################
# Shortcuts (mind the import must be done inside, as we want to allow importing spykfunc without spark)
######################################################################################################
def functionalizer_new():
    """ Direct Functionalizer object
    """
    from .functionalizer import Functionalizer
    return Functionalizer()


def session(recipe, mvd_file, touch_files, **opts):
    """ Creates and Initializes a Functionalizer session

    :returns: A :py:class:`~spykfunc.Functionalizer` instance
    """
    from .commands import arg_parser
    from .functionalizer import session
    args = (recipe, mvd_file, ".")
    if isinstance(touch_files, str):
        args = args + (touch_files,)
    else:
        args += tuple(touch_files)
    
    # Extract options that dont take arguments
    if opts.pop("s2s", False):
        args += ("--s2s",)
    if opts.pop("format-hdf5", False):
        args += ("--format-hdf5",)

    for opt, opt_val in opts.items():
        args += ("--" + opt.replace("_", "-"), opt_val)
    opts = arg_parser.parse_args(args)
    return session(opts)
