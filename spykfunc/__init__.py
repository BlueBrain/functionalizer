################################################################################
# Copyright (C) 2017 Fernando Pereira <fernando.pereira@epfl.ch>
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
except:
        __version__ = 'unknown'


# General logging
_log.basicConfig(level=_log.WARN)


class Config:
    log_level = _log.DEBUG
