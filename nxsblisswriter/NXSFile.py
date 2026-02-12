#!/usr/bin/env python
#   This file is part of nexdatas - Tango Server for NeXus data writer
#
#    Copyright (C) 2026 DESY, Jan Kotanski <jkotan@mail.desy.de>
#
#    nexdatas is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    nexdatas is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with nexdatas.  If not, see <http://www.gnu.org/licenses/>.
#

""" Provides the access to a database with NDTS configuration files """

# import time
import pathlib

# from blissdata.redis_engine.store import DataStore
# from blissdata.redis_engine.scan import ScanState
# from blissdata.redis_engine.exceptions import EndOfStream
# from blissdata.redis_engine.exceptions import NoScanAvailable


ALLOWED_NXS_SURFIXES = {".nxs", ".h5", ".hdf5", ".nx"}


def create_nexus_file(scan):
    """ open nexus file

    :param scan: blissdata scan
    :type scan: :obj:`blissdata.redis_engine.scan.Scan`
    :returns: nexus file object
    :rtype: :obj:`NXSFile`
    """
    fpath = pathlib.Path(scan.info["filename"])
    if fpath.suffix not in ALLOWED_NXS_SURFIXES:
        return None

    fdir = fpath.parent
    if not fdir.is_dir():
        fdir.mkdir(parents=True)

    nxsfl = NXSFile(fpath, scan)
    # ?? append mode
    if not fpath.exists():
        nxsfl.create_file_structure()
    return nxsfl


class NXSFile:
    def __init__(self, fpath, scan):
        """ constructor

        :param fpath:  NeXus path and file name
        :type fpath:  :obj:`pathlib.Path`
        :param scan: blissdata scan
        :type scan: :obj:`blissdata.redis_engine.scan.Scan`
        """
        self.scan = scan
        self.fpath = fpath

    def create_file_structure():
        pass

    def write_init_snapshot():
        pass

    def write_scan_points():
        pass

    def write_final_snapshot():
        pass
