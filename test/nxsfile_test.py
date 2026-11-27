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
# \package test nexdatas
# \file nxsfile_test.py
# unittests for the NXSFile API
#
import unittest
import os
import sys
import time
import pathlib
import tempfile
import shutil

import numpy as np

from nxsblisswriter.NXSFile import (
    first,
    NXSFile,
    create_nexus_file,
    ALLOWED_NXS_SURFIXES,
    PTH,
)


if sys.version_info > (3,):
    unicode = str
    long = int


# minimal blissdata-scan replacement exposing only the ``info`` mapping
class FakeScan(object):

    # constructor
    # \param info scan info dictionary
    # \param number scan number
    def __init__(self, info, number=1):
        #: (:obj:`dict`) scan info
        self.info = info
        #: (:obj:`int`) scan number
        self.number = number
        #: (:obj:`dict`) scan streams
        self.streams = {}


# minimal tango-like stream set swallowing log messages
class FakeStreams(object):

    def error(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


# test fixture
class NXSFileTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.seed = long(time.time() * 256)  # use fractional seconds

        self.__streams = FakeStreams()

    # test starter
    # \brief Common set up
    def setUp(self):
        print("\nsetting up...")
        print("SEED = %s" % self.seed)

    # test closer
    # \brief Common tear down
    def tearDown(self):
        print("tearing down ...")

    # Exception tester
    # \param exception expected exception
    # \param method called method
    # \param args list with method arguments
    # \param kwargs dictionary with method arguments
    def myAssertRaise(self, exception, method, *args, **kwargs):
        try:
            error = False
            method(*args, **kwargs)
        except Exception:
            error = True
        self.assertEqual(error, True)

    # first helper test
    # \brief It tests the first() module function
    def test_first(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        # a one-element array returns its only element
        self.assertEqual(first(np.array([5])), 5)
        # a multi-element array is returned unchanged
        multi = np.array([1, 2, 3])
        self.assertTrue(np.array_equal(first(multi), multi))
        # a non-array value is returned unchanged
        self.assertEqual(first(7), 7)
        self.assertEqual(first("text"), "text")

    # module constants test
    # \brief It tests the module level type tables
    def test_constants(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        for suffix in [".nxs", ".h5", ".hdf5", ".nx"]:
            self.assertTrue(suffix in ALLOWED_NXS_SURFIXES)
        for dtype in ["int64", "int32", "float64", "float32", "str"]:
            self.assertTrue(dtype in PTH)

    # links with datasources test
    # \brief It tests links with targets provided by datasources (#70)
    def test_datasource_links(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))
        import h5py

        tmpdir = tempfile.mkdtemp()
        fpath = pathlib.Path(tmpdir) / "links.nxs"
        extfile = os.path.join(tmpdir, "lima_0000.h5")
        with h5py.File(extfile, "w") as fl:
            fl.create_dataset("entry_0000/measurement/data", data=[1, 2, 3])

        xml = """<definition>
<group type="NXentry" name="scan_00012">
 <group type="NXinstrument" name="instrument">
  <group type="NXdetector" name="pilatus">
   <field name="counts" type="NX_INT32"/>
  </group>
 </group>
 <group type="NXdata" name="data">
  <link name="static" target="/scan_00012/instrument/pilatus/counts"/>
  <link name="init_link">
   <strategy mode="INIT"/>
   <datasource type="TANGO" name="det_path">
    <device name="p/det/1" member="attribute"/>
    <record name="DetPath"/>
   </datasource>
  </link>
  <link name="final_link">
   <strategy mode="FINAL"/>
   <datasource type="CLIENT" name="lima_file">
    <record name="lima_file"/>
   </datasource>
  </link>
  <link name="fallback" target="/scan_00012/instrument/pilatus/counts">
   <strategy mode="INIT"/>
   <datasource type="CLIENT" name="missing">
    <record name="missing"/>
   </datasource>
  </link>
  <link name="no_target">
   <strategy mode="INIT"/>
   <datasource type="CLIENT" name="missing2">
    <record name="missing2"/>
   </datasource>
  </link>
 </group>
</group>
</definition>"""
        info = {
            "filename": str(fpath),
            "snapshot": {
                "nxsdatawriter_xmlsettings": {"value": xml},
                "det_path": {"value": "/NXentry/instrument/pilatus/counts",
                             "strategy": "INIT"},
                "lima_file": {
                    "value": "%s://entry_0000/measurement/data" % extfile,
                    "strategy": "FINAL"},
            },
        }
        nxfl = NXSFile(
            FakeScan(info), fpath, self.__streams,
            "/scan_00012:NXentry/instrument:NXinstrument/collection")
        try:
            nxfl.create_file_structure()
            nxfl.write_init_snapshot()
            with h5py.File(str(fpath), "r") as fl:
                data = fl["scan_00012/data"]
                self.assertEqual(
                    data.get("init_link", getlink=True).path,
                    "/scan_00012/instrument/pilatus/counts")
                self.assertEqual(
                    data.get("fallback", getlink=True).path,
                    "/scan_00012/instrument/pilatus/counts")
                self.assertEqual(
                    data.get("static", getlink=True).path,
                    "/scan_00012/instrument/pilatus/counts")
                # FINAL links are created at the end of the scan
                self.assertTrue("final_link" not in data)
                self.assertTrue("no_target" not in data)
            nxfl.write_final_snapshot()
            nxfl.close()
            with h5py.File(str(fpath), "r") as fl:
                lnk = fl["scan_00012/data"].get("final_link", getlink=True)
                self.assertTrue(isinstance(lnk, h5py.ExternalLink))
                self.assertEqual(lnk.filename, extfile)
                self.assertEqual(lnk.path, "/entry_0000/measurement/data")
                self.assertEqual(
                    list(fl["scan_00012/data/final_link"][()]), [1, 2, 3])
                self.assertTrue("no_target" not in fl["scan_00012/data"])
        finally:
            shutil.rmtree(tmpdir)

    # channels/labels test
    # \brief It tests the channels and labels cached properties
    def test_channels_labels(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        info = {"datadesc": {
            "c1": {"label": "det one", "name": "d1"},
            "c2": {"label": "mot two", "name": "d2"},
        }}
        nxfl = NXSFile(FakeScan(info), "dummy.nxs", self.__streams)

        self.assertEqual(len(nxfl.channels), 2)
        # spaces in labels are replaced by underscores
        self.assertEqual(nxfl.labels, ("det_one", "mot_two"))
        # the properties are cached - the same object is returned
        self.assertTrue(nxfl.channels is nxfl.channels)
        self.assertTrue(nxfl.labels is nxfl.labels)

    # file structure test
    # \brief It tests create_file_structure and field creation and close
    def test_create_file_structure(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        fd, fname = tempfile.mkstemp(suffix=".nxs")
        os.close(fd)
        os.remove(fname)
        fpath = pathlib.Path(fname)

        # an empty snapshot creates an empty (truncated) nexus file
        scan = FakeScan({"snapshot": {}})
        nxfl = NXSFile(scan, fpath, self.__streams)
        try:
            nxfl.create_file_structure()
            self.assertTrue(fpath.exists())

            from pninexus import h5cpp
            root = nxfl._NXSFile__mfile.root()

            # a scalar string field keeps its value
            grp = h5cpp.node.Group(root, "entry")
            title = nxfl.create_field(grp, "title", "str", value="hello")
            self.assertTrue("hello" in list(title.read()))

            # an extensible integer field starts empty
            data = nxfl.create_field(
                grp, "data", "int64", value=None, shape=[0], chunk=[1])
            self.assertEqual(
                tuple(data.dataspace.current_dimensions), (0,))

            # create_groupfield builds the intermediate NX groups
            mca = nxfl.create_groupfield(
                root,
                ["entry:NXentry", "instrument:NXinstrument", "mca"],
                "float64", value=None, shape=[0, 10], chunk=[1, 10])
            self.assertEqual(len(mca.dataspace.current_dimensions), 2)

            nxfl.close()
            # the file remains on disk, the handle is released
            self.assertTrue(fpath.exists())
            self.assertTrue(nxfl._NXSFile__mfile is None)
        finally:
            if fpath.exists():
                os.remove(fpath)

    # append mode test - regression test for issue #77
    # \brief create_nexus_file opens an existing file so that root exists
    def test_create_nexus_file_append_existing(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        tdir = tempfile.mkdtemp()
        fpath = pathlib.Path(tdir) / "scan.nxs"
        scan = FakeScan({"filename": str(fpath), "snapshot": {}})
        try:
            # the first call creates the file structure
            nxfl1 = create_nexus_file(scan, self.__streams)
            self.assertTrue(nxfl1._NXSFile__mfile is not None)
            nxfl1.close()
            self.assertTrue(fpath.exists())

            # the second call must open the already existing file so that
            # its root is available - without the fix __mfile stayed None
            # and write_init_snapshot() raised
            # "'NoneType' object has no attribute 'root'"
            nxfl2 = create_nexus_file(scan, self.__streams)
            self.assertTrue(nxfl2._NXSFile__mfile is not None)
            nxfl2.write_init_snapshot()
            nxfl2.close()
        finally:
            if fpath.exists():
                os.remove(fpath)
            os.rmdir(tdir)


if __name__ == '__main__':
    unittest.main()
