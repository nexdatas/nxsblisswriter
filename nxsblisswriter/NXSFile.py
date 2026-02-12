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
from pninexus import nexus, h5cpp
import xml.etree.ElementTree as et
from lxml.etree import XMLParser
from lxml import etree
import numpy as np
import json

# from blissdata.redis_engine.store import DataStore
# from blissdata.redis_engine.scan import ScanState
# from blissdata.redis_engine.exceptions import EndOfStream
# from blissdata.redis_engine.exceptions import NoScanAvailable


ALLOWED_NXS_SURFIXES = {".nxs", ".h5", ".hdf5", ".nx"}

try:
    _npver = np.version.version.split(".")
    NPMAJOR = int(_npver[0])
except Exception:
    NPMAJOR = 1


pTh = {
    "long": h5cpp.datatype.Integer,
    "str": h5cpp.datatype.kVariableString,
    "unicode": h5cpp.datatype.kVariableString,
    "bool": h5cpp.datatype.kEBool,
    "int": h5cpp.datatype.kInt64,
    "int64": h5cpp.datatype.kInt64,
    "int32": h5cpp.datatype.kInt32,
    "int16": h5cpp.datatype.kInt16,
    "int8": h5cpp.datatype.kInt8,
    "uint": h5cpp.datatype.kInt64,
    "uint64": h5cpp.datatype.kUInt64,
    "uint32": h5cpp.datatype.kUInt32,
    "uint16": h5cpp.datatype.kUInt16,
    "uint8": h5cpp.datatype.kUInt8,
    "float": h5cpp.datatype.kFloat32,
    "float64": h5cpp.datatype.kFloat64,
    "float32": h5cpp.datatype.kFloat32,
    "string": h5cpp.datatype.kVariableString,
}


def create_field(grp, name, dtype, value):
    """ create field
    :param n: group name
    :type n: :obj:`str`
    :param type_code: nexus field type
    :type type_code: :obj:`str`
    :param shape: shape
    :type shape: :obj:`list` < :obj:`int` >
    :param chunk: chunk
    :type chunk: :obj:`list` < :obj:`int` >
    :param dfilter: filter deflater
    :type dfilter: :class:`H5CppDataFilter`
    :returns: file tree field
    :rtype: :class:`H5CppField`
    """
    dcpl = h5cpp.property.DatasetCreationList()
    shape = None
    if hasattr(value, "shape"):
        shape = value.shape
    elif dtype in ["str", "unicode", "string"]:
        dataspace = h5cpp.dataspace.Scalar()
        field = h5cpp.node.Dataset(
            grp, h5cpp.Path(name), pTh[dtype], dataspace,
            dcpl=dcpl)
        field.write(value)
        return field
    shape = shape or [1]
    dataspace = h5cpp.dataspace.Simple(
        tuple(shape), tuple([h5cpp.dataspace.UNLIMITED] * len(shape)))
    chunk = [(dm if dm != 0 else 1) for dm in shape]
    dcpl.layout = h5cpp.property.DatasetLayout.CHUNKED
    dcpl.chunk = tuple(chunk)
    field = h5cpp.node.Dataset(
        grp, h5cpp.Path(name), pTh[dtype], dataspace, dcpl=dcpl)
    field.write(value)
    return field


attrdesc = {
    "nexus_type": "type",
    "unit": "units",
    "depends_on": "depends_on",
    "trans_type": "transformation_type",
    "trans_vector": "vector",
    "trans_offset": "offset",
    "source": "nexdatas_source",
    "strategy": "nexdatas_strategy",
}


noattrs = {"name", "label", "dtype", "value", "nexus_path"}


def first(array):
    """  get first element if the only

    :param array: numpy array
    :type array: :class:`numpy.ndarray`
    :returns: first element of the array
    :type array: :obj:`any`
    """
    try:
        if isinstance(array, np.ndarray) and len(array) == 1:
            return array[0]
    except Exception:
        try:
            if hasattr(array, "all"):
                if NPMAJOR < 2:
                    array = array.all()
                if hasattr(array, "decode"):
                    return array.decode()
        except Exception:
            pass
    return array


def write_attr(am, name, dtype, value, item=None):
    try:
        at = am[name]
        # print("TYPE",name, value, at.dataspace.type)
    except Exception:  # as ae:
        #  print(ae)
        at = None
    if at is None:
        try:
            vshape = None
            if isinstance(value, list):
                vshape = np.array(value).shape
            elif hasattr(value, "shape"):
                vshape = value.shape
            if not vshape:
                at = am.create(name, pTh[str(dtype)])
            else:
                at = am.create(name, pTh[str(dtype)], vshape)
                print("VHASPE", vshape, name, value)
        except Exception as e:
            print("CREATE ATT", name, dtype, pTh[dtype])
            print("WWAA", am, name, dtype, value, type(value), item)
            print(str(e))
    try:
        if at is not None:
            try:
                rvalue = at.read()
                if at.dataspace.type == h5cpp.dataspace.Type.SCALAR:
                    rvalue = first(rvalue)
                if str(rvalue) != str(value):
                    at.write(value)
                    # print("WRITE", am, name, dtype, value,
                    #       type(value), rvalue)
                    # print("DIFF", name, str(value), str(rvalue))
                else:
                    pass
                    # print("THE SAME", name, value)
            except Exception as e:
                print(str(e))
                # print("at", at.read(), dir(at))
                shape = None
                if hasattr(at.dataspace, "current_dimensions"):
                    shape = at.dataspace.current_dimensions
                if shape:
                    if not isinstance(value, list) and \
                            not hasattr(value, "shape"):
                        value = json.loads(value)
                at.write(value)
    except Exception as e:
        # print("READ", at.read())
        print(str(e))
        # print("WW", am, name, dtype, value, item)
        # print(at, dir(at))


def write_snapshot_item(root, item):
    nxpath = item.get('nexus_path', None)
    value = item.get('value', None)
    dtype = item.get('dtype', None)
    if dtype == "string":
        dtype = "str"
    dataset = None
    if nxpath and value is not None:
        attr = None
        if "@" in nxpath:
            nxpath, attr = nxpath.split("@", 1)
        try:
            lnxpath = nxpath.split("/")
            h5path = "/".join([nd.split(":")[0] for nd in lnxpath])
            if not attr:
                dataset = root.get_dataset(h5path)
                if dataset.dataspace.type != \
                        h5cpp.dataspace.Type.SCALAR:
                    if not dataset.dataspace.size:
                        try:
                            dataset.extent(0, 1)
                        except Exception:
                            pass
                dataset.write(value)
            else:
                if ":" not in lnxpath:
                    try:
                        adataset = root.get_dataset(h5path)
                        am = adataset.attributes
                    except Exception:
                        group = root.get_group(h5path)
                        am = group.attributes
                else:
                    group = root.get_group(h5path)
                    am = group.attributes
                write_attr(am, attr, dtype, value)

        except Exception as e:
            # print(ds, item)
            if str(e).startswith("No node ["):
                grp = root
                for gr in lnxpath[:-1]:
                    gn = gr
                    gt = None
                    if ":" in gr:
                        gn, gt = gr.split(":")

                    if grp.has_group(gn):
                        grp = grp.get_group(gn)
                    else:
                        grp = h5cpp.node.Group(grp, gn)
                        if gt is not None:
                            grp.attributes.create(
                                "NX_class",
                                h5cpp.datatype.kVariableString).write(gt)
                    # print(gn)
                name = lnxpath[-1]
                if isinstance(value, list):
                    value = np.array(value, dtype=dtype)
                print("CREATE %s (%s)" % (nxpath, dtype))
                dataset = create_field(grp, name, dtype, value)
            else:
                print("ERRPOR", str(e), type(e))
                raise
    if dataset is not None:
        attrs = set(item.keys()) - noattrs
        am = dataset.attributes
        for anm in attrs:
            avl = item[anm]
            dtp = str(type(avl).__name__)
            nanm = attrdesc.get(anm, anm)
            write_attr(am, nanm, dtp, avl, item)


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

    nxsfl = NXSFile(scan)
    # ?? append mode
    if not fpath.exists():
        nxsfl.create_file_structure()
    return nxsfl


class NXSFile:
    def __init__(self, scan, fpath=None):
        """ constructor

        :param scan: blissdata scan
        :type scan: :obj:`blissdata.redis_engine.scan.Scan`
        """
        self.__scan = scan
        self.__mfile = None

    def create_file_structure(self):
        """ create nexus structure
        """
        si = self.__scan.info
        filename = si["filename"]
        snapshot = {}
        if "snapshot" in si:
            snapshot = si["snapshot"]
        xmls = None
        try:
            xmlc = snapshot["nxsdatawriter_xmlsettings"][0]["value"]
        except Exception as e:
            print(str(e))
            xmlc = None
        if xmlc:
            xmlc1 = xmlc.replace('"NX_DATE_TIME"', '"NX_CHAR"')
            xmlc2 = xmlc1.replace('index="1"', 'index="0"').replace(
                'index="2"', 'index="1"').replace('index="3"', 'index="2"')
            etroot = et.fromstring(
                xmlc2, parser=XMLParser(collect_ids=False))
            etdims = etroot.findall(".//dimensions")
            for etdim in etdims:
                dparent = etdim.getparent()
                if dparent.tag in ["field", "vds"]:
                    ddparent = dparent.getparent()
                    ddparent.remove(dparent)
            xmls = etree.tostring(etroot, encoding='unicode',
                                  method='xml', pretty_print=True)
        self.__mfile = nexus.create_file(filename,
                                         h5cpp.file.AccessFlags.TRUNCATE)
        root = self.__mfile.root()
        if xmls:
            nexus.create_from_string(root, xmls)

    def write_init_snapshot(self):
        """ write init data
        """
        si = self.__scan.info
        root = self.__mfile.root()
        snapshot = {}
        if "snapshot" in si:
            snapshot = si["snapshot"]
        for ds, items in snapshot.items():
            for item in items:
                strategy = None
                if "strategy" in item:
                    strategy = item["strategy"]
                if not strategy or strategy in ["INIT"]:
                    try:
                        print("WRITE", ds, strategy)
                        write_snapshot_item(root, item)
                    except Exception as e:
                        print("WWW", str(e))
                        break
            else:
                continue
            break

    def write_scan_points(self):
        """ write step data
        """
        si = self.__scan.info
        scsts = self.__scan.streams
        for snm in scsts.keys():
            item = None
            if snm in si["datadesc"]:
                item = si["datadesc"][snm]
                print(type(item[0]))
        # print(si["datadesc"])

    def write_final_snapshot(self):
        """ write final data
        """
        root = self.__mfile.root()
        si = self.__scan.info
        snapshot = {}
        if "snapshot" in si:
            snapshot = si["snapshot"]
        for ds, items in snapshot.items():
            for item in items:
                strategy = None
                if "strategy" in item:
                    strategy = item["strategy"]
                if strategy in ["FINAL"]:
                    try:
                        print("WRITE", ds, strategy)
                        write_snapshot_item(root, item)
                    except Exception as e:
                        print("WWW", str(e))
                        break
            else:
                continue
            break
        root.close()
        self.__mfile.close()
