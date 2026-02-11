from pninexus import nexus, h5cpp
import xml.etree.ElementTree as et
from lxml.etree import XMLParser
from lxml import etree
import numpy as np
import json

from sardana_blissdata.utils.redis_utils import get_data_store

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

noattrs = {"name", "label", "dtype" , "value", "nexus_path"}

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
    except Exception as ae:
        # print(ae)
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
                print("VHASPE", vshape, name , value)
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
                    # print("WRITE", am, name, dtype, value, type(value), rvalue)
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
                    if not isinstance(value, list) and not hasattr(value, "shape"):
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




def main():

    data_store = get_data_store("redis://localhost:6380")
    timestamp, key = data_store.get_last_scan()
    scan = data_store.load_scan(key)
    si = scan.info
    filename = si["filename"]
    snapshot = si["snapshot"]
    xmlc = snapshot["nxsdatawriter_xmlsettings"][0]["value"]
    # with open("xmlc.xml", 'r') as f:
    #     xmlc = f.read()
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

    print("Saving scan %s in the file: %s" % (scan.name, filename))
    mfile = nexus.create_file(filename,
                              h5cpp.file.AccessFlags.TRUNCATE)
    root = mfile.root()
    nexus.create_from_string(root, xmls)

    for ds, items in snapshot.items():
        for item in items:
            try:
                write_snapshot_item(root, item)
            except Exception as e:
                print("WWW", str(e))
                break
        else:
            continue
        break

    scsts = scan.streams
    for snm in scsts.keys():
        item = None
        if snm in si["datadesc"]:
            item = si["datadesc"][snm]
            print(item)
    # print(si["datadesc"])
    root.close()
    mfile.close()


if __name__ == "__main__":
    main()
