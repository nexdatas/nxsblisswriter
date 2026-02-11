from pninexus import nexus, h5cpp
from xml import sax
import xml.etree.ElementTree as et
from lxml.etree import XMLParser
from lxml import etree

def main():
    with open("xmlc.xml", 'r') as f:
        xmlc = f.read()
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
            # print(etdim.tag, dparent.tag)
            ddparent.remove(dparent)
    xmls = etree.tostring(etroot, encoding='unicode',
                       method='xml', pretty_print=True)

    mfile = nexus.create_file("mytest.nxs", h5cpp.file.AccessFlags.TRUNCATE)
    root = mfile.root()
    nexus.create_from_string(root, xmls)
    root.close()
    mfile.close()

if __name__ == "__main__":
    main()
