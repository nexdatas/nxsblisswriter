from pninexus import nexus, h5cpp

def main():
    with open("xmlc.xml", 'r') as f:
        xmlc = f.read()
    xmlc1 = xmlc.replace('"NX_DATE_TIME"', '"NX_CHAR"')
    xmlc2 = xmlc1.replace('index="1"', 'index="0"').replace(
        'index="2"', 'index="1"').replace('index="3"', 'index="2"')
    mfile = nexus.create_file("mytest.nxs", h5cpp.file.AccessFlags.TRUNCATE)
    root = mfile.root()
    nexus.create_from_string(root, xmlc2)
    root.close()
    mfile.close()

if __name__ == "__main__":
    main()
