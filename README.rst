======================================================
Welcome to NeXuS Configuration Server's documentation!
======================================================

|github workflow|
|docs|
|Pypi Version|
|Python Versions|

.. |github workflow| image:: https://github.com/nexdatas/nxsconfigserver/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/nexdatas/nxsconfigserver/actions
   :alt:

.. |docs| image:: https://img.shields.io/badge/Documentation-webpages-ADD8E6.svg
   :target: https://nexdatas.github.io/nxsconfigserver/index.html
   :alt:

.. |Pypi Version| image:: https://img.shields.io/pypi/v/nxsconfigserver.svg
                  :target: https://pypi.python.org/pypi/nxsconfigserver
                  :alt:

.. |Python Versions| image:: https://img.shields.io/pypi/pyversions/nxsconfigserver.svg
                     :target: https://pypi.python.org/pypi/nxsconfigserver/
                     :alt:



Authors: Jan Kotanski, Eugen Wintersberger, Halil Pasic

NeXuS Configuration Server is a Tango Server with its implementation based
on a MySQL database. It allows to store XML configuration datasources
and components. It also gives possibility to select mandatory components
and perform the process of component merging.

Tango Server API: https://nexdatas.github.io/nxsconfigserver/doc_html

| Source code: https://github.com/nexdatas/nxsconfigserver/
| Web page: https://nexdatas.github.io/nxsconfigserver/
| NexDaTaS Web page: https://nexdatas.github.io

------------
Installation
------------

Install the dependencies:

|    MySQLdb, PyTango, sphinx

From sources
^^^^^^^^^^^^

Download the latest version of NeXuS Configuration Server from

|    https://github.com/nexdatas/nxsconfigserver/
|    https://github.com/nexdatas/nxsconfigserver-db/

Extract the sources and run

.. code-block:: console

	  $ python setup.py install

To set database execute

.. code-block:: console

	  $ mysql < conf/mysql_create.sql

with proper privileges.

Debian packages
^^^^^^^^^^^^^^^

Debian Trixie, Bookworm, Bullseye and as well as Ubuntu Questing, Noble, Jammy  packages can be found in the HDRI repository.

To install the debian packages, add the PGP repository key

.. code-block:: console

	  $ sudo su
	  $ curl -s http://repos.pni-hdri.de/debian_repo.pub.gpg | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/debian-hdri-repo.gpg --import
	  $ chmod 644 /etc/apt/trusted.gpg.d/debian-hdri-repo.gpg

and then download the corresponding source list, e.g. for trixie

.. code-block:: console

	  $ cd /etc/apt/sources.list.d
	  $ wget http://repos.pni-hdri.de/trixie-pni-hdri.sources

Finally, for python2 packages

.. code-block:: console

	  $ apt-get update
	  $ apt-get install python-nxsconfigserver nxsconfigserver-db

and the NXSConfigServer tango server (from 2.10.0)

	  $ apt-get install nxsconfigserver

or for python3

.. code-block:: console

	  $ apt-get update
	  $ apt-get install python3-nxsconfigserver nxsconfigserver-db

and the NXSConfigServer tango server (from 2.10.0)

	  $ apt-get install nxsconfigserver3


From pip
""""""""

To install it from pip you need pymysqldb e.g.

.. code-block:: console

   $ python3 -m venv myvenv
   $ . myvenv/bin/activate

   $ pip install pymysqldb

   $ pip install nxsconfigserver

Moreover it is also good to install

.. code-block:: console

   $ pip install pytango
   $ pip install nxstools

Setting NeXus Configuration Server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To set up  NeXus Configuration Server with the default configuration run

.. code-block:: console

          $ nxsetup -x NXSConfigServer

The *nxsetup* command comes from the **python-nxstools** package.

===========
Description
===========

Configuration Server is dedicated to store NXDL-like configuration needed for
Tango Data Writer runs. The server uses as a storage system a MYSQL database.
To create required DB tables one can use ndts.sql script from the repository.

In Configuration Server the configuration is memorized in separate elements:
datasources or components.

**DataSources** describe access to input data, i.e to specific hardware
TANGO devices or other databases as well to client data.

**Components** specify Nexus tree with positions of datasets for particular
pieces of hardware and writing strategy for corresponding to them data.

+ They can include datasources directly as well as links to datasources
  defined in the server. To this end template syntax of
  $datasources.<ds_name> type is used.
+ Moreover, they can holds links to other components which describe their
  dependences. In this case $components.<comp_name> syntax is used.
+ Finally, the components can contains variables. The variables are defined
  in XML code by $var.<var_name> syntax and can be provided to
  the Configuration Server by passing a JSON string.
  The default value for variables is an empty string.

All elements of configuration can be created by GUI tool - ComponentDesigner.
The tool can connect to Configuration Server and fetch or store
the separate elements of the XML configuration.

During creation of the final configuration Configuration Server merges
all required and dependent components, connected to them datasources and
provided values of the variables. As a result it returns a single XML string.
This XML string can be pass directly into the dedicated Tango Data Writer
attribute.



===========
Client code
===========

.. code-block:: python

    # In this section we present an example how to communicate with
    # Configuration Server making use of PyTango.

    import tango

    cnfServer = tango.DeviceProxy("p00/xmlconfigserver/exp.01")

    cnfServer.JSONSettings = \
	'{"db":"ndts_p02","read_default_file":"/etc/my.cnf","use_unicode":true}'

    # opens DB connection
    cnfServer.Open()

    # After creating the server proxy we can set configuration for connection to
    #  the MYSQL DB.
    # The JSONSettings attribute is memorized so you have to write it only when you
    # change configuration of DB connection. Next, we open connection to
    # DB specified by our JSONSettings.



    # stores default component
    cpxml = open("default.xml", 'r').read()
    cnfServer.XMLString = cpxml
    cnfServer.StoreComponent('default')

    # stores slit1 component in DB
    cpxml = open("slit1.xml", 'r').read()
    cnfServer.XMLString = cpxml
    cnfServer.StoreComponent('slit1')

    # stores slit2 component in DB
    cpxml = open("slit2.xml", 'r').read()
    cnfServer.XMLString = cpxml
    cnfServer.StoreComponent('slit2')

    # stores slit3 component in DB
    cpxml = open("slit3.xml", 'r').read()
    cnfServer.XMLString = cpxml
    cnfServer.StoreComponent('slit3')

    # stores pilatus300k component in DB
    cpxml = open("pilatus.xml", 'r').read()
    cnfServer.XMLString = cpxml
    cnfServer.StoreComponent('pilatus300k')


    # stores motor01 datasource in DB
    dsxml = open("motor.ds.xml", 'r').read()
    cnfServer.XMLString = dsxml
    cnfServer.StoreDataSource('motor01')

    # stores motor02 datasource in DB
    dsxml = open("motor.ds.xml", 'r').read()
    cnfServer.XMLString = dsxml
    cnfServer.StoreDataSource('motor02')



    # removes slit3 component from DB
    cnfServer.DeleteComponent('slit3')

    # removes motor02 datasource from DB
    cnfServer.DeleteDataSource('motor02')

    # If someone cannot use ComponentDesigner it is also an option to store
    # or delete components and datasources using directly tango interface
    # as it is shown above.



    # provides names of available components
    cmpNameList = cnfServer.AvailableComponents()
    # provides names of available datasources
    dsNameList = cnfServer.AvailableDataSources()

    # To get information about names of available components and datasources
    # in Configuration Server we use the above commands.



    # provides a list of required components
    cmpList = cnfServer.Components(cmpNameList)
    # provides a list of required Datasources
    dsList = cnfServer.DataSources(dsNameList)

    # Having names of stored elements we can get their XML code.

    # provides a list of Datasources from a given Component
    dsList = cnf.Server.ComponentDataSources('pilatus300k')
    dsList = cnf.Server.ComponentsDataSources(['pilatus300k', 'slit1'])

    # as well as query Configuration Server which datasource
    # are related to the particular component.

    # provides a dependent components
    cpList = cnf.Server.DependentComponents(['pilatus300k', 'slit3'])


    # Moreover, one can also query Configuration Server for a list of
    # dependent components

    # provides a list of Variables from a given components
    varList = cnf.Server.ComponentVariables('pilatus300k')
    varList = cnf.Server.ComponentsVariables(['pilatus300k', 'slit3'])

    #or ask for a list of variables which are related to the particular components.

    # sets values of variables
    cnf.Server.Variables = '{"entry_id":"123","beamtime_id":"123453535453"}'

    #The variable values can be passed to the Configuration Server
    # via a JSON string.



    # sets given component as mandatory for the final configuration
    cnfServer.SetMandatoryComponents(['default','slit1'])
    # un-sets given component as mandatory for the final configuration
    cnfServer.UnsetMandatoryComponents(['slit1'])

    # provides names of mandatory components
    man =  cnfServer.MandatoryComponents()

    # Some of the component can be set as mandatory in
    # the final configuration. To define them Configuration Server provides
    # above commands.



    # provides the current configuration version
    version =  cnfServer.Version

    # Each configuration has a revision number. It can be found
    # together with Configuration Server version in Version attribute.

    # creates the final configuration from slit2 and pilatus300k
    # as well as all mandatory components
    cnfServer.CreateConfiguration('slit2', 'pilatus300k')
    # XML string ready to use by Tango Data Server
    finalXML = cnfServer.XMLString

    # In order to create our final configuration we execute CreateConfiguration
    # command with a list of names of required components. The command merges
    # these components with mandatory ones and provides the resulting NXDL-like
    # configuration in the XMLString attribute.




    # merges given components
    mergedComp = cnfServer.Merge(['slit2', 'pilatus300k'])

    # Similarly, the Merge command provides configuration by unresolved links
    # to datasoures and with non-assigned variable values.


    # closes connection to DB
    cnfServer.close()

    # Command close terminates our connection to the DB server.

=======================
Configuration Variables
=======================

Values of configuration variables can be also define inside the component xmls.
Let's consider two following components:

*mydetector* with a general detector transformation group

.. code-block:: xml

   <definition>
     <group type='NXentry' name='entry'>
       <group type='NXinstrument' name='instrument'>
          <group type='NXdetector' name='$var.detector#\"mydetector\"'>
             <group type='NXtransformations' name='transformations'/>
	  </group>
       </group>
     </group>
   </definition>

and *pilatus* created for the particular detector

.. code-block:: xml

   <definition>
     <group type='NXentry' name='entry'>
       <group type='NXinstrument' name='instrument'>
          <group type='NXdetector' name='pilatus'>
             <field type='NX_FLOAT64' name='data'/>
	  </group>
       </group>
     </group>
     <doc>$var(detector=pilatus)</doc>
   </definition>


Creating configuration without variables

.. code-block:: python

   cnfServer.Variables = '{}'
   cnfServer.CreateConfiguration(["mydetector"])

results in

.. code-block:: xml

   <definition>
     <group type='NXentry' name='entry'>
       <group type='NXinstrument' name='instrument'>
          <group type='NXdetector' name='mydetector'>
             <group type='NXtransformations' name='transformations'/>
	  </group>
       </group>
     </group>
   </definition>

When configuration variables are defined

.. code-block:: python

   cnfServer.Variables = '{"detector": "det1"}'
   cnfServer.CreateConfiguration(["mydetector"])

one can get

.. code-block:: xml

   <definition>
     <group type='NXentry' name='entry'>
       <group type='NXinstrument' name='instrument'>
          <group type='NXdetector' name='det1'>
             <group type='NXtransformations' name='transformations'/>
	  </group>
       </group>
     </group>
   </definition>

Finally, creating configuration xml from our two components without variables

.. code-block:: python

   cnfServer.Variables = '{}'
   cnfServer.CreateConfiguration(["mydetector", "pilatus"])

results in

.. code-block:: xml

   <definition>
   <group name="entry" type="NXentry">
     <group name="instrument" type="NXinstrument">
       <group name="pilatus" type="NXdetector">
         <group name="transformations" type="NXtransformations"/>
         <field name="data" type="NX_FLOAT64"/>
	 </group>
       </group>
     </group>
     <doc>$var(detector=pilatus)</doc>
   </definition>
