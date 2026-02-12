==================================================
Welcome to NeXuS BlissData Writer's documentation!
==================================================

|github workflow|
|docs|
|Pypi Version|
|Python Versions|

.. |github workflow| image:: https://github.com/nexdatas/nxsblisswriter/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/nexdatas/nxsblisswriter/actions
   :alt:

.. |docs| image:: https://img.shields.io/badge/Documentation-webpages-ADD8E6.svg
   :target: https://nexdatas.github.io/nxsblisswriter/index.html
   :alt:

.. |Pypi Version| image:: https://img.shields.io/pypi/v/nxsblisswriter.svg
                  :target: https://pypi.python.org/pypi/nxsblisswriter
                  :alt:

.. |Python Versions| image:: https://img.shields.io/pypi/pyversions/nxsblisswriter.svg
                     :target: https://pypi.python.org/pypi/nxsblisswriter/
                     :alt:



Authors: Jan Kotanski

NeXuS BlissData Writer Server is a Tango Server  which
allows to write NeXus file from (meta)data stored in BlissData
by NXSDataWriter tango server

Tango Server API: https://nexdatas.github.io/nxsblisswriter/doc_html

| Source code: https://github.com/nexdatas/nxsblisswriter/
| Web page: https://nexdatas.github.io/nxsblisswriter/
| NexDaTaS Web page: https://nexdatas.github.io

------------
Installation
------------

Install the dependencies:

|    tango, sphinx

From sources
^^^^^^^^^^^^

Download the latest version of NeXuS Configuration Server from

|    https://github.com/nexdatas/nxsblisswriter/
|    https://github.com/nexdatas/nxsblisswriter-db/

Extract the sources and run

.. code-block:: console

	  $ python setup.py install

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

Finally, 

.. code-block:: console

	  $ apt-get update
	  $ apt-get install python3-nxsblisswriter

and the NXSBlissWriter tango server (from 2.10.0)

	  $ apt-get install nxsblisswriter


From pip
""""""""

To install it from pip you need pymysqldb e.g.

.. code-block:: console

   $ python3 -m venv myvenv
   $ . myvenv/bin/activate

   $ pip install nxsblisswriter

Moreover it is also good to install

.. code-block:: console

   $ pip install pytango
   $ pip install nxstools

Setting NeXus Configuration Server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To set up  NeXus Blissdata WriterServer with the default configuration run

.. code-block:: console

          $ nxsetup -x NXSBlissWriter

The *nxsetup* command comes from the **python-nxstools** package.

