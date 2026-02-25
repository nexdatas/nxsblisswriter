# -*- coding: utf-8 -*-
#
# This file is part of the NXSBlissWriter project
#
#
#
# Distributed under the terms of the GPL license.
# See LICENSE.txt for more info.

"""
NeXus Bliss Writer

NeXus Bliss Writer stores (meta)data from blissdata provided by NXSDataWriter
"""

import threading

from tango import DebugIt, DevState
from tango.server import Device
# from tango.server import DevStatus
from tango.server import command
from tango.server import device_property

from .NXSWriterService import NXSWriterService as NWS

__all__ = ["NXSBlissWriter"]


class NXSBlissWriter(Device):
    """
    NeXus Bliss Writer stores (meta)data from blissdata provided
    by NXSDataWriter

    **Properties:**

    - Device Property
        RedisUrl
            - Dlissdata redis url
            - Type:'str'
        Session
            - session to be recorder
            - Type:'str'
        NextScanTimeout
            - timeout for next scan writing
            - Type:'int'
        DefaultNeXusPath
            - default NeXus path
            - Type:'str'
    """

    # -----------------
    # Device Properties
    # -----------------

    RedisUrl = device_property(
        dtype='str',
        default_value="redis://localhost:6380",
        doc="Blissdata redis url"
    )

    Session = device_property(
        dtype='str',
        doc="session to be recorder"
    )

    NextScanTimeout = device_property(
        dtype='int',
        default_value=2,
        doc="timeout for next scan writing"
    )

    DefaultNeXusPath = device_property(
        dtype='str',
        default_value="/scan{serialno}:NXentry/"
        "instrument:NXinstrument/collection",
        doc="default NeXus path"
    )

    # ---------------
    # General methods
    # ---------------

    def init_device(self):
        """Initializes the attributes and properties of the NXSBlissWriter."""
        Device.init_device(self)
        self.info_stream("Initializing device...")
        self.nxs_writer_service = NWS(
            self.RedisUrl, self.Session, self.NextScanTimeout,
            self.DefaultNeXusPath)
        self.Start()

    def dev_status(self):
        return self.nxs_writer_service.get_status()

    # --------
    # Commands
    # --------

    @command(
    )
    @DebugIt()
    def Start(self):
        self.thread = threading.Thread(target=self.nxs_writer_service.start)
        self.thread.start()
        self.set_state(DevState.ON)

    @command(
    )
    @DebugIt()
    def Stop(self):
        self.nxs_writer_service.stop()
        self.thread.join()
        self.set_state(DevState.OFF)
