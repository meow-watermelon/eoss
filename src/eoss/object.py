import os
import time
from . import logger
from . import mds_client
from . import LOGGING_PATH
from . import object_name
from . import STORAGE_PATH
from .exceptions import MDSConnectException
from .exceptions import MDSExecuteException
from .exceptions import MDSCommitException
from .exceptions import EOSSInternalException

object_log = LOGGING_PATH + "/" + "object.log"
log = logger.Logger(__name__, object_log)


class Object:
    def __init__(self, object_filename, *, object_version=None):
        self.object_filename = object_filename
        self.object_version = object_version
        self.object_name = object_name.set_object_name(
            self.object_filename, self.object_version
        )
        self.mds_client = mds_client.MDSClient()

    def init_mds(self):
        try:
            self.mds_client.connect()
        except MDSConnectException as e:
            log.error(f"unable to connect to metadata database: {str(e)}")
            raise MDSConnectException

        self.mds_client.cursor()
        log.info("metadata database initialized")

    def close_mds(self):
        self.mds_client.close()

    def set_object_init_data(self):
        pass

    def set_object_size(self):
        pass

    def set_object_timestamp(self):
        pass

    def set_object_state(self, state):
        pass

    def check_object_exists(self):
        pass
