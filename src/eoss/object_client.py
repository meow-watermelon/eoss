import os
import time
from . import logger
from . import mds_client
from . import object_name
from . import LOGGING_PATH
from . import METADATA_DB_TABLE
from . import STORAGE_PATH
from .exceptions import MDSConnectException
from .exceptions import MDSExecuteException
from .exceptions import MDSCommitException
from .exceptions import EOSSInternalException

object_log = LOGGING_PATH + "/" + "object.log"
log = logger.Logger(__name__, object_log)


class ObjectClient:
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
        """
        set initialized data for object, only id, filename and state would be inserted
        this method should only run once when a new object is uploaded
        """
        try:
            self.mds_client.execute(
                f"INSERT INTO {METADATA_DB_TABLE} VALUES (?, ?, ?, ?, ?, ?)", (self.object_name, self.object_filename, None, None, None, 1)
            )
        except MDSExecuteException as e:
            log.error(f"failed to set initial object data for object {self.object_filename}")
            raise MDSExecuteException

        try:
            self.mds_client.commit()
        except MDSCommitException as e:
            log.error(f"failed to commit initial object data for object {self.object_filename}")
            raise MDSCommitException

        log.info(f"object {self.object_filename} initialized done in MDS database")

    def set_object_size(self):
        """
        update size column in MDS
        """
        size = None

        try:
            size = os.path.getsize(STORAGE_PATH + "/" + self.object_name)
        except OSError as e:
            log.warning(f"failed to get size of object: {e}")
        else:
            log.info(f"object {self.object_filename} size: {size}")

        try:
            self.mds_client.execute(
                f"UPDATE {METADATA_DB_TABLE} SET size = ? WHERE id = ?", (size, self.object_name)
            )
        except MDSExecuteException as e:
            log.warning(f"failed to update size of object {self.object_filename}: {e}")

        try:
            self.mds_client.commit()
        except MDSCommitException as e:
            log.warning(f"failed to commit size of object {self.object_filename}: {e}")

    def set_object_timestamp(self):
        pass

    def set_object_state(self, state):
        pass

    def delete_object(self):
        pass

    def check_object_exists(self):
        pass
