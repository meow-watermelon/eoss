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
        log.info(self.__repr__())
        self.mds_client = mds_client.MDSClient()

    def __repr__(self):
        return f"object filename: {self.object_filename}; object name: {self.object_name}; object version: {self.object_version}"

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
        set initialized data for object, only id, filename, version and state would be inserted
        this method should only run once when a new object is uploaded
        """
        try:
            self.mds_client.execute(
                f"INSERT INTO {METADATA_DB_TABLE} VALUES (?, ?, ?, ?, ?, ?)",
                (self.object_name, self.object_filename, self.object_version, None, None, 1),
            )
        except MDSExecuteException as e:
            log.error(
                f"failed to set initial object data for object {self.object_name}"
            )
            raise MDSExecuteException

        try:
            self.mds_client.commit()
        except MDSCommitException as e:
            log.error(
                f"failed to commit initial object data for object {self.object_name}"
            )
            raise MDSCommitException

        log.info(f"object {self.object_name} initialized done in MDS database")

    def set_object_size(self):
        """
        update size column in MDS
        """
        size = None

        try:
            size = os.path.getsize(STORAGE_PATH + "/" + self.object_name)
        except OSError as e:
            log.warning(f"failed to get size of object {self.object_name}: {e}")
        else:
            log.info(f"object {self.object_name} size: {size}")

        try:
            self.mds_client.execute(
                f"UPDATE {METADATA_DB_TABLE} SET size = ? WHERE id = ?",
                (size, self.object_name),
            )
        except MDSExecuteException as e:
            log.warning(f"failed to update size of object {self.object_name}: {e}")
        else:
            try:
                self.mds_client.commit()
            except MDSCommitException as e:
                log.warning(f"failed to commit size of object {self.object_name}: {e}")

    def set_object_timestamp(self):
        """
        update latest object saved timestamp
        """
        timestamp = int(time.time())

        log.info(f"object {self.object_name} latest saved timestamp: {timestamp}")

        try:
            self.mds_client.execute(
                f"UPDATE {METADATA_DB_TABLE} SET timestamp = ? WHERE id = ?",
                (timestamp, self.object_name),
            )
        except MDSExecuteException as e:
            log.warning(f"failed to update timestamp of object {self.object_name}: {e}")
        else:
            try:
                self.mds_client.commit()
            except MDSCommitException as e:
                log.warning(
                    f"failed to commit timestamp of object {self.object_name}: {e}"
                )

    def set_object_state(self, state):
        """
        set object uploading state

        number 1: object uploading request initialized
        number 2: object is saved in local storage w/ "object_name.temp" name
        number 0: object is renamed to "object_name" and fully closed
        """
        log.info(f"set state on object {self.object_name}: {state}")

        try:
            self.mds_client.execute(
                f"UPDATE {METADATA_DB_TABLE} SET state = ? WHERE id = ?",
                (state, self.object_name),
            )
        except MDSExecuteException as e:
            log.warning(f"failed to set state on object {self.object_name}: {e}")
        else:
            try:
                self.mds_client.commit()
            except MDSCommitException as e:
                log.warning(f"failed to commit state of object {self.object_name}: {e}")

    def delete_object(self):
        """
        delete object file and remove record from MDS
        """
        try:
            os.unlink(STORAGE_PATH + "/" + self.object_name)
        except Exception as e:
            log.error(f"failed to delete object file {self.object_name}: {e}")
            raise EOSSInternalException(e)

        try:
            self.mds_client.execute(
                f"DELETE FROM {METADATA_DB_TABLE} WHERE id = ?",
                (self.object_name,),
            )
        except MDSExecuteException as e:
            log.error(f"failed to delete object record {self.object_name} in MDS: {e}")
            raise MDSExecuteException(e)

        try:
            self.mds_client.commit()
        except MDSCommitException as e:
            log.error(f"failed to commit deletion on object {self.object_name}: {e}")
            raise MDSCommitException(e)

        log.info(f"object {self.object_name} is deleted")

    def rollback(self):
        """
        rollback uploading procedure
        this method should only run when uploading procedure or other MDS calls failed
        """
        rollback_flag = 0

        for object_file in (
            STORAGE_PATH + "/" + self.object_name,
            STORAGE_PATH + "/" + self.object_name + ".temp",
        ):
            if os.path.exists(object_file):
                try:
                    os.unlink(object_file)
                except Exception as e:
                    rollback_flag += 1
                    log.warning(f"[ROLLBACK] failed to delete file {object_file}: {e}")

        try:
            self.mds_client.execute(
                f"DELETE FROM {METADATA_DB_TABLE} WHERE id = ?",
                (self.object_name,),
            )
        except MDSExecuteException as e:
            rollback_flag += 1
            log.warning(
                f"[ROLLBACK] failed to delete object record {self.object_name} in MDS: {e}"
            )

        try:
            self.mds_client.commit()
        except MDSCommitException as e:
            rollback_flag += 1
            log.warning(
                f"[ROLLBACK] failed to commit deletion on object {self.object_name}: {e}"
            )

        if not rollback_flag:
            log.info(f"[ROLLBACK] rollback procedure on object {self.object_name} done")

    def check_object_exists(self):
        """
        check if object exists
        this method should return different values based on object uploading state

        True: object exists and fully closed
        False: object does not exists
        number 1: object uploading request initialized
        number 2: object is saved in local storage w/ "object_name.temp" name
        """
        output = None

        try:
            output = self.mds_client.execute(
                f"SELECT state FROM {METADATA_DB_TABLE} WHERE id = ?",
                (self.object_name,),
            ).fetchall()
        except MDSExecuteException as e:
            log.error(
                f"failed to access MDS to acquire object {self.object_name} state: {e}"
            )
            raise MDSExecuteException

        if output is None:
            log.error(f"uncaught issue when acquiring object {self.object_name} state")
            raise EOSSInternalException
        else:
            # check if output is empty list
            if isinstance(output, list) and len(output) == 0:
                return False

            object_exists_flag = output[0][0]

            if object_exists_flag == 0:
                return True
            if object_exists_flag == 1:
                return 1
            if object_exists_flag == 2:
                return 2
