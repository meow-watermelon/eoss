import fcntl
import os
import pathlib
import time
from . import logger
from . import mds_client
from . import object_name
from . import LOGGING_PATH
from . import METADATA_DB_TABLE
from . import OBJECT_LOCK_PATH
from . import STORAGE_PATH
from .exceptions import MDSConnectException
from .exceptions import MDSExecuteException
from .exceptions import MDSCommitException
from .exceptions import EOSSInternalException
from .exceptions import ObjectUnderLockException

object_client_log = os.path.join(LOGGING_PATH, "object_client.log")
log = logger.Logger(__name__, object_client_log)


class ObjectClient:
    def __init__(self, object_filename, *, object_version=None):
        self._object_filename = object_filename
        self._object_version = object_version
        log.info(self.__repr__())
        self.mds_client = mds_client.MDSClient()

    def __repr__(self):
        return f"object filename: {self.object_filename}; object name: {self.object_name}; object version: {self.object_version}"

    @property
    def object_filename(self):
        return self._object_filename

    @property
    def object_version(self):
        return self._object_version

    @property
    def object_name(self):
        return object_name.set_object_name(self.object_filename, self.object_version)

    @property
    def object_lock_filename(self):
        return os.path.join(OBJECT_LOCK_PATH, self.object_name + ".lock")

    def init_mds(self):
        try:
            self.mds_client.connect()
        except MDSConnectException as e:
            log.error(f"unable to connect to metadata database: {str(e)}")
            raise MDSConnectException(e)

        self.mds_client.cursor()
        log.info("metadata database initialized")

    def close_mds(self):
        self.mds_client.close()

    def set_object_init_data(self, override=False):
        """
        set initialized data for object, only id, filename, version and state would be inserted
        only set override=True when uploading the same object
        """
        try:
            if override:
                self.mds_client.execute(
                    f"UPDATE {METADATA_DB_TABLE} SET size = ?, timestamp = ?, state = ? WHERE id = ?",
                    (
                        None,
                        None,
                        1,
                        self.object_name,
                    ),
                )
            else:
                self.mds_client.execute(
                    f"INSERT INTO {METADATA_DB_TABLE} VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        self.object_name,
                        self.object_filename,
                        self.object_version,
                        None,
                        None,
                        1,
                    ),
                )
        except MDSExecuteException as e:
            log.error(
                f"failed to set initial object data for object {self.object_name}"
            )
            raise MDSExecuteException(e)

        try:
            self.mds_client.commit()
        except MDSCommitException as e:
            log.error(
                f"failed to commit initial object data for object {self.object_name}"
            )
            raise MDSCommitException(e)

        log.info(f"object {self.object_name} initialized done in MDS database")

    def set_object_size(self):
        """
        update size column in MDS
        """
        size = None

        try:
            size = os.path.getsize(os.path.join(STORAGE_PATH, self.object_name))
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
            log.error(f"failed to set state on object {self.object_name}: {e}")
            raise MDSExecuteException(e)

        try:
            self.mds_client.commit()
        except MDSCommitException as e:
            log.error(f"failed to commit state of object {self.object_name}: {e}")
            raise MDSCommitException(e)

    def delete_object(self):
        """
        delete object file and remove record from MDS
        this method can only delete fully closed object
        """
        try:
            os.unlink(os.path.join(STORAGE_PATH, self.object_name))
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
        rollback procedure:
        1. delete saved files including temp one
        2. delete row from metadata database
        """
        rollback_flag = 0

        for object_file in (
            os.path.join(STORAGE_PATH, self.object_name),
            os.path.join(STORAGE_PATH, self.object_name + ".temp"),
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
            return True
        else:
            log.warning(
                f"[ROLLBACK] rollback procedure on object {self.object_name} failed"
            )
            return False

    def check_object_exists(self):
        """
        check if object exists
        this method should return different values based on object uploading state

        True: object exists and fully closed
        False: object does not exists
        number 1: object uploading request initialized
        number 2: object is saved in local storage w/ "object_name.temp" name
        number 3: object state is fully closed but object does not exist
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
            raise MDSExecuteException(e)

        if output is None:
            log.error(f"uncaught issue when acquiring object {self.object_name} state")
            raise EOSSInternalException("uncaught non-state exception")
        else:
            # check if output is empty list
            if isinstance(output, list) and len(output) == 0:
                return False

            object_exists_flag = output[0][0]

            if object_exists_flag == 0 and os.path.exists(
                os.path.join(STORAGE_PATH, self.object_name)
            ):
                return True
            if object_exists_flag == 0 and not os.path.exists(
                os.path.join(STORAGE_PATH, self.object_name)
            ):
                return 3
            if object_exists_flag == 1:
                return 1
            if object_exists_flag == 2:
                return 2

    def set_write_lock(self):
        """
        create an exclusive write lock
        """
        self.object_lock_filename_fd = open(self.object_lock_filename, "wb")

        log.info(f"setting write lock on object {self.object_name}")
        try:
            fcntl.flock(self.object_lock_filename_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            log.info(f"object {self.object_name} write lock bailed")
            raise ObjectUnderLockException(e)
        else:
            log.info(f"set object {self.object_name} write lock done")

    def set_read_lock(self):
        """
        create a shared read lock
        """
        if not os.path.exists(self.object_lock_filename):
            pathlib.Path.touch(self.object_lock_filename, exist_ok=True)

        self.object_lock_filename_fd = open(self.object_lock_filename, "rb")

        log.info(f"setting read lock on object {self.object_name}")
        try:
            fcntl.flock(self.object_lock_filename_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as e:
            log.info(f"object {self.object_name} read lock bailed")
            raise ObjectUnderLockException(e)
        else:
            log.info(f"set object {self.object_name} read lock done")

    def remove_lock(self):
        """
        remove a lock
        """
        fcntl.flock(self.object_lock_filename_fd, fcntl.LOCK_UN)
        self.object_lock_filename_fd.close()
        log.info(f"removed lock on object lock file {self.object_lock_filename}")
