import os
import sqlite3
from . import logger
from . import LOGGING_PATH
from . import METADATA_DB_PATH
from . import METADATA_DB_TABLE
from .exceptions import MDSConnectException
from .exceptions import MDSExecuteException
from .exceptions import MDSCommitException
from .exceptions import EOSSInternalException

mds_client_log = os.path.join(LOGGING_PATH, "mds_client.log")
log = logger.Logger(__name__, mds_client_log)


class MDSClient:
    def __init__(self):
        self.db_name = METADATA_DB_PATH
        self.db_connection = None
        self.db_cursor = None
        log.info(f"initialized metadata database file {self.db_name}")

    def connect(self):
        try:
            self.db_connection = sqlite3.connect(self.db_name)
        except sqlite3.OperationalError as e:
            log.error(
                f"failed to connect metadata database {self.db_name} - error: {str(e)}"
            )
            raise MDSConnectException(str(e))

    def cursor(self):
        self.db_cursor = self.db_connection.cursor()

    def execute(self, sql_executable, parameters=None):
        if parameters is None:
            parameters = ()

        log.info(f"SQL executable: {sql_executable} parameters: {parameters}")

        try:
            self.db_cursor.execute(sql_executable, parameters)
        except (
            sqlite3.OperationalError,
            sqlite3.DatabaseError,
            sqlite3.InterfaceError,
            sqlite3.IntegrityError,
        ) as e:
            log.error(f"failed to execute {sql_executable} - error: {str(e)}")
            raise MDSExecuteException(str(e))

        return self.db_cursor

    def fetchall(self):
        output = []

        try:
            output = self.db_cursor.fetchall()
        except sqlite3.OperationalError as e:
            log.error(f"failed to execute fetchall() call - error: {str(e)}")
            raise MDSExecuteException(str(e))

    def commit(self):
        try:
            self.db_connection.commit()
        except sqlite3.OperationalError as e:
            log.error(f"failed to commit - error: {str(e)}")
            raise MDSCommitException(str(e))

    def close(self):
        self.db_connection.close()
