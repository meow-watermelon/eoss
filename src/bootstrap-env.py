#!/usr/bin/env python3

import os
import sys
from eoss import LOGGING_PATH
from eoss import METADATA_DB_PATH
from eoss import METADATA_DB_TABLE
from eoss import STORAGE_PATH
from eoss.exceptions import MDSConnectException
from eoss.exceptions import MDSExecuteException
from eoss.exceptions import MDSCommitException


def bootstrap_mds(mds_table):
    from eoss import mds_client

    # initial mds client
    mds = mds_client.MDSClient()

    # connect mds filename
    try:
        mds.connect()
    except MDSConnectException as e:
        print(
            f"ERROR: failed to connect to MDS database file {mds.db_name}: {e}",
            file=sys.stderr,
        )
        return False

    # initialize database cursor
    mds.cursor()

    # execute SQL query to create table
    sql_create_table = f"CREATE TABLE {mds_table} (id STRING, filename STRING, version STRING, size INTEGER, timestamp INTEGER, state INTEGER)"
    try:
        mds.execute(sql_create_table)
    except MDSExecuteException as e:
        print(
            f"ERROR: failed to execute SQL query to create the table {mds_table}: {e}",
            file=sys.stderr,
        )
        return False

    # commit changes
    try:
        mds.commit()
    except MDSCommitException as e:
        print(f"ERROR: failed to commit SQL query: {e}", file=sys.stderr)
        return False

    # close mds
    mds.close()

    return True


if __name__ == "__main__":
    # create directories
    for eoss_dir in (LOGGING_PATH, os.path.dirname(METADATA_DB_PATH), STORAGE_PATH):
        try:
            os.makedirs(eoss_dir, exist_ok=True)
        except Exception as e:
            print(f"ERROR: failed to create directory {eoss_dir}: {e}", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"Directory {eoss_dir} created")

    # bootstrap database
    if os.path.exists(METADATA_DB_PATH):
        print(
            f"ERROR: MDS database {METADATA_DB_PATH} exists already, please remove this file if you want to bootstrap a clean MDS database"
        )
        sys.exit(2)
    else:
        mds_bootstrap_return = bootstrap_mds(METADATA_DB_TABLE)
        if mds_bootstrap_return:
            print(
                f"MDS database {METADATA_DB_PATH} bootstrapped done with table {METADATA_DB_TABLE}"
            )
        else:
            print(f"ERROR: failed to bootstrap MDS database {METADATA_DB_PATH}")
            sys.exit(2)

    sys.exit(0)
