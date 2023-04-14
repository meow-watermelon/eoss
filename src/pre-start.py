#!/usr/bin/env python3

import os
import sys
from eoss import METADATA_DB_PATH
from eoss import METADATA_DB_TABLE
from eoss import STORAGE_PATH
from eoss.exceptions import MDSConnectException
from eoss.exceptions import MDSExecuteException
from eoss.exceptions import MDSCommitException


def clean_up_storage(record_name):
    object_name = os.path.join(STORAGE_PATH, record_name)
    object_temp_name = os.path.join(STORAGE_PATH, record_name + ".temp")
    flag = 0

    for filename in (object_name, object_temp_name):
        if os.path.exists(filename):
            try:
                os.unlink(filename)
            except Exception as e:
                flag += 1
                print(f"ERROR: failed to remove {filename}: {e}", file=sys.stderr)
            else:
                print(f"file {filename} is removed")


def clean_up_eoss():
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

    # execute SQL query to populate non-0(non-closed) object name(s)
    sql_query_table = f"SELECT id FROM {METADATA_DB_TABLE} WHERE state != 0"
    try:
        output = mds.execute(sql_query_table).fetchall()
    except MDSExecuteException as e:
        print(
            f"ERROR: failed to execute SQL query: {e}",
            file=sys.stderr,
        )
        return False
    else:
        print(f"{len(output)} records located")

    if output is None:
        print(f"uncaught issue when acquiring object state", file=sys.stderr)
        return False
    else:
        if isinstance(output, list) and len(output) == 0:
            print(f"no non-closed record exists")
            return True
        if output:
            # clean up storage path
            for item in output:
                clean_up_storage(item[0])

    # delete record
    sql_delete_record = f"DELETE FROM {METADATA_DB_TABLE} WHERE state != 0"
    try:
        mds.execute(sql_delete_record)
    except MDSExecuteException as e:
        print(
            f"ERROR: failed to delete non-closed object record from metadata database: {e}",
            file=sys.stderr,
        )
        return False

    try:
        mds.commit()
    except MDSCommitException as e:
        print(
            f"ERROR: failed to commit record deletion to metadata database: {e}",
            file=sys.stderr,
        )
        return False


if __name__ == "__main__":
    flag = clean_up_eoss()

    if flag:
        print(f"EOSS clean up is done")
    else:
        print(f"ERROR: unable to clean up EOSS leftover data", file=sys.stderr)
        sys.exit(2)

    sys.exit(0)
