#!/usr/bin/env python3

import os
import time
from eoss import logger
from eoss import mds_client
from eoss import object_client
from eoss import utils
from eoss import LOGGING_PATH
from eoss import METADATA_DB_TABLE
from eoss import SAFEMODE
from eoss import STORAGE_PATH
from eoss.exceptions import MDSConnectException
from eoss.exceptions import MDSExecuteException
from eoss.exceptions import MDSCommitException
from eoss.exceptions import EOSSInternalException
from eoss.exceptions import ObjectUnderLockException
from flask import Flask
from flask import g
from flask import jsonify
from flask import request
from flask import send_file
from werkzeug.serving import WSGIRequestHandler

# set up loggers
log = logger.Logger(__name__, os.path.join(LOGGING_PATH, "eoss.log"))
access_log = logger.AccessLogger("access_log", os.path.join(LOGGING_PATH, "access.log"))

# enable HTTP/1.1
WSGIRequestHandler.protocol_version = "HTTP/1.1"

app = Flask(__name__)


@app.route(
    "/eoss/v1/object/<string:object_filename>", methods=["GET", "HEAD", "DELETE", "PUT"]
)
def process_object(object_filename):
    if request.method not in ("GET", "HEAD", "DELETE", "PUT"):
        log.warning("request method {request.method} is not allowed, ignored")
        return ("Bad Method", 405)

    # check if SAFEMODE is enabled
    if (request.method in ("DELETE", "PUT")) and SAFEMODE:
        log.info("safemode is enabled, DELETE and PUT methods are not usable")
        return ("EOSS Safemode Enabled", 525)

    # HTTP methods usage
    # GET: get object
    # HEAD: check if object exists
    # DELETE: delete object
    # PUT: upload object

    # get object version information
    if "X-EOSS-Object-Version" in request.headers:
        object_version = request.headers["X-EOSS-Object-Version"]
    else:
        object_version = None

    # initialize object client
    eoss_object_client = object_client.ObjectClient(
        object_filename, object_version=object_version
    )
    log.info(
        f"object_filename: {object_filename} object_version: {object_version} object_name: {eoss_object_client.object_name}"
    )

    try:
        eoss_object_client.init_mds()
    except MDSConnectException as e:
        log.error(f"failed to connect to metadata database: {str(e)}")
        return ("MDS Connection Failure", 520)
    else:
        log.info("metadata database initialized")

    # retrieve object existence state
    try:
        object_exists_flag = eoss_object_client.check_object_exists()
    except MDSExecuteException as e:
        log.error(f"failed to execute SQL query: {e}")
        return ("MDS Execution Failure", 521)
    except EOSSInternalException as e:
        log.error(
            f"uncaught issue when acquiring object {eoss_object_client.object_name} state"
        )
        return ("EOSS Internal Exception Failure", 523)
    else:
        log.info(
            f"object existence flag for object {eoss_object_client.object_name} is {object_exists_flag}"
        )

    # log request method
    log.info(
        f"HTTP request method {request.method} detected for object {eoss_object_client.object_name}"
    )

    # HEAD method
    if request.method == "HEAD":
        eoss_object_client.close_mds()

        if object_exists_flag is True:
            return ("Object Exists", 200)
        if object_exists_flag is False:
            return ("Object Does Not Exist", 404)
        if object_exists_flag == 1:
            return ("Object Initialized Only", 440)
        if object_exists_flag == 2:
            return ("Object Saved Not Closed", 441)
        if object_exists_flag == 3:
            return ("Object MDS Closed Not In Local", 524)

    # GET method
    if request.method == "GET":
        eoss_object_client.close_mds()
        # set read lock
        try:
            eoss_object_client.set_read_lock()
        except ObjectUnderLockException as e:
            log.info(f"object {eoss_object_client.object_name} read lock bailed")
            return ("Object Read Conflict", 409)

        if object_exists_flag is True:
            # download object
            try:
                eoss_object_client.remove_lock()
                return send_file(
                    os.path.join(STORAGE_PATH, eoss_object_client.object_name),
                    as_attachment=True,
                    download_name=object_filename,
                )
            except Exception as e:
                log.error(
                    f"failed to download object {object_filename} - object_name: {eoss_object_client.object_name}: {e}"
                )
                eoss_object_client.remove_lock()
                return ("EOSS Internal Exception Failure", 523)
        if object_exists_flag is False:
            eoss_object_client.remove_lock()
            return ("Object Does Not Exist", 404)
        if object_exists_flag == 1:
            eoss_object_client.remove_lock()
            return ("Object Initialized Only", 440)
        if object_exists_flag == 2:
            eoss_object_client.remove_lock()
            return ("Object Saved Not Closed", 441)
        if object_exists_flag == 3:
            eoss_object_client.remove_lock()
            return ("Object MDS Closed Not In Local", 524)

    # DELETE method
    if request.method == "DELETE":
        # set write lock
        try:
            eoss_object_client.set_write_lock()
        except ObjectUnderLockException as e:
            log.info(f"object {eoss_object_client.object_name} write lock bailed")
            return ("Object Write Conflict", 409)

        if object_exists_flag is True:
            # delete object
            try:
                eoss_object_client.delete_object()
            except EOSSInternalException as e:
                log.error(
                    f"uncaught issue when deleting object {eoss_object_client.object_name}: {e}"
                )
                eoss_object_client.remove_lock()
                return ("EOSS Internal Exception Failure", 523)
            except MDSExecuteException as e:
                log.error(
                    f"failed to delete object record {eoss_object_client.object_name}: {e}"
                )
                eoss_object_client.remove_lock()
                return ("MDS Execution Failure", 521)
            except MDSCommitException as e:
                log.error(
                    f"failed to commit deletion on object {eoss_object_client.object_name}: {e}"
                )
                eoss_object_client.remove_lock()
                return ("MDS Commit Failure", 522)

            eoss_object_client.close_mds()
            eoss_object_client.remove_lock()
            log.info(f"object {eoss_object_client.object_name} is deleted")

            return ("Object Deleted", 200)
        else:
            eoss_object_client.close_mds()
            eoss_object_client.remove_lock()

            if object_exists_flag is False:
                return ("Object Does Not Exist", 404)
            if object_exists_flag == 1:
                return ("Object Initialized Only", 440)
            if object_exists_flag == 2:
                return ("Object Saved Not Closed", 441)
            if object_exists_flag == 3:
                return ("Object MDS Closed Not In Local", 524)

    # PUT method
    if request.method == "PUT":
        # set write lock
        try:
            eoss_object_client.set_write_lock()
        except ObjectUnderLockException as e:
            log.info(f"object {eoss_object_client.object_name} write lock bailed")
            return ("Object Write Conflict", 409)

        if object_exists_flag is True or object_exists_flag is False:
            # initialize object metadata
            try:
                if object_exists_flag is True:
                    eoss_object_client.set_object_init_data(override=True)
                else:
                    eoss_object_client.set_object_init_data()
            except MDSExecuteException as e:
                log.error(
                    f"failed to set initial object data for object {eoss_object_client.object_name}"
                )
                eoss_object_client.remove_lock()
                return ("MDS Execution Failure", 521)
            except MDSCommitException as e:
                log.error(
                    f"failed to commit initial object data for object {eoss_object_client.object_name}"
                )
                eoss_object_client.remove_lock()
                return ("MDS Commit Failure", 522)
            else:
                log.info(
                    f"initial data for object {eoss_object_client.object_name} set done"
                )

            # state 1 phase
            try:
                eoss_object_client.set_object_state(1)
            except Exception as e:
                log.error(
                    f"failed to set object state 1 for object {eoss_object_client.object_name}: {e}"
                )
                rollback_flag = eoss_object_client.rollback()
                eoss_object_client.close_mds()
                eoss_object_client.remove_lock()

                if rollback_flag:
                    return ("EOSS Rollback Done", 526)
                else:
                    return ("EOSS Rollback Failed", 527)
            else:
                log.info(f"object {eoss_object_client.object_name} state initialized")

            # write data to temp file
            try:
                with open(
                    os.path.join(
                        STORAGE_PATH, eoss_object_client.object_name + ".temp"
                    ),
                    "wb",
                ) as f:
                    f.write(request.data)
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as e:
                log.error(
                    f"failed to write object data to {eoss_object_client.object_name} temp file: {e}"
                )
                rollback_flag = eoss_object_client.rollback()
                eoss_object_client.close_mds()
                eoss_object_client.remove_lock()

                if rollback_flag:
                    return ("EOSS Rollback Done", 526)
                else:
                    return ("EOSS Rollback Failed", 527)
            else:
                log.info(
                    f"object {eoss_object_client.object_name} data is saved in temp file"
                )

            # state 2 phase
            try:
                eoss_object_client.set_object_state(2)
            except Exception as e:
                log.error(
                    f"failed to set object state 2 for object {eoss_object_client.object_name}: {e}"
                )
                rollback_flag = eoss_object_client.rollback()
                eoss_object_client.close_mds()
                eoss_object_client.remove_lock()

                if rollback_flag:
                    return ("EOSS Rollback Done", 526)
                else:
                    return ("EOSS Rollback Failed", 527)
            else:
                log.info(
                    f"set object state 2 for object {eoss_object_client.object_name} as temp file is saved already"
                )

            # rename temp file to final object name
            try:
                os.rename(
                    os.path.join(
                        STORAGE_PATH, eoss_object_client.object_name + ".temp"
                    ),
                    os.path.join(STORAGE_PATH, eoss_object_client.object_name),
                )
            except Exception as e:
                log.error(
                    f"failed to rename temp file to final file for object {eoss_object_client.object_name}: {e}"
                )
                rollback_flag = eoss_object_client.rollback()
                eoss_object_client.close_mds()
                eoss_object_client.remove_lock()

                if rollback_flag:
                    return ("EOSS Rollback Done", 526)
                else:
                    return ("EOSS Rollback Failed", 527)
            else:
                log.info(
                    f"renamed temp file to final file for object {eoss_object_client.object_name}"
                )

            # set up object size
            eoss_object_client.set_object_size()
            log.info(f"set size for object {eoss_object_client.object_name}")

            # set up latest update timestamp
            eoss_object_client.set_object_timestamp()
            log.info(f"set timestamp for object {eoss_object_client.object_name}")

            # state 0 phase
            try:
                eoss_object_client.set_object_state(0)
            except Exception as e:
                log.error(
                    f"failed to set object state 0 for object {eoss_object_client.object_name}: {e}"
                )
                rollback_flag = eoss_object_client.rollback()
                eoss_object_client.close_mds()
                eoss_object_client.remove_lock()

                if rollback_flag:
                    return ("EOSS Rollback Done", 526)
                else:
                    return ("EOSS Rollback Failed", 527)
            else:
                log.info(
                    f"object {eoss_object_client.object_name} is saved and metadata database is updated in final state"
                )

            eoss_object_client.remove_lock()
            return ("Object Uploaded", 201)
        else:
            eoss_object_client.close_mds()
            eoss_object_client.remove_lock()

            if object_exists_flag == 1:
                return ("Object Initialized Only", 440)
            if object_exists_flag == 2:
                return ("Object Saved Not Closed", 441)
            if object_exists_flag == 3:
                return ("Object MDS Closed Not In Local", 524)


@app.route("/eoss/v1/stats", methods=["GET"])
def get_eoss_object_stats():
    if request.method != "GET":
        return ("Bad Method", 405)

    output = {}
    mds = mds_client.MDSClient()

    try:
        mds.connect()
    except MDSConnectException as e:
        log.error(f"failed to connect to metadata database: {e}")
        return ("MDS Connection Failure", 520)

    mds.cursor()

    # get total number of objects
    try:
        mds_output = mds.execute(
            f"SELECT COUNT(id) FROM {METADATA_DB_TABLE}"
        ).fetchall()
    except MDSExecuteException as e:
        log.error(f"failed to execute SQL query: {e}")
        return ("MDS Execution Failure", 520)
    else:
        total_number_objects = mds_output[0][0]
        output["total_number_objects"] = total_number_objects

    # get total storage usage
    # unit: byte
    try:
        mds_output = mds.execute(
            f"SELECT SUM(size) FROM {METADATA_DB_TABLE}"
        ).fetchall()
    except MDSExecuteException as e:
        log.error(f"failed to execute SQL query: {e}")
        return ("MDS Execution Failure", 520)
    else:
        total_storage_usage = mds_output[0][0]

        if total_storage_usage is None:
            total_storage_usage = 0

        output["total_storage_usage"] = total_storage_usage

    # get youngest and oldest timestamps for objects
    try:
        mds_output = mds.execute(
            f"SELECT DISTINCT MIN(timestamp) FROM {METADATA_DB_TABLE} UNION ALL SELECT DISTINCT MAX(timestamp) FROM {METADATA_DB_TABLE}"
        ).fetchall()
    except MDSExecuteException as e:
        log.error(f"failed to execute SQL query: {e}")
        return ("MDS Execution Failure", 520)
    else:
        youngest_object_updated_timestamp = mds_output[0][0]
        oldest_object_updated_timestamp = mds_output[1][0]
        output["youngest_object_updated_timestamp"] = youngest_object_updated_timestamp
        output["oldest_object_updated_timestamp"] = oldest_object_updated_timestamp

    # get object state stats
    try:
        mds_output = mds.execute(
            f"SELECT COUNT(state) FROM {METADATA_DB_TABLE} WHERE STATE = 0 UNION ALL SELECT COUNT(state) FROM {METADATA_DB_TABLE} WHERE STATE = 1 UNION ALL SELECT COUNT(state) FROM {METADATA_DB_TABLE} WHERE STATE = 2"
        ).fetchall()
    except MDSExecuteException as e:
        log.error(f"failed to execute SQL query: {e}")
        return ("MDS Execution Failure", 520)
    else:
        object_uploaded = mds_output[0][0]
        object_uploading_init = mds_output[1][0]
        object_saved_in_temp_name = mds_output[2][0]
        output["number_object_uploaded"] = object_uploaded
        output["number_object_upload_init"] = object_uploading_init
        output["number_object_saved_in_temp_name"] = object_saved_in_temp_name

    mds.close()

    return (jsonify(output), 200)


@app.before_request
def before_request():
    g.start = time.time()


@app.after_request
def after_request(response):
    # set up response headers
    request_id = utils.set_request_id()
    response.headers["X-EOSS-Request-ID"] = request_id

    # record response latency
    # unit: ms
    latency = int((time.time() - g.start) * 1000)

    # log request access information
    access_log.info(
        f"{request_id} {latency} {request.remote_addr} {request.method} {request.path} {response.status_code} {request.user_agent}"
    )

    return response


@app.errorhandler(Exception)
def internal_error(exception):
    if hasattr(exception, "description"):
        message = exception.description
    else:
        message = "uncaught exception"

    if hasattr(exception, "code"):
        code = exception.code
    else:
        code = 500

    log.exception(message)

    return (message, code)


# rewrite all other HTTP response code 404 to 403
@app.errorhandler(404)
def return_403(error):
    return ("", 403)
