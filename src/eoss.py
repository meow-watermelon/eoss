#!/usr/bin/env python3

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
from flask import Flask
from flask import g
from flask import jsonify
from flask import request
from werkzeug.serving import WSGIRequestHandler

# set up loggers
log = logger.Logger(__name__, LOGGING_PATH + "/" + "eoss.log")
access_log = logger.AccessLogger("access_log", LOGGING_PATH + "/" + "access.log")

app = Flask(__name__)


@app.route(
    "/eoss/v1/object/<string:object_filename>", methods=["GET", "HEAD", "DELETE", "PUT"]
)
def process_object(object_filename):
    if request.method not in ("GET", "HEAD", "DELETE", "PUT"):
        return ("Bad Request", 400)

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

    log.info(f"object_filename: {object_filename} object_version: {object_version}")

    # initialize object client
    eoss_object_client = object_client.ObjectClient(
        object_filename, object_version=object_version
    )
    try:
        eoss_object_client.init_mds()
    except MDSConnectException as e:
        log.error(f"failed to connect to metadata database: {str(e)}")
        return ("MDS Connection Failure", 520)

    if request.method == "HEAD":
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

    if request.method == "GET":
        pass

    if request.method == "DELETE":
        pass

    if request.method == "PUT":
        pass


@app.route("/eoss/v1/stats", methods=["GET"])
def get_eoss_object_stats():
    if request.method != "GET":
        return ("Bad Request", 400)

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
        log.info(f"{mds_output}")
        youngest_object_updated_timestamp = mds_output[0][0]
        oldest_object_updated_timestamp = mds_output[1][0]
        output["youngest_object_updated_timestamp"] = youngest_object_updated_timestamp
        output["oldest_object_updated_timestamp"] = oldest_object_updated_timestamp

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


if __name__ == "__main__":
    # enable HTTP/1.1
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
