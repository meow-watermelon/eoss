#!/usr/bin/env python3

import time
from eoss import logger
from eoss import object_client
from eoss import utils
from eoss import LOGGING_PATH
from eoss import STORAGE_PATH
from eoss.exceptions import MDSConnectException
from eoss.exceptions import MDSExecuteException
from eoss.exceptions import MDSCommitException
from eoss.exceptions import EOSSInternalException
from flask import Flask
from flask import g
from flask import request
from werkzeug.serving import WSGIRequestHandler

# set up loggers
log = logger.Logger(__name__, LOGGING_PATH + "/" + "eoss.log")
access_log = logger.AccessLogger("access_log", LOGGING_PATH + "/" + "access.log")

app = Flask(__name__)


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
