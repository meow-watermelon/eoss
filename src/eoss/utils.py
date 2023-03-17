import uuid


def set_request_id():
    """
    generate a unique request id for incoming request
    """
    return str(uuid.uuid4())
