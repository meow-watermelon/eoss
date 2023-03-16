import base64
from . import VERSION_SALT

def set_object_name(object_filename, version_string=None):
    """
    construct object name by using base64
    1. if version_string is valid, the object name text is object_filename:VERSION_SALT:version_string
    2. if version_string is invalid, the object name text is the same as original object_filename
    """
    if not version_string:
        object_name = base64.b64encode(object_filename.encode()).decode()
    else:
        object_name_plain = ":".join((object_filename, VERSION_SALT, version_string))
        object_name = base64.b64encode(object_name_plain.encode()).decode()

    return object_name

def decode_object_name(object_name):
    """
    return plain text of decoded object name string
    """
    return base64.b64decode(object_name.encode()).decode()
