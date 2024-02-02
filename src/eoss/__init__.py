import yaml

__version__ = "0.0.4"


def read_config(config_file):
    """
    read YAML format configuration file
    """
    config = {}

    try:
        with open(config_file, "rt") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
    except:
        pass

    return config


config_file_path = "/home/ericlee/Projects/git/eoss/config/eoss.yaml"

SETTINGS = read_config(config_file_path)

# populate eoss settings
VERSION_SALT = SETTINGS.get("VERSION_SALT", "snoopy")
STORAGE_PATH = SETTINGS.get("STORAGE_PATH", "/tmp")
METADATA_DB_PATH = SETTINGS.get("METADATA_DB_PATH", "/tmp/mds.sql")
METADATA_DB_TABLE = SETTINGS.get("METADATA_DB_TABLE", "metadata")
LOGGING_PATH = SETTINGS.get("LOGGING_PATH", "/tmp")
OBJECT_LOCK_PATH = SETTINGS.get("OBJECT_LOCK_PATH", "/tmp")
LOG_BACKUP_COUNT = SETTINGS.get("LOG_BACKUP_COUNT", 10)
LOG_MAX_BYTES = SETTINGS.get("LOG_MAX_BYTES", 1073741824)
SAFEMODE = SETTINGS.get("SAFEMODE", False)
