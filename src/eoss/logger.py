import logging
import logging.handlers
from . import LOG_BACKUP_COUNT
from . import LOG_MAX_BYTES


class Logger:
    def __init__(self, name, log_filename):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.channel = logging.handlers.RotatingFileHandler(
            log_filename,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
        )
        self.channel.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        self.logger.addHandler(self.channel)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def exception(self, msg):
        self.logger.exception(msg)


class AccessLogger(Logger):
    def __init__(self, name, log_filename):
        super().__init__(name, log_filename)
        self.channel.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
