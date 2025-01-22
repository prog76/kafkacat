
import json
import logging
import sys


def setup_logging(verbose, log_format):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    root_logger.handlers = []
    handler = logging.StreamHandler(stream=sys.stderr)
    if log_format == "json":
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        record.msg = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = {
            "level": record.levelname,
            "time": record.asctime,
            "message": record.msg,
        }
        return json.dumps(s)
