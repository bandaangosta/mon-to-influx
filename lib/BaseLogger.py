import sys
import time
import logging
from logging.handlers import RotatingFileHandler, WatchedFileHandler

LOG_FILE = '/var/log/mon_to_influx.log'
FORMAT_BASE = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d:%(funcName)s() - %(message)s'

def createLogger(name, format=FORMAT_BASE, rotator=False):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if rotator:
        handler = RotatingFileHandler(LOG_FILE, maxBytes=50000000, backupCount=5)
    else:
        handler = WatchedFileHandler(LOG_FILE)
    formatter = logging.Formatter(format)
    formatter.converter = time.gmtime  # if you want UTC time
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


def getStdoutHandler(format=FORMAT_BASE):
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setLevel(logging.DEBUG)
    formatter = logging.Formatter(format)
    formatter.converter = time.gmtime  # if you want UTC time
    stdout.setFormatter(formatter)
    return stdout
