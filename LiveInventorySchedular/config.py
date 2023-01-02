"""
this module reads all config value from config.ini file
"""
# importing necessary library
import configparser
from LiveInventorySchedular.constants import CONFIG_FILE_PATH

# handler for config parser
config = configparser.ConfigParser()
# read the config.ini file
config.read(CONFIG_FILE_PATH)


class Config:
    """
    this class handles the stuff related to config.ini file sections
    """
    DB_CONFIG = dict(config['dbconfig'])
    NETWORK_CONFIG = dict(config['network_filepath'])
    EXTRA = dict(config['extra'])
    TIME_INTERVAL_DELETE_INVALID_RECORD_INVENTORY = EXTRA.get('TIME_INTERVAL_DELETE_INVALID_RECORD_INVENTORY')
    LOGGER_CONFIG = {'nativelogger': {
        'filehandler': {
            'maxbytes': 10485760,
            'maxbackupcount': 5,
            'logrotate': {
                'when': 'h',
                'at': 1,
                'utc': False,
                'interval': 1
            },
            'logdir': 'logs',
            'level': 'DEBUG',
            'format': "%(levelname)s [%(asctime)s] %(filename)s %(funcName)s: %(message)s"
        },
        'consolehandler': {
            'level': 'INFO',
            'format': "%(levelname)s [%(asctime)s] %(filename)s %(funcName)s : %(message)s"
        }
    }, 'log2cloudwatch': {
        'level': 'DEBUG',
        'format': "%(levelname)s [%(asctime)s] %(filename)s %(funcName)s : %(message)s"
    }
    }
