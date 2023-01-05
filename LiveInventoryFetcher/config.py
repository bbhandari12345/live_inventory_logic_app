"""
this module reads all config value from config.ini file
"""
# importing necessary library
import configparser
from LiveInventoryFetcher.constants import CONFIG_FILE_PATH

# handler for config parser
config = configparser.RawConfigParser()
# read the config.ini file
config.read(CONFIG_FILE_PATH)


class Config:
    """
    this class handles the stuff related to config.ini file sections
    """
    DB_CONFIG = dict(config['dbconfig'])

    NETWORK_CONFIG = dict(config['network_filepath'])
    data_file_path = NETWORK_CONFIG.get('data_file_path')

    BLOB_URL = "https://savapi.blob.core.windows.net/stage?sp=r&st=2023-01-04T10:57:22Z&se=2023-01-04T18:57:22Z&spr" \
               "=https&sv=2021-06-08&sr=c&sig=eXydEpyYoS%2BgoMt037X4tkyd4lPJoKvmntJchLeEDtQ%3D"
    BLOB_CONTAINER_NAME = "stage/"
    BLOB_NAME = "live-inventory/"
    IS_BLOB = True
    AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=savapi;AccountKey" \
                                      "=U7AF6GQLjl584h3HON2OQnA0YZIN86y32Jq3yRb1DvOGZ0nIm+1u6syEwaqsls01WRkMzpFmshyJ" \
                                      "+AStNWC0tQ==;EndpointSuffix=core.windows.net"

    EXTRA = dict(config['extra'])
    REQUEST_TIMEOUT = int(EXTRA.get('request_timeout'))
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
