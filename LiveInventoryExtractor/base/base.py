from abc import ABC, abstractmethod
from enum import Enum
from typing import Any
from LiveInventoryExtractor.config import Config
from LiveInventoryExtractor.common_utils.blob_utils import connect_blob
import os
import uuid
import json
import logging
import requests


class UC_DataException(Exception):
    pass


class UC_ConfigReadException(Exception):
    pass


class ObjectType(Enum):
    FETCHER = "FETCHER"
    EXTRACTOR = "EXTRACTOR"
    DISPATCHER = "DISPATCHER"
    GENERATOR = "GENERATOR"


class Base(ABC):
    """
    Base class to derive all other class extractor, fetcher and dispatcher from
    """

    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.config_template = None
        self.object_type = None  # Member variable to store current object type
        self.kwargs = kwargs
        self.data = None  # Member variable to store payload
        self.meta = {}  # Member variable to store meta data about payload and the object
        # get the logger instance
        self.logger = logging

    @abstractmethod
    def execute(self) -> Any:
        """
        The entry point for this class. All derived classes must implement 
        concrete implementation of this method
        """
        pass

    def read_config(self) -> Any:
        try:
            with requests.get(self.kwargs.get('config_file_path')) as config_file:
                self.config_template = json.loads(config_file.text)

        except Exception as ex:
            raise UC_ConfigReadException(ex)

    def write(self, **kwargs) -> Any:
        # Check for potential errors
        if self.data is None:
            raise UC_DataException("Data not populated")

        if self.data == "":
            raise UC_DataException("Data is empty")

        if 'data_file_dir' in kwargs.keys():
            data_file_dir = kwargs.get('data_file_dir')
        else:
            raise NotImplementedError("Implementation for 'write' method not present")

        if data_file_dir is None:
            raise UC_DataException("Data file path is None")

        if Config.IS_BLOB:
            blob_service_client = connect_blob()
            if not blob_service_client:
                raise Exception(
                    "Data file path does not exist or is inaccessible"
                )
        else:
            if not os.path.exists(data_file_dir):
                raise UC_DataException("Data file path does not exist or is inaccessible")

            if not os.path.isdir(data_file_dir):
                raise UC_DataException("Data file path is not a directory")

        # Write the vendor data to file
        try:
            file_name = uuid.uuid1()
            if Config.IS_BLOB:
                data_file_path = os.path.join(
                    str(file_name) + ".json"
                )

                container = Config.BLOB_CONTAINER_NAME + Config.BLOB_NAME + data_file_dir
                blob_client = blob_service_client.get_blob_client(
                    container=container,
                    blob=data_file_path
                )
                blob_client.upload_blob(str(self.data))
            else:
                data_file_path = os.path.join(data_file_dir, str(file_name) + ".json")
                with open(data_file_path, 'w') as outfile:
                    outfile.write(self.data)

            # Save the write path
            self.meta[self.object_type.value.lower() + "_data_file_path"] = data_file_path
            return self
        except Exception as ex:
            raise UC_DataException(ex)
