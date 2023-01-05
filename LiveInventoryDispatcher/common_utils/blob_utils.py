"""
this module holds all function regarding blob storage
"""
from LiveInventoryExtractor.config import Config
from azure.storage.blob import BlobServiceClient
from typing import Union
import logging as logger
import os
import json
import uuid


def connect_blob() -> Union[None, BlobServiceClient]:
    blob_service_client = None
    try:
        conn_str = Config.AZURE_STORAGE_CONNECTION_STRING
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    except Exception as e:
        logger.error(e, exc_info=True)
    return blob_service_client


def upload_file_blob(file_name, data) -> bool:
    """
    this function upload data to the blob
    :param filename:
    :param data:
    :return:
    """
    try:
        random_uuid = uuid.uuid1()
        data_file_path = os.path.join(
            str(file_name) + "_" + str(random_uuid) + ".json"
        )
        data_file_dir = Config.data_file_path
        if not data_file_dir:
            raise Exception("data file path dir not defined")
        blob_service_client = connect_blob()
        if not blob_service_client:
            raise Exception(
                "Data file path does not exist or is inaccessible"
            )
        container = Config.BLOB_CONTAINER_NAME + Config.BLOB_NAME + data_file_dir
        blob_client = blob_service_client.get_blob_client(
            container=container,
            blob=data_file_path
        )
        blob_client.upload_blob(str(data))
        return True
    except Exception as exe:
        logger.error(exe)
        logger.error(f"failed to upload data to the blob", exc_info=True)
        return False
