import string
from LiveInventoryFetcher.base.base import Base, UC_ConfigReadException
from LiveInventoryFetcher.fetcher.fetcherbase import FetcherBase
from LiveInventoryFetcher.config import Config
import csv
import json
import requests
from requests.auth import HTTPDigestAuth

from flatten_dict import flatten, unflatten


class RESTCSVFetcherArgsException(Exception):
    pass


class TemplatingException(Exception):
    pass


class CSVDataException(Exception):
    pass


class CSVFetcher(FetcherBase):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        # Check if all the mandatory parameters are present
        self.single_item = False
        self.response_info = {
            "vendor_id": None,
            "response_text": None,
            "response_code": None
        }
        mandatory_params = ['vendor_id', 'config_file_path', 'template_values', 'data_file_path']
        for param in mandatory_params:
            if not param in kwargs:
                raise RESTCSVFetcherArgsException("Mandatory parameter '{}' not found".format(param))

    def fetch_config(self) -> Base:
        """
        Read the config file template and construct final config object out of it
        """

        # Read the config template
        self.read_config()
        # Load other values
        try:
            template_values = self.kwargs.get('template_values')
            self.csv_file_path = self.kwargs.get('data_file_path')

            if template_values is None or len(template_values) == 0:
                raise RESTCSVFetcherArgsException("No template_values passed", exc_info=True)
            if self.csv_file_path is None:
                raise RESTCSVFetcherArgsException("No CSV file path found", exc_info=True)

        except Exception as ex:
            raise UC_ConfigReadException(ex, exc_info=True)

        # Use the template_values and item_codes to construct the final config object
        self.request_config = self.__create_config(self.config_template, template_values)
        return self

    def fetch_vendor_data(self) -> Base:
        """
        Make the API call and get the data from vendor API
        """
        try:
            # 1. Prepare/Get data for the request
            self.logger.info("Fetching data from CSV file server")
            self.logger.debug("Flattening the config json for easier reading")
            flat_request_config = flatten(self.request_config, reducer='dot')

            self.logger.debug("Reading values needed for creating request")
            req_url = flat_request_config.get('api_request_template.url.raw')
            req_method = flat_request_config.get('api_request_template.url.method')
            req_headers_raw = flat_request_config.get('api_request_template.header')
            req_url_query_raw = flat_request_config.get('api_request_template.url.query')
            data_list_path = flat_request_config.get('data_list_path')
            csv_delimiter = flat_request_config.get('delimiter', None)
            csv_encoding = flat_request_config.get('encoding', None)
            req_body = flat_request_config.get('data', None)
            authentication_required = flat_request_config.get('api_request_template.url.auth_required', None)
            self.logger.debug("Creating collection/dictionaries for request")
            req_header = {}
            if req_url_query_raw is not None:
                for obj in req_headers_raw:
                    req_header[obj['key']] = obj['value']

            req_url_params = {}
            if req_url_query_raw is not None:
                for obj in req_url_query_raw:
                    req_url_params[obj['key']] = obj['value']

            if authentication_required is True:
                self.logger.info("checking if request need to be sent for CSV")
                # self.logger.info("Creating JSON file")
                response = requests.request(method=req_method,
                                            url=req_url,
                                            timeout=Config.REQUEST_TIMEOUT,
                                            auth=HTTPDigestAuth(req_header.get('username'),
                                                                req_header.get('password')))
                self.response_info["vendor_id"] = self.kwargs.get('vendor_id')
                self.response_info["response_code"] = response.status_code
                if response.status_code not in range(200, 210):
                    self.response_info["response_text"] = response.reason
                    self.logger.error(
                        "Could not get data from vendor API. API returned HTTP Status code: {}".format(
                            response.status_code))
                    raise CSVDataException("The response from vendor API is not valid")

                with open(self.csv_file_path, 'wb') as f:
                    f.write(response.content)

        except Exception as ex:
            self.logger.error("Could not get data from CSV request", exc_info=True)
            raise CSVDataException("Error preparing data from CSV request")

        try:
            self.logger.info("Reading CSV file from path")
            with open(self.csv_file_path, 'r', encoding=csv_encoding) as csvf:
                # load csv file data using csv library's dictionary reader and replace null values
                # csvReader = csv.DictReader(csvf, delimiter=csv_delimiter)
                csvReader = csv.DictReader((line.replace('\0', '').replace('\x00', '') for line in csvf),
                                           delimiter=csv_delimiter)
                jsonArray = []
                for row in csvReader:
                    jsonArray.append(row)
            self.data = json.dumps(jsonArray)  # response.text
        except Exception as ex:
            self.logger.error("The response is not a valid JSON", exc_info=True)
            raise CSVDataException("The received response is not a valid JSON")
        return self

    def __create_config(self, config_template, template_values) -> string:
        """
        Creates the config object based on the template and the inputs passed
        """
        try:
            self.logger.info("Creating configuration object from template")
            template_str = json.dumps(config_template)

            self.logger.info("Replacing template values")
            for k, v in template_values.items():
                template_str = template_str.replace(f'<<{k}>>', v)

            # Load/Replace item codes, we will flatten the dictionary, change the values and unflatten it
            config_template = json.loads(template_str)
            flat_config = flatten(config_template, reducer='dot')
            csv_delimiter = config_template.get('delimiter', None)
            csv_encoding = config_template.get('encoding', None)

            config_template = unflatten(flat_config, splitter='dot')
            return config_template
        except Exception as ex:
            self.logger.error("Could not create configuration object.", exc_info=True)
            self.logger.error(ex)
            raise RESTCSVFetcherArgsException(ex)
