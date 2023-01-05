import string
from LiveInventoryFetcher.base.base import Base, UC_ConfigReadException
from LiveInventoryFetcher.fetcher.fetcherbase import FetcherBase
from LiveInventoryFetcher.transformer import transformer
import json
from LiveInventoryFetcher.orm import li_vendors
import ftplib
import zipfile
import os
from LiveInventoryFetcher.config import Config
from flatten_dict import flatten, unflatten

DATA_FILE_PATH = Config.BLOB_URL + "/" + Config.BLOB_CONTAINER_NAME + Config.BLOB_NAME + \
                 Config.NETWORK_CONFIG.get('data_file_path')


class RESTCSVFetcherArgsException(Exception):
    pass


class TemplatingException(Exception):
    pass


class FTPDataException(Exception):
    pass


class FTPFileNotFoundException(Exception):
    pass


class FTPFetcher(FetcherBase):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.request_config = None
        self.response_info = {
            "vendor_id": self.kwargs.get('vendor_id'),
            "response_text": None,
            "response_code": None
        }

        # Check if all the mandatory parameters are present
        self.single_item = False
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
        # self.read_csv()
        # Load other values
        try:
            self.template_values = self.kwargs.get('template_values')
            self.data_file_path = self.kwargs.get('data_file_path')

            if self.template_values is None or len(self.template_values) == 0:
                raise RESTCSVFetcherArgsException("No template_values passed")
            if self.data_file_path is None:
                raise RESTCSVFetcherArgsException("No CSV file path found")

        except Exception as ex:
            raise UC_ConfigReadException(ex)

        # Use the template_values and item_codes to construct the final config object
        self.request_config = self.__create_config(self.config_template, self.template_values)
        return self

    def dispatch_ftp_response(self, res_code, res_text):
        self.response_info["response_code"] = res_code
        self.response_info["response_text"] = res_text
        my_vendors_1 = li_vendors.LIVendors()
        my_vendors_1.update_response_code_text_in_vendors(self.response_info)
        self.logger.info(f"***********SUCCESS -update vendors table **************")
        return self

    def fetch_vendor_data(self) -> Base:
        """
        Make the API call and get the data from vendor API
        """
        try:
            # 1. Prepare/Get data for the request
            self.logger.info("Fetching data from FTP server")

            self.logger.debug("Flattening the config json for easier reading")
            flat_request_config = flatten(self.request_config, reducer='dot')

            self.logger.debug("Reading values needed for creating request")
            url = flat_request_config.get('ftp-request-template.url')
            username = self.template_values.get('username')
            password = self.template_values.get('password')
            zipFile = flat_request_config.get('zip_file', None)
            filename = flat_request_config.get('file_name', None)
            file_type = flat_request_config.get('file_type')
            csv_delimiter = flat_request_config.get('csv_delimiter', None)
            csv_encoding = flat_request_config.get('encoding', None)
            port = flat_request_config.get('port', None)
            req_body = flat_request_config.get('data', None)
            authentication_required = flat_request_config.get('ftp-request-template.url.auth_required', None)
            self.logger.debug("Creating FTP for request")

            try:
                with ftplib.FTP() as ftp:
                    file_copy = zipFile or filename
                    if port:
                        ftp.connect(url, port)
                    else:
                        ftp.connect(url)
                    ftp.login(username, password)
                    if zipFile:
                        localfile = open(DATA_FILE_PATH + "/" + zipFile, 'wb')
                    else:
                        localfile = open(self.data_file_path, 'wb')

                    if file_copy not in ftp.nlst():
                        self.logger.warn(f"File {file_copy} not found in FTP server.")
                        localfile.close()
                        ftp.quit()
                        self.dispatch_ftp_response(404, "File not available in the server")
                        raise FTPFileNotFoundException(f'File {file_copy} not available in FTP server')
                    # ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
                    res = ftp.retrbinary('RETR ' + file_copy, localfile.write)

                    self.dispatch_ftp_response(200, None)
                    if not res.startswith('226 Transfer complete'):
                        self.logger.error('Download failed')
                        self.dispatch_ftp_response(400, "download failed")
                    if os.path.isfile(DATA_FILE_PATH + file_copy):
                        self.logger.info("file transferred successfully")
                        self.dispatch_ftp_response(200, None)

                    localfile.close()
                    ftp.quit()

            except ftplib.all_errors as e:
                self.logger.error('FTP error:', e)
                self.dispatch_ftp_response(400, "FTP error")
                raise e

            if zipFile is not None:
                try:
                    # sleep(3)
                    if os.path.isfile(DATA_FILE_PATH + zipFile):
                        self.logger.info("file found")
                        with zipfile.ZipFile(DATA_FILE_PATH + zipFile, 'r') as zip_ref:
                            listOfFileNames = zip_ref.namelist()
                            for fileName in listOfFileNames:
                                zip_ref.extract(fileName, path=DATA_FILE_PATH)
                                filename = fileName
                            # zip_ref.extractall(DATA_FILE_PATH)
                            self.logger.info("extracting zip file")
                        json_data = transformer(DATA_FILE_PATH + filename, file_type, csv_encoding, csv_delimiter)
                except Exception as e:
                    self.logger.error("zip file exception: ", e)
                    self.dispatch_ftp_response(400, "zip file exception")
                    raise FTPDataException("Zip file not found")
            else:
                json_data = transformer(self.data_file_path, file_type, csv_encoding, csv_delimiter)
                self.dispatch_ftp_response(200, None)
            self.data = json_data

        except FTPFileNotFoundException as nex:
            # Write empty json file in case there was no data in FTP Server
            self.logger.warn("FTP File not found. Nothing to do.")
            self.data = "{}"
            self.dispatch_ftp_response(400, "response is not a valid")

        except Exception as ex:
            self.logger.error(ex, exc_info=True)
            self.logger.error("The response from ftp is not valid")
            self.dispatch_ftp_response(400, "response is not a valid")
            raise FTPDataException("The received response is not a valid")
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
                if v.find('"') > 1:
                    v = v.replace('"', '\'')
                template_str = template_str.replace(f'<<{k}>>', v)

            # Load/Replace item codes, we will flatten the dictionary, change the values and unflatten it
            config_template = json.loads(template_str)
            flat_config = flatten(config_template, reducer='dot')
            csv_delimiter = config_template.get('delimiter', None)
            csv_encoding = config_template.get('encoding', None)

            config_template = unflatten(flat_config, splitter='dot')
            return config_template
        except Exception as ex:
            self.logger.error("Could not create configuration object.")
            self.logger.error(ex)
            raise RESTCSVFetcherArgsException(ex)
