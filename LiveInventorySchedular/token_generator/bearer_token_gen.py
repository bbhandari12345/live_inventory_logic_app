import string
from LiveInventorySchedular.config import Config
from .tokenbase import *
from LiveInventorySchedular.orm.li_vendor_config import LIVendorsConfig
from LiveInventorySchedular.orm.li_vendors import LIVendors

import json
import requests
from flatten_dict import flatten, unflatten


class TokenGeneratorArgsException(Exception):
    pass


class TemplatingException(Exception):
    pass


class APIDataException(Exception):
    pass


class DataDispatchError(Exception):
    pass


class BearerTokenGenerator(TokenGenBase):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        # Check if all the mandatory parameters are present
        mandatory_params = ['vendor_id', 'config_file_path', 'template_values']
        for param in mandatory_params:
            if not param in kwargs:
                raise TokenGeneratorArgsException("Mandatory parameter '{}' not found".format(param))

    def fetch_config(self) -> Base:
        """
        Read the config file template and construct final config object out of it
        """

        # Read the config template
        self.read_config()

        # Load other values
        try:
            template_values = self.kwargs.get('template_values')

            if template_values is None or len(template_values) == 0:
                raise TokenGeneratorArgsException("No template_values passed")

        except Exception as ex:
            raise UC_ConfigReadException(ex)

        # Use the template_values and item_codes to construct the final config object
        self.request_config = self.__create_config(self.config_template, template_values)

        return self

    def fetch_vendor_data(self) -> Base:
        """
        Make the API call and get the data from vendor API
        """

        try:
            vendor_id = self.kwargs.get('vendor_id')
            # 1. Prepare/Get data for the request
            self.logger.info("Sending request, Generating bearer token data from vendor API")

            self.logger.debug("Flattening the config json for easier reading")
            flat_request_config = flatten(self.request_config, reducer='dot')

            self.logger.debug("Reading values needed for creating request")
            req_url = flat_request_config.get('token_generator_template.url.raw')
            req_method = flat_request_config.get('token_generator_template.url.method')
            req_headers_raw = flat_request_config.get('token_generator_template.header')
            req_url_query_raw = flat_request_config.get('token_generator_template.url.query')
            req_url_xwww_token_data = flat_request_config.get('token_generator_template.token_data.x-www-url')
            req_body = json.dumps(self.request_config.get('data'))

            # x-www-form-uni-coded support
            if req_url_xwww_token_data is not None:
                tmp_req_url_xwww = req_url_xwww_token_data
                tmp_req_lst = []
                for i in tmp_req_url_xwww:
                    for k, v in i.items():
                        if k == 'key':
                            tmp_req = f"{v}"
                        if k == 'value':
                            tmp_req += f"={v}"
                    tmp_req_lst.append(tmp_req)
                req_body = "&".join(tmp_req_lst)

            if isinstance(self.request_config.get('data'), str):
                req_body = self.request_config.get('data')

            self.logger.debug("Creating collection/dictionaries for request")
            req_header = {}
            for obj in req_headers_raw:
                req_header[obj['key']] = obj['value']

            req_url_params = {}
            for obj in req_url_query_raw:
                req_url_params[obj['key']] = obj['value']

        except Exception as ex:
            self.logger.error("Could not prepare the REST request")
            raise APIDataException("Error preparing data for REST request")

        try:
            # 2. Make API Request
            self.logger.info("Making API Request")
            response = requests.request(method=req_method,
                                        url=req_url,
                                        data=req_body,
                                        headers=req_header,
                                        verify=False,
                                        timeout=Config.REQUEST_TIMEOUT,
                                        params=req_url_params)

            if response.status_code not in range(200, 210):
                self.logger.error("Could not get data from token generator,. API returned HTTP Status code: {}".format(
                    response.status_code))

            # ensure the response text is json, if yes, flatten it.
            self.logger.debug("Checking if the response is valid JSON")
            tmp_flat_data = []
            auth_key_path = flat_request_config.get('auth_key_update', None)
            if auth_key_path is None:
                self.logger.warn("Authorization key update path not specified")
            try:
                tmp = json.loads(response.text)
                tmp.update({'vendor_id': vendor_id, 'field_to_update': auth_key_path})
                if isinstance(tmp.get("token_type"), str):
                    tmp.update({auth_key_path: (tmp.get('token_type') or "Bearer") + ' ' + tmp.get('access_token')})
                else:
                    tmp.update({auth_key_path: "Bearer" + ' ' + tmp.get('token')})

            except ValueError as ex:
                self.logger.error("The response is not a valid JSON")
                raise APIDataException("The received response is not a valid JSON")
            # TODO: This should ideally be a flattened JSON so that a generic implementation of extractor is easy
            self.data = tmp
            # self.create_auth_data = self.__auth_data(self.config_template, self.data)

        except Exception as ex:
            self.logger.error("Could not get data from the API request")
            raise APIDataException("Could not get data from the API")

        return self

    def __create_config(self, config_template, template_values) -> string:
        """
        Creates the config object based on the template and the inputs passed
        """
        try:
            self.logger.debug("Creating configuration object from template")
            template_str = json.dumps(config_template)

            self.logger.debug("Replacing template values")
            for k, v in template_values.items():
                template_str = template_str.replace(f'<<{k}>>', v)

            # Load/Replace item codes, we will flatten the dictionary, change the values and unflatten it
            config_template = json.loads(template_str)
            flat_config = flatten(config_template, reducer='dot')

            config_template = unflatten(flat_config, splitter='dot')
            return config_template
        except Exception as ex:
            self.logger.error("Could not create configuration object.")
            self.logger.error(ex)
            raise TokenGeneratorArgsException(ex)

    def load_data(self) -> Any:

        try:
            self.logger.info("loading data token generator")
            self.logger.debug("Reading token generator data")
            if self.data is not None:
                self.update_data = self.data
                # raise TokenUpdaterArgsException("Mandatory parameter '{}' not found".format(kwargs))

        except Exception as ex:
            self.logger.error("Could not dispatch data")
            self.logger.exception(ex)
            raise DataDispatchError("Could not update token")

        return self

    def dispatch(self):
        # for data in self.update_data:
        vendors_auth = LIVendorsConfig(self.update_data)
        vendors_table = LIVendors(self.update_data)
        data = self.update_data
        try:
            vendors_auth.load(partial=True)
            vendors_table.load(partial=True)
            # vendors_auth.update(row_value=self.update_data['Authorization'], identifier='vendor_id')
            vendors_auth.updateAuthorization(data)
            vendors_table.update_token_expiry(data)
            # return inventory().one(data['vendor_code']), 200

        except Exception as err:
            raise DataDispatchError("Could not update data")
            # raise err

        return self
