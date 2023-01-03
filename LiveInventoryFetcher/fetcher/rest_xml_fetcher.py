import string
from LiveInventoryFetcher.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventoryFetcher.base.base import Base, UC_ConfigReadException
from LiveInventoryFetcher.common_utils.db_queries import QUERY_CHECK_IF_VENDOR_CODE_EXISTS_IN_VENDOR_CODES_TABLE
from LiveInventoryFetcher.fetcher.fetcherbase import FetcherBase
from LiveInventoryFetcher.config import Config
import xmltodict
import xml.etree.ElementTree as ET
import re
import copy
from LiveInventoryFetcher.orm.li_vendor_codes import LIVendorCodes

import json
import requests
from flatten_dict import flatten, unflatten


FETCHER_FILE_PATH = Config.NETWORK_CONFIG.get('fetcher_directory_path')


class RESTXMLFetcherArgsException(Exception):
    pass


class TemplatingException(Exception):
    pass


class APIDataException(Exception):
    pass


def safeget(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            print("DEBUG: Dictionary: {}\nKey:{}".format(dct, key))
            return None
    return dct


class RESTXMLFetcher(FetcherBase):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)
        self.request_in_api_request_template_query = False
        self.response = None
        self.response = None
        self.multi_body = False
        self.response_info = {
            "vendor_id": None,
            "response_text": None,
            "response_code": None
        }
        self.multi_req_url_params = []
        self.response_bodies = []
        self.item_codes = []
        self.error_field_mapping = {}
        # Check if all the mandatory parameters are present
        self.single_item = False
        self.summary['FailedBatches'] = 0
        mandatory_params = ['vendor_id', 'config_file_path', 'item_codes', 'template_values']
        for param in mandatory_params:
            if not param in kwargs:
                raise RESTXMLFetcherArgsException("Mandatory parameter '{}' not found".format(param))

    def fetch_config(self) -> Base:
        """
        Read the config file template and construct final config object out of it
        """

        # Read the config template
        self.read_config()
        vendor_id = self.kwargs.get('vendor_id')
        self.item_codes = self.kwargs.get('item_codes')

        if self.config_template.get("mapping") and self.config_template.get("mapping").get("vendor_code_table"):
            error_mapping_list = self.config_template.get("mapping").get("vendor_code_table")
            self.error_field_mapping = {fld_map.get('destination_field'): fld_map.get('source_field') for fld_map in error_mapping_list}

        invalid_vendor_codes = []
        # Load other values
        try:
            with self.li_db.transaction(auto_commit=True) as query_set:
                query_check = query_set.execute_query(QUERY_CHECK_IF_VENDOR_CODE_EXISTS_IN_VENDOR_CODES_TABLE,
                                                      {'vendor_id': vendor_id})
                query_check_list = query_check.to_list()
                valid_vendor_code_list = [item.get('vendor_code') for item in query_check_list]

            validation_vendor_code = self.config_template.get("vendor_code_validation", None)
            for vendor_code in self.item_codes:
                check_numeric = re.search(r'^\d+$', vendor_code)
                if vendor_code not in valid_vendor_code_list:
                    self.logger.error(
                        f"Vendor code: '{vendor_code}' not valid vendor code for vendor_id: '{vendor_id}' or not present in database")
                    self.logger.info(f"Removing invalid vendor code '{vendor_code}' and formulating request")
                    invalid_vendor_codes.append(vendor_code)
                elif validation_vendor_code:
                    validation_type = validation_vendor_code.get('type')
                    validation_condition = validation_vendor_code.get('condition')
                    if validation_type == "numeric" and check_numeric:
                        check_expression = eval(f"{len(vendor_code)} {validation_condition}")
                        if not check_expression:
                            invalid_vendor_codes.append(vendor_code)

            with open(FETCHER_FILE_PATH + f'{vendor_id}_invalid_vendor_codes.json', 'w') as f:
                json.dump(invalid_vendor_codes, f)

            item_codes = list(set(self.item_codes) - set(invalid_vendor_codes))
            self.summary['RequestItemCodeCount'] = len(item_codes)

            template_values = self.kwargs.get('template_values')

            if item_codes is None or len(item_codes) == 0:
                raise RESTXMLFetcherArgsException("No item_codes passed")
            elif len(item_codes) == 1:
                self.single_item = True
            if template_values is None or len(template_values) == 0:
                raise RESTXMLFetcherArgsException("No template_values passed")

        except Exception as ex:
            raise UC_ConfigReadException(ex, exc_info=True)

        # Use the template_values and item_codes to construct the final config object
        self.request_config = self.__create_config(self.config_template, item_codes, template_values)

        return self

    def fetch_vendor_data(self) -> Base:
        """
        Make the API call and get the data from vendor API
        """
        try:
            # 1. Prepare/Get data for the request
            self.logger.info("Fetching data from vendor API")

            self.logger.debug("Get parameters needed for sending request")
            flat_request_config, req_url, req_method, \
                req_url_query_raw, req_body, \
                xml_payload_limit, req_header, req_url_params = self.get_request_params()

            # Get the path to where the data resides in response
            data_list_path = flat_request_config.get('data_list_path')
            tmp_flat_data = []
            tmp_split_data_list_path = data_list_path.split(".")
            special_case = False
        except Exception as ex:
            self.logger.error("Could not prepare data for the request")
            self.logger.error("This would usually(but not always) mean issues with DB config or the config file")
            self.logger.error("The code returned: ")
            self.logger.error(ex)
            raise APIDataException("Error preparing data for REST request")

        # We'll now call the API

        # We have 3 variations of requests that we can make here
        # 1. Single Request - We just send one request and that's it
        # 2. Multiple requests (paginated/chunked) - This can have 2 further variations
        #   a. The payload (item_codes) are to be included in the body of the request
        #   b. The item_codes are to be included in the request URL parameter
        #
        # All the 3 cases are handled below separately
        #

        try:
            # Case where the item codes are supposed to be part of the url parameters
            # e.g: Jenne
            if self.request_in_api_request_template_query:
                self.summary['RunType'] : "Multi-request,Vendor Codes in URL Parameters. XML Response"
                # Chunk item code list and create url parameters, updates self.multi_req_url_params
                self.chunk_item_codes_list(req_url_query_raw, xml_payload_limit)

                # Make the actual api calls
                tmp_flat_data = self.request_with_item_in_parameters(req_url, req_method, req_body, tmp_split_data_list_path, req_header)

        except Exception as ex:
            self.logger.error("Could not get or process data from API for multiple values in query params", exc_info=True)
            self.logger.error("Code returned:")
            self.logger.error(ex)
            raise APIDataException("Error getting/parsing data from request for XML response")

        try:
            if self.multi_body is True:
                # Case where item codes are to be part of body and we are to send multiple requests
                self.summary['RunType'] : "Multi-request, Vendor Codes in request body. XML Response"
                self.response_bodies = []
                self.logger.info("Making multiple XML API Request")
                for body in self.body_number:
                    resp = self.make_api_call(req_method, req_url, body, req_header, req_url_params)
                    if resp is None:
                        # This batch failed for some reason, log the error and continue
                        # Logging has already been done in the called function
                        self.logger.warn("Batch failed in mult-request sync")
                        self.summary['FailedBatches'] += 1
                        continue
                    tmp_response_txt = json.dumps(xmltodict.parse(resp.text))
                    self.response_bodies.append(tmp_response_txt)

            else:
                # Case where item codes are part of body, and we have just one request
                self.summary['RunType'] : "Single-request, Vendor Codes in URL Parameters. XML Response"
                self.logger.info("Making xml API Request")
                self.response = self.make_api_call(req_method, req_url, req_body, req_header, req_url_params)
                if self.response is None:
                    # This is a failure in case of single request sync, this should be treated as fatal
                    self.logger.error("Error fetching data from API")
        except Exception as ex:
            self.logger.error("Could not fetch data from API")
            self.logger.error("Code returned: ")
            self.logger.error(ex)
            raise APIDataException("Could not get data from API")
             
        try:
            if not self.request_in_api_request_template_query:
                # Process the response body for cases where item codes were part of request body
                # For the case where item codes are sent in url params, the responses have already been processed
                
                tmp_flat_data = self.process_response_body(data_list_path)
        except ValueError as ex:
            self.logger.error("The response is not a valid JSON")
            raise APIDataException("The received response is not a valid JSON")

        if self.error_field_mapping:
            self.vendor_codes_error_status = self.process_error_field_db(tmp_flat_data)
        
        ## Update last fetch date of vendor_codes if it is not already updated while setting error codes
        if not self.error_field_mapping:
            update_last_fetch_date = LIVendorCodes()
            update_last_fetch_date.bulk_update_last_fetch_date_vendor_codes(self.kwargs.get('vendor_id'))

        if tmp_flat_data is not None:
            self.summary["ResponseItemCodeCount"] = len(tmp_flat_data)
        else:
            self.summary["ResponseItemCodeCount"] = 0
        self.data = json.dumps(tmp_flat_data)
        
        return self

    def process_response_body(self, data_list_path):
        """
        Helper function
        Processess the response body to get the desired data for cases where the 
        Body contains the item codes in request
        """
        try:
            tmp_flat_data = []
            if hasattr(self, 'response') and self.response is not None:
                tmp = self.response.text
                xml_text = json.dumps(xmltodict.parse(tmp))
                json_text = json.loads(xml_text)
            else:
                json_text = self.response_bodies

            tmp_split_data_list_path = data_list_path.split(".")

            if isinstance(json_text, dict) and data_list_path is not None and len(
                                tmp_split_data_list_path) > 1 and self.single_item is False:
                tmp_flat = flatten(json_text, reducer="dot", max_flatten_depth=4)
                tmp_flat_data = tmp_flat.get(data_list_path)
            elif isinstance(json_text, dict) and data_list_path is not None and len(
                                tmp_split_data_list_path) == 1 and self.single_item is False:
                tmp_flat_data.append(json_text.get(data_list_path))
            elif isinstance(json_text, list) and data_list_path:
                for data in json_text:
                    tmp_json_data = json.loads(data)
                    tmp_data = safeget(tmp_json_data, tmp_split_data_list_path)
                    for item in tmp_data:
                        tmp_flat_data.append(item)
            elif self.single_item is True:
                tmp_flat_data.append(safeget(json_text, tmp_split_data_list_path))
            else:
                tmp_flat_data.append(json_text)
        except Exception as ex:
            self.logger.error("Error while post-processing the data received from API")
            self.logger.error("Context: XML Data, Cases with item codes in request body")
            self.logger.error("Code returned:")
            self.logger.error(ex)
            raise APIDataException("Error while processing data received from API for XML data")
        return tmp_flat_data

    def get_request_params(self):
        """
        Get the paramters needed from request
        """
        self.logger.debug("Flattening the config json for easier reading")
        flat_request_config = flatten(self.request_config, reducer='dot')

        self.logger.debug("Reading values needed for creating request")
        req_url = flat_request_config.get('api_request_template.url.raw')
        req_method = flat_request_config.get('api_request_template.url.method')
        req_headers_raw = flat_request_config.get('api_request_template.header')
        req_url_query_raw = flat_request_config.get('api_request_template.url.query')
        req_body = flat_request_config.get('final_xml_req')[0]
        xml_payload_limit = flat_request_config.get('xml_payload_limit', 0)
        if xml_payload_limit:
            xml_payload_limit = int(xml_payload_limit)

        self.logger.debug("Creating collection/dictionaries for request")
        req_header = {}
        for obj in req_headers_raw:
            req_header[obj['key']] = obj['value']

        req_url_params = {}
        if req_url_query_raw is not None:
            for obj in req_url_query_raw:
                req_url_params[obj['key']] = obj['value']
        return flat_request_config,req_url,req_method,req_url_query_raw,req_body,xml_payload_limit,req_header, req_url_params


    def make_api_call(self, req_method, req_url, body, req_header, req_url_params):
        """
        Helper function
        Make the actual api call and check the return code status
        """
        self.logger.info("Making API Call")
        requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
        # req_header = {}
        response = requests.request(method=req_method,
                                    url=req_url,
                                    data=body,
                                    headers=req_header,
                                    verify=False,
                                    timeout=Config.REQUEST_TIMEOUT,
                                    params=req_url_params)
        self.response_info["vendor_id"] = self.kwargs.get('vendor_id')
        self.response_info["response_code"] = response.status_code
        if response.status_code not in range(200, 210):
            self.response_info["response_text"] = response.reason
            self.logger.error("API returned incorrect status code: {}".format(response.status_code))
            self.logger.error("API response body was: ")
            self.logger.error(response.text)
            response = None  # This will be checked by the caller
        return response

    def request_with_item_in_parameters(self, req_url, req_method, req_body, tmp_split_data_list_path, req_header):
        """
        This method makes calls to the API for case where the items are included in the URL parameter
        """
        tmp_flat_data = []
        for req_url_params in self.multi_req_url_params:
            self.response = self.make_api_call(req_method, req_url, req_body, req_header, req_url_params)

            tmp_response_txt = json.dumps(xmltodict.parse(self.response.text))
            try:
                tmp_data = safeget(json.loads(tmp_response_txt), tmp_split_data_list_path)
                if not isinstance(tmp_data, list):
                    # This should be a list, but in case the response contains a single item
                    # This can be a dictionary, in that case we will put this dictionary in 
                    # a list
                    tmp_list = []
                    tmp_list.append(tmp_data)
                    tmp_data = tmp_list
                self.response_bodies.append(tmp_data)
                for item in tmp_data:
                    tmp_flat_data.append(item)
            except Exception as ex:
                # This exception is usually hit when the server does not respond with
                # details on the item codes passed in request.
                # TODO: This is handled like a warning case for now as most likely
                #   The vendor does not have details on some item codes that we sent
                self.logger.warn("Could not extract data from response. This might be because the server did not send details on the item codes.")
                self.logger.warn("DEBUG Info:")
                self.logger.warn("Request URL: {}".format(req_url))
                self.logger.warn("Request URL parameters: {}".format(req_url_params))
                self.logger.warn("Response text: {}".format(tmp_response_txt))
                self.logger.warn("Error thrown by the code: {}".format(ex))
                self.summary['FailedBatches'] += 1

        return tmp_flat_data


    def chunk_item_codes_list(self, req_url_query_raw, xml_payload_limit):
        """ Splits/Chunks the item codes to sublists (including the other common parameters)
            So that we can easily make multiple calls to get the data form entire list of item codes 
            Result is an update to the instance variable multi_req_url_params, which contains
            a list of url parameters for each of the multiple calls that we need to make
        """
        item_codes = self.kwargs.get('item_codes')
        if item_codes and len(item_codes) > 0:
            list_of_codes = []
            for idx, item in enumerate(item_codes):
                list_of_codes.append(item)
                if (idx != 0 and idx % xml_payload_limit == xml_payload_limit - 1) or item == item_codes[
                            -1]:
                    req_url_params = {}
                    if req_url_query_raw is not None:
                        for obj in req_url_query_raw:
                            if obj['key'] == 'productList':
                                req_url_params[obj['key']] = obj['value'].replace(f'<<TPL_ITEM_CODE>>',
                                                                                          ','.join(list_of_codes))
                            else:
                                req_url_params[obj['key']] = obj['value']
                    self.multi_req_url_params.append(req_url_params)
                    list_of_codes = []

    def __create_config(self, config_template, item_codes, template_values) -> string:
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
            xml_payload_format = config_template.get('xml_payload_format', None)
            xml_req_body = config_template.get('xml_req_body', None)
            xml_payload = flat_config.get(xml_payload_format, None)
            xml_append_multi = config_template.get('xml_append_multi', None)
            xml_iterator = config_template.get('xml_iterator', None)
            xml_multi_req_body = config_template.get('xml_multi_req_body', None)
            xml_req_multi_start_tg = config_template.get('xml_append_multi_start_tg', None)
            xml_payload_limit = config_template.get('xml_payload_limit', 0)
            if xml_payload_limit:
                xml_payload_limit = int(xml_payload_limit)

            data_list_path = config_template.get('data_list_path', None).split('.')
            item_obj_str = xml_payload
            if xml_multi_req_body:
                xml_req_body_manufacture = config_template.get('xml_req_body_manufacture', None)
                xml_req_body_distributor = config_template.get('xml_req_body_distributor', None)

            req_url_query_raw = flat_config.get('api_request_template.url.query',
                                                None)  # if true payload to be inserted in api_request_template.url.query
            tmp_item_obj_str = ""
            item_list = []
            first_code = True
            line_number = 0
            self.body_number = []
            self.multi_body = False
            break_request = False
            if len(item_codes) > xml_payload_limit:
                self.multi_body = True

            if xml_req_body and xml_multi_req_body:
                #xml_req_body, first_code, break_request = self.process_config_multibody(config_template, item_codes, 
                #                                                xml_payload, xml_append_multi, xml_req_multi_start_tg, 
                #                                                xml_payload_limit, item_obj_str, xml_req_body_manufacture, 
                #                                                xml_req_body_distributor, first_code)
                self.process_multibody_config(item_codes, xml_payload_limit, data_list_path, xml_payload, xml_req_body_manufacture, xml_req_body_distributor)
            elif xml_req_body:
                for idx, item in enumerate(item_codes):
                    if first_code or break_request:
                        tmp_item_obj_str = item_obj_str.replace(f'<<TPL_ITEM_CODE>>', item)
                        if xml_iterator:
                            line_number += 1
                            tmp_item_obj_str = tmp_item_obj_str.replace(f'<<{xml_iterator}>>', str(line_number))
                        first_code = False
                        break_request = False
                    else:
                        add_here = tmp_item_obj_str.rfind(xml_append_multi) + len(xml_append_multi)
                        xml_req_body = xml_req_body.replace(f'<<TPL_ITEM_CODE>>', item)
                        if xml_iterator:
                            line_number += 1
                            xml_req_body = xml_req_body.replace(f'<<{xml_iterator}>>', str(line_number))
                        tmp_item_obj_str = tmp_item_obj_str[:add_here] + xml_req_body + tmp_item_obj_str[add_here:]
                    if (idx != 0 and idx % xml_payload_limit == xml_payload_limit - 1) or item == item_codes[-1]:
                        self.body_number.append(tmp_item_obj_str)
                        break_request = True
                    else:
                        xml_req_body = config_template.get('xml_req_body', None)
            elif req_url_query_raw != []:
                self.request_in_api_request_template_query = True

            item_list.append(tmp_item_obj_str)
            flat_config['final_xml_req'] = item_list

            config_template = unflatten(flat_config, splitter='dot')
            return config_template
        except Exception as ex:
            self.logger.error("Could not create configuration object.", exc_info=True)
            self.logger.error(ex)
            raise RESTXMLFetcherArgsException(ex)

    def process_multibody_config(self, item_codes, xml_payload_limit, \
                                        data_list_path, xml_payload, \
                                        xml_req_body_manufacture, xml_req_body_distributor):
        """
        Process configuration and create bodies for cases like Techdata - multiple requests - item list in body
        KLUDGE, TODO: Though this is supposed to be generic, this is not. Its very specific to Techdata.
        """
        try:
            # Load the template, we are replacing the placeholder because having << or >> in xml is not allowed
            template_root = ET.fromstring(xml_payload.replace("<<TPL_ITEM_CODE>>", "TPL_ITEM_CODE"))

            # Now lets iterate the template till we reach the item node
            item_node = template_root
            for i in range(1,len(data_list_path)):  # Note that we are starting at index 1, that's because we are already at root node, and can skip it
                item_node = item_node.find(data_list_path[i])

            # Remove the item node from the template since we are going to add it from manufacturer and distributer templates
            template_root.remove(item_node)

            # Chunk the item codes to list of lists, with each sublist being of max size = payload limit
            chunked_list = [item_codes[i:i+xml_payload_limit-1] for i in range(0, len(item_codes), xml_payload_limit-1)]

            # Now all we need to do is iterate through the items list and keep adding the items to xml tree
            for chunk in chunked_list: # Each iteration results in payload for a request
                curr_req_node = copy.deepcopy(template_root)
                for item in chunk: # Each iteration is an item code in a request
                    if item.isdigit(): # Use distributeritemIdentifier
                        curr_req_node.append(ET.fromstring(xml_req_body_distributor.replace("<<TPL_ITEM_CODE>>", item)))
                    else:  # Use Manufacturer Item Identifier
                        curr_req_node.append(ET.fromstring(xml_req_body_manufacture.replace("<<TPL_ITEM_CODE>>", item)))
                self.body_number.append(ET.tostring(curr_req_node).decode())
                if not self.multi_body:
                    self.multi_body = True
        except Exception as ex:
            self.logger.error("Could not prepare request body for XML multirequest")
            self.logger.error("Code returned: ")
            self.logger.error(ex)
            raise APIDataException("Could not prepare request body for XML multirequest")

        
    def process_error_field_db(self, data) -> None:
        """
        Takes a list of objects, transform errors in objects according to our vendor_codes table schema

        :param data: List of inventory items
        :type data: list
        :return None
        """

        status_mapping = self.config_template.get("mapping").get("message_when_no_error")
        tmp_item_list = []

        try:
            for item in data:
                flatten_item = flatten(item, reducer="dot", max_flatten_depth=4)

                tmp_dict = {}
                for fld in self.error_field_mapping.keys():
                    _fld = copy.deepcopy(fld)

                    #### Check if order has multi_vendor_code. If yes then take first field else take other
                    if len(fld.split(',')) > 0:
                        fld_list = fld.split(',')
                        for item in fld_list:
                            if flatten_item.get(item):
                                _fld = item
                                break
                        val = flatten_item.get(_fld)
                        if status_mapping:
                            if val in status_mapping:
                                tmp_dict.update({"error": False, self.error_field_mapping[fld]: val})
                            else:
                                tmp_dict.update({"error": True, self.error_field_mapping[fld]: val})
                        else:
                            tmp_dict.update({self.error_field_mapping[fld]: val})
                    else:
                        if flatten_item.get(_fld):
                            val = flatten_item.get(_fld)
                            if status_mapping:
                                if val in status_mapping:
                                    tmp_dict.update({"error": False, self.error_field_mapping[_fld]: val})
                                else:
                                    tmp_dict.update({"error": True, self.error_field_mapping[_fld]: val})
                            else:
                                tmp_dict.update({self.error_field_mapping[fld]: val})
                
                # if status_mapping and tmp_dict.get("error") == None:
                #     del tmp_dict["error_description"]
                
                if not status_mapping:
                    if tmp_dict.get("error") == None and tmp_dict.get("error_description"):
                        tmp_dict.update({"error": True})
                
                    if tmp_dict.get("error") == None and tmp_dict.get("error_description") == None:
                        tmp_dict.update({"error": False, "error_description": "No Error"})
                
                tmp_item_list.append(tmp_dict)
            # return the error and error description value for each vendor_id
            return self.update_error_field_db(tmp_item_list)
        except Exception as e:
            self.logger.error("Could not transform data", exc_info=True)
            self.logger.exception(e)        

    def update_error_field_db(self, transformed_data) -> None:
        try:
            vendor_id = self.kwargs.get('vendor_id')
            if transformed_data:
                # # Filter out only those object which has vendor_code
                transformed_data = list(filter(lambda p: (p.get('vendor_code') is not None and p.get('vendor_code') in self.kwargs.get('item_codes')), transformed_data))

                # Very Important part which filters duplicate vendor_code from the list and prevents unique constraint violation
                new_data = []
                _ = [new_data.append(x) for x in transformed_data if x.get('vendor_code') not in [y.get('vendor_code') for y in new_data]]
                transformed_data = new_data

                # FOR FUTURE REFERENCE:- Use below code if upsert query is used instead of update
                # for items in transformed_data:
                #     items.update({"vendor_id": vendor_id})
                
                # FOR FUTURE REFERENCE:- Use below code if upsert query is used instead of update
                # fetched_vendor_codes = (obj.get("vendor_code") for obj in transformed_data)

                update_vendor_codes = LIVendorCodes(transformed_data)
                vendor_obj = update_vendor_codes.get_product_by_InternalId(vendor_id, 'vendor_id')

                # FOR FUTURE REFERENCE:- Use below code if upsert query is used instead of update
                # for item in transformed_data:
                #     for obj in vendor_obj["data"]:
                #         if item.get("vendor_code") in obj.get("vendor_code"):
                #             item.update({"internal_id": obj.get("internal_id")})

                _result = {}
                for item in transformed_data:
                    _result.update({item.get("vendor_code"): [item["error"], item["error_description"]]})
                
                final_obj = {'vendor_id':[], "vendor_code": [], "error": [], "error_description": [], "internal_id": []}
                for item in vendor_obj["data"]:
                    if _result.get(item.get('vendor_code')) is not None and item.get('vendor_code'):
                        item["error"] =  _result.get(item['vendor_code'])[0] if _result.get(item['vendor_code']) and _result.get(item['vendor_code'])[0] is not None else True
                        item["error_description"] = _result.get(item['vendor_code'])[1] if _result.get(item['vendor_code']) and _result.get(item['vendor_code'])[1] else 'Invalid Item Code'

                        final_obj['vendor_id'].append(item['vendor_id'])
                        final_obj["vendor_code"].append(item["vendor_code"])
                        final_obj["error"].append(item["error"])
                        final_obj["error_description"].append(item["error_description"])
                        final_obj["internal_id"].append(item["internal_id"])

                try:
                    update_vendor_codes.load(partial=True)
                    update_vendor_codes.bulk_update(final_obj, 'vendor_id,vendor_code,internal_id', vendor_id=vendor_id, vendor_codes=final_obj['vendor_code'])
                   
                    # FOR FUTURE REFERENCE:- Use below code if upsert query is used instead of update
                    #update_vendor_codes.upsert(transformed_data, conflict_fields="vendor_id, vendor_code")
                except Exception as err:
                    self.logger.exception(err, exc_info=True)

                # FOR FUTURE REFERENCE:- Use below code if upsert query is used instead of update
                # try:
                #     update_vendor_codes.bulk_update_fetch_date_vendor_codes(vendor_id, list(fetched_vendor_codes))
                # except Exception as ex:
                #     self.logger.error(ex, exc_info=True)
                #     self.logger.error(f"exception occurred while updating last fetch date for {vendor_id}")
                #     raise UC_DataDispatchError(f"Could not update data for {vendor_id}")
            else:
                self.logger.info("No data found for mapping vendor_code errors for vendor {%s}" % vendor_id)
            return final_obj
        except Exception as e:
            self.logger.error("Could not update error fields in DB Table", exc_info=True)
            self.logger.exception(e)
        