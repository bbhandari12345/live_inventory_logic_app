from LiveInventoryFetcher.base.base import Base, UC_ConfigReadException
from LiveInventoryFetcher.fetcher.fetcherbase import FetcherBase
from LiveInventoryFetcher.config import Config
from LiveInventoryFetcher.common_utils.db_queries import QUERY_CHECK_IF_VENDOR_CODE_EXISTS_IN_VENDOR_CODES_TABLE
from LiveInventoryFetcher.utils.data_access_layer.sql_db import DBEngineFactory
import string
import json
import requests
from flatten_dict import flatten, unflatten
from LiveInventoryFetcher.orm.li_vendor_codes import LIVendorCodes
from typing import List
import copy

FETCHER_FILE_PATH = Config.NETWORK_CONFIG.get('fetcher_directory_path')


class RESTJSONFetcherArgsException(Exception):
    pass


class TemplatingException(Exception):
    pass


class APIDataException(Exception):
    pass


class APIValueException(Exception):
    def __init__(self, message, errors=None):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = errors


def safeget(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


class RESTJSONFetcher(FetcherBase):
    
    ITEM_CODE_STR = f'<<TPL_ITEM_CODE>>'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.item_codes = []
        self.error_field_mapping = {}
        self.li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)
        self.response_info = {
            "vendor_id": None,
            "response_text": None,
            "response_code": None
        }
        self.summary['FailedBatches'] = 0
        # Check if all the mandatory parameters are present
        mandatory_params = ['vendor_id', 'config_file_path',
                            'item_codes', 'template_values']
        for param in mandatory_params:
            if not param in kwargs:
                raise RESTJSONFetcherArgsException(
                    "Mandatory parameter '{}' not found".format(param))

    def fetch_config(self) -> Base:
        """
        Read the config file template and construct final config object out of it
        """
        # Read the config template
        self.read_config()

        if self.config_template.get("mapping") and self.config_template.get("mapping").get("vendor_code_table"):
            error_mapping_list = self.config_template.get("mapping").get("vendor_code_table")
            self.error_field_mapping = {fld_map.get('destination_field'): fld_map.get('source_field') for fld_map in error_mapping_list}

        invalid_vendor_codes = []
        # Load other values
        try:
            self.item_codes = self.kwargs.get('item_codes')
            template_values = self.kwargs.get('template_values')
            vendor_id = self.kwargs.get('vendor_id')

            with self.li_db.transaction(auto_commit=True) as query_set:
                query_check = query_set.execute_query(
                    QUERY_CHECK_IF_VENDOR_CODE_EXISTS_IN_VENDOR_CODES_TABLE, {'vendor_id': vendor_id})
                query_check_list = query_check.to_list()
                valid_vendor_code_list = [
                    item.get('vendor_code') for item in query_check_list]

            for vendor_code in self.item_codes:
                if vendor_code not in valid_vendor_code_list:
                    self.logger.error(
                        f"Vendor code: '{vendor_code}' not valid vendor code for vendor_id: '{vendor_id}' or not present in database")
                    self.logger.info(
                        f"Removing invalid vendor code '{vendor_code}' and formulating request")
                    invalid_vendor_codes.append(vendor_code)
                elif self.config_template.get("vendor_code_validation") is True and (vendor_code.count("-") > 1 or len(vendor_code) > 12):
                    invalid_vendor_codes.append(vendor_code)

            with open(FETCHER_FILE_PATH + f'{vendor_id}_invalid_vendor_codes.json', 'w') as f:
                json.dump(invalid_vendor_codes, f)

            self.item_codes = list(
                set(self.item_codes) - set(invalid_vendor_codes))

            if self.item_codes is None or len(self.item_codes) == 0:
                raise RESTJSONFetcherArgsException("No item_codes passed")

            self.summary['RequestItemCodeCount'] = len(self.item_codes)
            if template_values is None or len(template_values) == 0:
                raise RESTJSONFetcherArgsException("No template_values passed")

        except Exception as ex:
            raise UC_ConfigReadException(ex, exc_info=True)

        # Use the template_values and item_codes to construct the final config object
        self.request_config = self.__create_config(
            self.config_template, self.item_codes, template_values)
        return self


    def make_api_call(self, req_method, req_url, body, req_header, req_url_params):
        """
        Helper function
        Make the actual api call and check the return code status
        """
        self.logger.info("Making API Call")
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
            self.summary["FailedBatches"] += 1
            #response = None # This will be checked by the caller
        return response


    def fetch_vendor_data(self) -> Base:
        """
        Make the API call and get the data from vendor API
        """
        try:
            # 1. Prepare/Get data for the request
            self.logger.info("Fetching data from vendor API")

            self.logger.debug("Flattening the config json for easier reading")
            flat_request_config = flatten(self.request_config, reducer='dot')

            self.logger.debug("Reading values needed for creating request")
            req_url = flat_request_config.get('api_request_template.url.raw')
            req_method = flat_request_config.get(
                'api_request_template.url.method')
            req_headers_raw = flat_request_config.get(
                'api_request_template.header')
            req_url_query_raw = flat_request_config.get(
                'api_request_template.url.query')
            req_body = json.dumps(self.request_config.get('data', {}))

            self.logger.debug("Creating collection/dictionaries for request")
            req_header = {}
            for obj in req_headers_raw:
                req_header[obj['key']] = obj['value']

            req_url_params = {}
            if req_url_query_raw is not None:
                for obj in req_url_query_raw:
                    req_url_params[obj['key']] = obj['value']

            items_response_path = self.config_template.get(
                'items_response', None)

     

        except Exception as ex:
            self.logger.error(
                "Could not prepare the REST request", exc_info=True)
            raise APIDataException("Error preparing data for REST request")

        tmp_flat_data = []
        try:
            # This block is to handle cases with pagination. eg: Nuvia
            if self.request_config.get('pagination_control', None) and req_body=='{}':
                # We need multiple calls with pagination control, there's nothing to be sent in body
                fetch_next_page = True
                page_num = 1
                total_pages = None
                self.response_bodies = []
                while(fetch_next_page): # We'll loop till we go through all pages

                    # Here we are handling sub-case where the page number has to be sent in url parameter
                    if flat_request_config.get('pagination_control.request.param_location') == 'url':
                        req_url_params[flat_request_config.get('pagination_control.request.page_number')] = page_num
                    tmp_response = self.make_api_call(req_method, req_url,
                                                        req_body, req_header, req_url_params)
                    
                    self.response_bodies += json.loads(tmp_response.text)

                    # Here we are trying to get a read on how many pages in total are there
                    # This could be done just once, but no harm reading it in each iteration
                    if flat_request_config.get('pagination_control.response.param_location') == 'header':
                        total_pages = tmp_response.headers.get(flat_request_config.get('pagination_control.response.total_pages'))
                        total_pages = int(total_pages)
                    if total_pages and page_num>=total_pages:
                        fetch_next_page = False
                    else:
                        page_num += 1

            elif self.single_item_code_in_url:
                # If this is the case where we have to send multiple requests
                # with single item code in the request url
                self.logger.info("Making multiple API requests with item code in url")
                self.response_bodies = []
                for item in self.item_codes:
                    tmp_response = self.make_api_call(req_method, req_url.replace(self.ITEM_CODE_STR, item),
                                                        req_body, req_header, req_url_params)
                    if tmp_response.status_code == 404:
                        self.logger.warn("Item not found. The API returned 404")
                    else:
                        self.response_bodies.append(json.loads(tmp_response.text))
            elif hasattr(self, "body_number"):
                self.logger.info("Making multiple API Request")
                self.response_bodies = []
                for item in self.body_number:
                    body = json.dumps(item.get('data'))
                    resp = self.make_api_call(req_method, req_url, body, req_header, req_url_params)
                    
                    if resp.status_code in range(200, 210):
                        tmp_response_txt = json.loads(resp.text)
                        if isinstance(tmp_response_txt, dict) and items_response_path is not None:
                            tmp_flat_data = self.response_mapper(
                                tmp_response_txt)
                            self.response_bodies.append(tmp_flat_data)
                            self.multi_response = True
                        elif isinstance(tmp_response_txt, dict) and items_response_path is None:
                            tmp_data = tmp_response_txt.get(
                                items_response_path)
                            tmp_flat_data = tmp_data
                            self.response_bodies.append(tmp_flat_data)
                        elif type(tmp_response_txt) is list and items_response_path is None:
                            for items in tmp_response_txt:
                                self.response_bodies.append(items)
                    else:
                        self.logger.error("Could not get data from vendor API. API returned HTTP Status code: {}".format(resp.status_code), exc_info=True)
            else:
                self.logger.info("Making API Request")
                self.response = requests.request(method=req_method,
                                                 url=req_url,
                                                 data=req_body,
                                                 headers=req_header,
                                                 verify=False,
                                                 timeout=Config.REQUEST_TIMEOUT,
                                                 params=req_url_params)
                self.response_info["vendor_id"] = self.kwargs.get('vendor_id')
                self.response_info["response_code"] = self.response.status_code
                if self.response.status_code not in range(200, 210):
                    self.response_info["response_text"] = self.response.reason
                    self.logger.error("Could not get data from vendor API. API returned HTTP Status code: {}".format(
                        self.response.status_code))
                    raise APIDataException(
                        "The response from vendor API is not valid")

                self.logger.debug("Checking if the response is valid JSON")

            try:
                if hasattr(self, 'response') and self.response.status_code in range(200, 210):
                    tmp_response_txt = json.loads(self.response.text)
                else:
                    tmp_response_txt = self.response_bodies

                if type(tmp_response_txt) is dict and items_response_path is not None:
                    tmp_flat_data = self.response_mapper(tmp_response_txt)
                elif type(tmp_response_txt) is dict and items_response_path is None:
                    tmp_data = tmp_response_txt.get(items_response_path)
                    tmp_flat_data = tmp_data
                elif type(tmp_response_txt) is list and items_response_path is None:
                    tmp_flat_data = tmp_response_txt
                elif type(tmp_response_txt) is list and self.multi_response is True:
                    tmp_response = []
                    tmp_flat_data = tmp_response_txt
                    for item in tmp_flat_data:
                        for i in item:
                            tmp_response.append(i)
                    tmp_flat_data = tmp_response
                else:
                    for data in tmp_response_txt:
                        if data.get('product_code'):
                            is_in_req = str(data.get('product_code'))
                            if is_in_req in self.item_codes:
                                tmp_flat = flatten(
                                    data, reducer="dot", max_flatten_depth=4)
                                tmp_flat_data.append(tmp_flat)
                        else:
                            tmp_flat = flatten(
                                data, reducer="dot", max_flatten_depth=4)
                            tmp_flat_data.append(tmp_flat)

            except ValueError as ex:
                self.logger.error(ex)
                self.logger.error("The response is not a valid JSON", exc_info=True)

            if self.error_field_mapping:
                self.vendor_codes_error_status = self.process_error_field_db(tmp_flat_data)
            
            ## Update last fetch date of vendor_codes if it is not already updated while setting error codes
            if not self.error_field_mapping:
                update_last_fetch_date = LIVendorCodes()
                update_last_fetch_date.bulk_update_last_fetch_date_vendor_codes(self.kwargs.get('vendor_id'))

            # TODO: This should ideally be a flattened JSON so that a generic implementation of extractor is easy

            self.summary["ResponseItemCodeCount"] = len(tmp_flat_data)
            self.data = json.dumps(tmp_flat_data)  # response.text
        except Exception as ex:
            self.logger.error(
                "Could not get data from the API request", exc_info=True)
            # raise APIDataException("Could not get data from the API")
        return self

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

            items_list_path = config_template.get('items_list', None)
            is_url_get = config_template.get(
                'api_request_template').get('url').get('method')

            
            
            # TODO: KLUDGE
            # Here we are trying to identify case where
            # item code is part of the url and we have 
            # to send one item code per request
            self.single_item_code_in_url = False
            if self.ITEM_CODE_STR in config_template.get('api_request_template').get('url').get('raw'):
                self.single_item_code_in_url = True

            # For 'GET' type requests or where we don't need to send any body
            # We don't need to worry about preparing body -so we are done with
            # request preparation here and will return
            if items_list_path is None or is_url_get == "GET":
                self.logger.warn("Items list path not specified")
                config_template = unflatten(flat_config, splitter='dot')
                return config_template

            curr_item = flat_config.get(items_list_path)
            if not isinstance(curr_item, list):
                self.logger.error("The item list node is not a list")
                raise RESTJSONFetcherArgsException(
                    "The item list node is not a list")

            item_obj_str = json.dumps(curr_item[0])

            validation_vendor_code = flat_config.get(
                "vendor_code_validation", None)

            item_list = []
           
            for item in item_codes:
                tmp_item_obj_str = item_obj_str.replace(
                    self.ITEM_CODE_STR, item)
                # For Westcon
                if validation_vendor_code and isinstance(validation_vendor_code, list):
                    for idx, validation in enumerate(validation_vendor_code):
                        validation_condition = validation.get('condition')
                        check_condition = eval(
                            f"{len(item)} {validation_condition}")
                        if check_condition and idx == 0:
                            tmp_item_obj_str = tmp_item_obj_str.split(",")[
                                0] + "}"
                            break
                        elif check_condition and idx == 1:
                            tmp_item_obj_str = "{" + \
                                tmp_item_obj_str.split(",")[1]
                    item_list.append(json.loads(tmp_item_obj_str))
                else:
                    item_list.append(json.loads(tmp_item_obj_str))

            def chunks(lst, n):
                for i in range(0, len(lst), n):
                    yield lst[i:i + n]

            # TODO: This hard-coded limit on chunk size has to change, 
            # as has the way to how we chunk it
            CHUNK_SIZE = 100
            if len(item_codes) > CHUNK_SIZE:
                self.split_body = chunks(item_list, CHUNK_SIZE)
                self.body_number = []
                for items in self.split_body:
                    flat_config[items_list_path] = items
                    body_items = unflatten(flat_config, splitter='dot')
                    self.body_number.append(body_items)
            # config_template = flat_config
            else:
                flat_config[items_list_path] = item_list

            config_template = unflatten(flat_config, splitter='dot')
            return config_template
        except Exception as ex:
            self.logger.error(
                "Could not create configuration object.", exc_info=True)
            self.logger.error(ex)
            raise RESTJSONFetcherArgsException(ex)

    def response_mapper(self, data):
        items_response_path = self.config_template.get('items_response', None)
        if items_response_path:
            items_response_path_list = items_response_path.split(".")
            data_to_dump = safeget(data, items_response_path_list)
            return data_to_dump

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
                flatten_item = flatten(
                    item, reducer="dot", max_flatten_depth=4)

                tmp_dict = {}
                for fld in self.error_field_mapping.keys():
                    if fld == 'multi_vendor_code':
                        fld = 'vendor_code'
                        if item.get('DistributorItemIdentifier') in self.vendor_codes and type(int(item.get('DistributorItemIdentifier'))) == int:
                            tmp_dict[fld] = item.get(
                                'DistributorItemIdentifier')
                        elif item.get('ManufacturerItemIdentifier') in self.vendor_codes and type(item.get('ManufacturerItemIdentifier')) == str:
                            tmp_dict[fld] = item.get(
                                'ManufacturerItemIdentifier')
                        continue
                    
                    if status_mapping:
                        if flatten_item.get(fld) in status_mapping:
                            tmp_dict.update({"error": False, self.error_field_mapping[fld]: flatten_item.get(fld)})
                        else:
                            tmp_dict.update({"error": True, self.error_field_mapping[fld]: flatten_item.get(fld)})
                    else:
                        if flatten_item.get(fld) and (flatten_item.get(fld) != False or flatten_item.get(fld) != "FAILED"):
                            tmp_dict[self.error_field_mapping[fld]] = True if self.error_field_mapping[fld] == "error" else flatten_item.get(fld)
                        else:
                            tmp_dict[self.error_field_mapping[fld]] = False if self.error_field_mapping[fld] == "error" else "No Error"

                    # if multi / looping required with addition req e.g. adding quantity
                    if_fld_has_multi = fld.find('[i]')
                    if if_fld_has_multi != -1:
                        fld_tmp = fld[:if_fld_has_multi]
                        tmp_fld_nesting = fld_tmp.split('.')
                        tmp_list = [tmp_fld_nesting[0]]
                        tmp_safeget = safeget(item, tmp_list) or 0
                        if tmp_safeget != 0 and isinstance(tmp_safeget, list):
                            tmp_safegate_sum = sum(
                                [int(x[tmp_fld_nesting[1]]) for x in tmp_safeget])
                            tmp_dict[self.error_field_mapping[fld]
                                     ] = tmp_safegate_sum
                        elif tmp_safeget != 0 and isinstance(tmp_safeget, dict):
                            tmp_dict[self.error_field_mapping[fld]
                                     ] = tmp_safeget[tmp_fld_nesting[-1]]
                        else:
                            tmp_dict[self.error_field_mapping[fld]] = 0
                        continue

                    # checking level of nesting
                    # tmp_fld_nesting = fld.split(".")
                    # if len(tmp_fld_nesting) > 1:
                    #     tmp_dict[self.error_field_mapping[fld]] = safeget(item, tmp_fld_nesting[0]) or safeget(item, tmp_fld_nesting) or None
                    # elif response_is_nested == 1:
                    #     tmp_dict[self.error_field_mapping[fld]] = item.get(fld) or safeget(item, rsp_list_path.get(self.error_field_mapping[fld]))
                    # elif response_is_nested != 1:
                    #     tmp_dict[self.error_field_mapping[fld]] = item.get(fld)
                    # else:
                    #     tmp_dict[self.error_field_mapping[fld]] = "N/A"

                tmp_item_list.append(copy.deepcopy(tmp_dict))
            # return the error and error description value for each vendor_id
            return self.update_error_field_db(tmp_item_list)
        except Exception as ex:
            self.logger.error("Could not transform data", exc_info=True)
            self.logger.exception(ex)

    def update_error_field_db(self, transformed_data: List) -> None:
        """
        Update error fields in vendor_codes table

        :param transformed_data: List of objects with error, error_description and vendor_code
        :type transformed_data: list
        :return None
        """

        try:
            vendor_id = self.kwargs.get('vendor_id')
            if transformed_data:
                # Filter out only those object which has vendor_code
                transformed_data = list(filter(lambda p: (
                    p.get('vendor_code') is not None and p.get('vendor_code') in self.kwargs.get('item_codes')), transformed_data))

                # Very Important part which filters duplicate vendor_code from the list and prevents unique constraint violation
                new_data = []
                _ = [new_data.append(x) for x in transformed_data if x.get('vendor_code') not in [
                    y.get('vendor_code') for y in new_data]]
                transformed_data = new_data                    

                # FOR FUTURE REFERENCE:- Use below code if upsert query is used instead of update
                # for items in transformed_data:
                #     items.update({"vendor_id": vendor_id})

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
                    # update_vendor_codes.upsert(
                    #     transformed_data, conflict_fields="vendor_id, vendor_code")

                except Exception as err:
                    self.logger.exception(err, exc_info=True)
                    
                # FOR FUTURE REFERENCE:- Use below code if upsert query is used instead of update
                # try:
                #     update_vendor_codes.bulk_update_fetch_date_vendor_codes(
                #         vendor_id, list(fetched_vendor_codes))
                # except Exception as ex:
                #     self.logger.error(ex, exc_info=True)
                #     self.logger.error(
                #         f"exception occurred while updating last fetch date for {vendor_id}")
                #     raise UC_DataDispatchError(
                #         f"Could not update data for {vendor_id}")
            else:
                self.logger.info("No data found for mapping vendor_code errors for vendor {%s}" % vendor_id)
            return final_obj
        except Exception as e:
            self.logger.error("Could not update error fields in DB Table", exc_info=True)
            self.logger.exception(e)
