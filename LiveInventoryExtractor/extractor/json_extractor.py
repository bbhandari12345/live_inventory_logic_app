from LiveInventoryExtractor.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventoryExtractor.extractor.extractorbase import *
from LiveInventoryExtractor.base.base import UC_ConfigReadException
from LiveInventoryExtractor.config import Config
import json
import copy
from datetime import datetime
from LiveInventoryExtractor.common_utils.db_queries import QUERY_FETCH_VENDOR_CODE_INTERNAL_ID_MAPPING
from flatten_dict import flatten


class UC_DataTransformError(Exception):
    pass


def safeget(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


class JSONExtractor(ExtractorBase):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.vendor_code_to_internal_id = None
        self.li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)

    def fetch_config(self) -> any:
        try:
            self.read_config()
            self.response_mapping_list = self.config_template.get('mapping')
            field_mapping_list = self.config_template.get('mapping').get('inventory_table')
            self.field_mapping = {fld_map.get('destination_field'): fld_map.get('source_field') for fld_map in
                                  field_mapping_list}
            self.vendor_id = self.kwargs.get('vendor_id')
            self.vendor_codes = self.kwargs.get('item_codes')
            self.vendor_codes_error_status = self.kwargs.get('vendor_codes_error_status')
            return self
        except Exception as ex:
            self.logger.error("Could not get configuration", exc_info=True)
            self.logger.error(ex)
            raise UC_ConfigReadException("Could not get configuration")

    def transform_data(self) -> any:
        try:
            self.logger.info("Transforming data")

            self.logger.debug("Reading fetcher data")
            with open(self.kwargs['fetcher_file_path'], 'r') as data_file:
                vendor_data = json.load(data_file)

            rsp_list_path = self.response_mapping_list.get('items_response')
            self.logger.debug("Starting field mapping")

            response_is_nested = None

            if isinstance(rsp_list_path, str):
                self.logger.info("The response map list is a str")
                response_is_nested = 1

            tmp_item_list = []

            try:
                self.data = []

                next_availability_date_format = [x.get('source_format') for x in
                                                 self.config_template.get('mapping').get('inventory_table') if
                                                 x.get('source_field') == 'next_availability_date']
                next_availability_date_format = next_availability_date_format[0] if bool(
                    next_availability_date_format) else None
                if bool(next_availability_date_format):
                    next_availability_date_format = next_availability_date_format
                else:
                    next_availability_date_format = None

                # get currency_sign from config if available else None
                currency_sign = [x.get('currency_sign') for x in
                                 self.config_template.get('mapping').get('inventory_table') if
                                 x.get('source_field') == 'cost']
                currency_sign = currency_sign[0] if bool(currency_sign) else None

                for item in vendor_data:
                    tmp_dict = {}
                    flatten_each_vendor_item = flatten(item, reducer='dot')
                    for fld in self.field_mapping.keys():
                        mapping_fld_name = self.field_mapping[fld]
                        if fld == 'multi_vendor_code':
                            fld = 'vendor_code'
                            if item.get('DistributorItemIdentifier') in self.vendor_codes and type(
                                    int(item.get('DistributorItemIdentifier'))) == int:
                                tmp_dict[fld] = item.get('DistributorItemIdentifier')
                            elif item.get('ManufacturerItemIdentifier') in self.vendor_codes and type(
                                    item.get('ManufacturerItemIdentifier')) == str:
                                tmp_dict[fld] = item.get('ManufacturerItemIdentifier')
                            continue

                        # check for currency_sign cost
                        if mapping_fld_name == 'cost' and flatten_each_vendor_item.get(fld) is not None:
                            # get the currency_sign
                            if currency_sign is not None:
                                tmp_dict[mapping_fld_name] = float(
                                    flatten_each_vendor_item.get(fld).replace(currency_sign, '').strip())
                                continue

                        # for next availability date
                        if mapping_fld_name == 'next_availability_date':
                            whole_tree = fld.split('[date].') if fld.find('[date]') != -1 else [None, None]
                            root_date_key, leaf_date_key = whole_tree[0], whole_tree[1]
                            date_array = flatten_each_vendor_item.get(
                                root_date_key) if root_date_key is not None else None
                            if date_array and next_availability_date_format:
                                valid_date_candidate = [dt for dt in date_array if dt.get(leaf_date_key) is not None]
                                if bool(valid_date_candidate):
                                    valid_date_objects_cleaned = [
                                        datetime.strptime(x.get(leaf_date_key), next_availability_date_format) for x in
                                        valid_date_candidate]
                                    if bool(valid_date_objects_cleaned):
                                        tmp_dict[mapping_fld_name] = min(valid_date_objects_cleaned)
                                        continue

                        # There can be cases where the vendor code might come in one or more fields
                        # eg: it might come as part of product number or long product number
                        # This 'if' block handles such case, the config file should have
                        # all possible fields listed with comma separated values
                        if mapping_fld_name == 'vendor_code' and ',' in fld:
                            # KLUDGE: This looks convoluted, but basically it does this
                            # 1. Check if vendor_code mapping has a comma, if yes extract all possible vendor code field names to list
                            # 2. Try, in order to get value from the possible vendor code fields till you get a result
                            # 3. Once you get the result from one of the values, set it to current value 'fld'
                            # 4. Skip this iteration once you find it
                            possible_vendor_code_fields = [x.strip() for x in fld.split(',')]
                            for p_vc in possible_vendor_code_fields:
                                vendor_code = safeget(item, p_vc.split('.'))
                                if vendor_code and len(vendor_code) > 0:
                                    fld = p_vc
                                    continue

                        if item.get(fld):
                            tmp_dict[mapping_fld_name] = item.get(fld)
                            continue

                        # if multi / looping required with addition req e.g. adding quantity
                        if_fld_has_multi = fld.find('[i]')
                        if if_fld_has_multi != -1:
                            fld_tmp = fld[:if_fld_has_multi]
                            tmp_fld_nesting = fld_tmp.split('.')
                            tmp_list = [tmp_fld_nesting[0]]
                            tmp_safeget = safeget(item, tmp_list) or 0
                            if tmp_safeget != 0 and isinstance(tmp_safeget, list):
                                tmp_safegate_sum = sum([int(x[tmp_fld_nesting[1]]) for x in tmp_safeget])
                                tmp_dict[mapping_fld_name] = tmp_safegate_sum
                            elif tmp_safeget != 0 and isinstance(tmp_safeget, dict):
                                tmp_dict[mapping_fld_name] = tmp_safeget[tmp_fld_nesting[-1]]
                            else:
                                tmp_dict[mapping_fld_name] = 0
                            continue

                        # checking level of nesting
                        tmp_fld_nesting = fld.split(".")
                        if len(tmp_fld_nesting) > 1:
                            # it returns the value that has obtained from api that has nesting for all cases
                            # result when we send base key
                            possibility1 = safeget(item, tmp_fld_nesting[0])
                            # results when we send all key
                            possibility2 = safeget(item, tmp_fld_nesting)
                            # if base key results is not known else check for another
                            # possibility results and so on, default assign None
                            if possibility1 is not None:
                                tmp_dict[mapping_fld_name] = possibility1
                            elif possibility2 is not None:
                                tmp_dict[mapping_fld_name] = possibility2
                            else:
                                tmp_dict[mapping_fld_name] = None
                        elif response_is_nested == 1:
                            tmp_dict[mapping_fld_name] = item.get(fld) or safeget(item,
                                                                                  rsp_list_path.get(mapping_fld_name))
                        elif response_is_nested != 1:
                            tmp_dict[mapping_fld_name] = item.get(fld)
                        else:
                            tmp_dict[mapping_fld_name] = "N/A"

                    tmp_item_list.append(copy.deepcopy(tmp_dict))

                """
                TITLE: 
                    Set format for next_availability_date

                DESCRIPTION:
                    Observed Behavior

                        Column next_availability_date is set to data type - text which is recording the value in the format sent to 
                        us by the Vendor’s API, and is resulting in inconsistencies.

                    Expected Behavior

                        next_availability_date should follow a standard format when storing the the database and also when 
                        returning to NetSuite through the inventory endpoint.

                Step:
                    => Check the availability of data filed next_availability_date in the data that needs to be dispatched
                    => If it exit:
                        => check the datatype of next_availability_date value
                        => if it is datetime 
                            => set that datetime object format into format like '2022-09-14 00:00:00'
                        => if it is string:
                            => check the format for next_availability_date in config file for that vendor_id
                            Note: while setting the format in vendor_configs 
                            check: https://docs.python.org/3/library/datetime.html#
                                   https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
                            => if the format for next_availability_date in config file for that vendor_id is present:
                                => convert that string (next_availability_date value) into datetime format based on format 
                                given on config file and set that datetime object format into format like 
                                '2022-09-14 00:00:00' 
                            => else:
                                => dispatch that next_availability_date value string as it is
                """

                # for updating the modified_on and next_availability_date fields
                current_time_stamp = datetime.now()
                current_time_stamp = current_time_stamp.strftime('%Y-%m-%dT%H:%M:%S.%f')
                for each_item in tmp_item_list:
                    # update only those whose vendor code we have
                    each_item["modified_on"] = current_time_stamp
                    if 'next_availability_date' in each_item and each_item['next_availability_date']:
                        try:
                            if type(each_item['next_availability_date']) == datetime:
                                each_item['next_availability_date'] = each_item['next_availability_date'].strftime(
                                    '%Y-%m-%d %H:%M:%S')

                            elif type(each_item['next_availability_date']) == str:  # for vendor_id 1930087
                                each_item['next_availability_date'] = datetime.strptime(
                                    each_item['next_availability_date'],
                                    next_availability_date_format) if next_availability_date_format else \
                                    f"Failed to convert: {each_item['next_availability_date']}"

                            else:
                                temp_date_string = each_item['next_availability_date']
                                each_item['next_availability_date'] = f"Failed to convert: {temp_date_string}"

                        except Exception as ex:
                            self.logger.warning(f"next_availability_date extraction failed \n {ex}"
                                                f"unknown format", exc_info=True)
                            each_item['next_availability_date'] = f"Failed to convert: " \
                                                                  f"{each_item['next_availability_date']}"
                    else:
                        each_item['next_availability_date'] = None

                    self.data.append(each_item)

                # execute the query here for selected vendor code self.vendor_codes get corresponding internal_id
                with self.li_db.transaction(auto_commit=True) as query_set:
                    data_query = query_set.execute_query(QUERY_FETCH_VENDOR_CODE_INTERNAL_ID_MAPPING, (self.vendor_id,))

                    if data_query:
                        self.vendor_code_to_internal_id = data_query.to_list()
                    else:
                        self.logger.info("No vendor sync candidates found - Looks like everything is uptodate")

                # Filter data to process only required vendor codes
                self.data = [x for x in self.data if
                             x.get('vendor_code') and x.get('vendor_code').lower() in [item.lower() for item in
                                                                                       self.vendor_codes]]

                # creating a hash object for vendor_code to internal id mapping
                """
                observed :[ {a:1}, {a:2},{b:3}]
                expected: {a : [1,2], b:3}
                """
                vendor_code_to_internal_id_hash = {}
                for item in self.vendor_code_to_internal_id:
                    if item.get('vendor_code').lower() in vendor_code_to_internal_id_hash:
                        temp_list_internal_ids = vendor_code_to_internal_id_hash.get(item.get('vendor_code').lower())
                        temp_list_internal_ids.append(item.get('internal_id'))
                        vendor_code_to_internal_id_hash.update({
                            item.get('vendor_code').lower(): temp_list_internal_ids
                        })
                    else:
                        vendor_code_to_internal_id_hash.update({
                            item.get('vendor_code').lower(): [item.get('internal_id')]
                        })

                temp_items = []
                # iterate through each item
                for item_data in self.data:
                    # get all the internal_id associated with each vendor_code
                    vendor_code_internal_ids = vendor_code_to_internal_id_hash.get(item_data.get('vendor_code').lower())
                    # for vendor_code having one or more internal id
                    if len(vendor_code_internal_ids) > 0:
                        # iterate through each internal id for one vendor_code
                        for each_vendor_code_internal_ids in vendor_code_internal_ids:
                            # to work independently we deepcopy the object
                            copy_item_data = copy.deepcopy(item_data)
                            copy_item_data["internal_id"] = each_vendor_code_internal_ids
                            temp_items.append(copy_item_data)
                    else:
                        self.logger.info(f'no internal_id is mapped for given vendor_code')

                # creating hash map for error response
                """
                observed: 
                data ={
                'vendor_id': [1930087, 1930087, 1930087, 1930087, 1930087, 1930087], 
                'vendor_code': ['ACTS24X7-MP11X_S2/YR', 'ACTS24X7-MP11X_S3/YR', 'ACTS24X7-MP11X_S4/YR', 
                'MP124/24S/AC/SIP', 'MP124-CONNECTION-BOX', 'MP124-PATCH-PANEL'], 
                'error': [True, True, True, False, True, False], 
                'error_description': ['Invalid Item Code', 'Invalid Item Code', 'Invalid Item Code', 'No Error', 
                'Invalid Item Code', 'No Error'], 
                'internal_id': [45289, 46597, 46626, 46629, 46628, 46627]}
                expected:
                data = {"<vendor_code>": <temp_bool_value>, ...}
                """
                vendor_code_to_error_status_hash = {}
                if self.vendor_codes_error_status:
                    for index, item in enumerate(self.vendor_codes_error_status.get('vendor_code')):
                        vendor_code_to_error_status_hash.update({
                            item.lower(): self.vendor_codes_error_status.get('error')[index]
                        })

                # defining all dispatching field None for vendor_code having error = True value
                new_dispatchable_data = []
                # iterating through each vendor record item
                for each_record in temp_items:
                    each_record_copy = copy.deepcopy(each_record)
                    #     iterating through each error mapping of vendor_codes for vendor_id
                    if vendor_code_to_error_status_hash.get(each_record_copy.get('vendor_code').lower()) is True and \
                            bool(vendor_code_to_error_status_hash) is True:
                        # creating the all Null field for that vendor_code
                        temp_vendor_code_record = dict.fromkeys(list(each_record_copy.keys()))
                        # preserving the old not nullable value
                        temp_vendor_code_record.update({
                            'vendor_code': each_record_copy.get('vendor_code'),
                            'internal_id': each_record_copy.get('internal_id'),
                            'modified_on': each_record_copy.get('modified_on')
                        })
                        # adding to the new list
                        new_dispatchable_data.append(temp_vendor_code_record)
                        # if error is false
                    else:
                        # adding to the new list
                        new_dispatchable_data.append(each_record_copy)

                self.data = json.dumps(new_dispatchable_data, default=str)
            except Exception as ex:
                self.logger.error("Could not transform data", exc_info=True)
                self.logger.exception(ex)
                raise UC_DataTransformError("Could not transform data")
            return self
        except Exception as ex:
            self.logger.error("Could not transform data", exc_info=True)
            self.logger.exception(ex)
            raise UC_DataTransformError("Could not transform data")
