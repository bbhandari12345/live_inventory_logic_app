from LiveInventoryFetcher.scheduler.schedulerbase import *
from LiveInventoryFetcher.config import Config
from LiveInventoryFetcher.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventoryFetcher.common_utils.db_queries import QUERY_FETCH_SYNC_CANDIDATE_FROM_VENDORS, \
    QUERY_FETCH_VENDORS_CONFIG,  QUERY_FETCH_AND_PRIORITY_FETCH_SYNC_CANDIDATE_FROM_VENDORS_VENDORS_CODE_PRODUCTS, \
    QUERY_FETCH_VENDORS_CONFIG_PRIORITY,QUERY_GENERATE_ACCESS_TOKEN_CMD, QUERY_FETCH_VENDORS_CONNECTION_TYPE, \
    QUERY_FETCH_FILTER_ALLOWED_CODES


class UC_VendorSchedulerError(Exception):
    pass


FETCHER_FILE_PATH = Config.NETWORK_CONFIG.get('fetcher_directory_path')
EXTRACTOR_FILE_PATH = Config.NETWORK_CONFIG.get('extractor_data_directory_path')
CONFIG_FILE_PATH = Config.NETWORK_CONFIG.get('config_directory_path')
DATA_FILE_PATH = Config.NETWORK_CONFIG.get('data_file_path')


class VendorScheduler(SchedulerBase):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)
        self.access_token_cmds = []
        self.vendors_to_sync_cmds = []
        self.vendors_to_sync_priority_cmds = []
        self.vendors_to_sync_priority_config_dict = {}
        self.vendors_to_sync_priority_cmds = []
        self.vendors_to_sync_priority = {}
        self.vendors_to_sync_config_dict = {}
        self.vendors_to_sync_cmds = []
        self.vendors_to_sync = []
        self.vendor_internal_id_mappings_without_filter = []

        self.vendor_ids_to_sync = self.kwargs.get('vendor_ids',
                                                  None)  # If this option is present, we'll only sync these vendor ids
        self.internal_ids_to_sync = self.kwargs.get('internal_ids',
                                                    None)  # If this option is present, we'll only sync these internal ids
        if self.vendor_ids_to_sync:
            self.vendors_to_sync = self.vendor_ids_to_sync

        elif self.internal_ids_to_sync:
            self.vendor_internal_id_mappings_with_filter = self.fetch_vendors_for_internal_ids()
            self.vendors_to_sync = [row['vendor_id'] for row in self.vendor_internal_id_mappings_with_filter]

    def fetch_sync_candidates(self):
        """
        Check db for vendor_id to be synced
        :return: A list of sync candidates i.e. vendor_id
        """
        try:
            self.logger.info("Gathering fetch sync data for vendors")
            self.logger.debug("Reading database for fetching sync candidate")
            with self.li_db.transaction(auto_commit=True) as self.query_set:
                vendors_to_sync_result_set = self.query_set.execute_query(QUERY_FETCH_SYNC_CANDIDATE_FROM_VENDORS)
                if vendors_to_sync_result_set:
                    vendors_to_sync_result_list = vendors_to_sync_result_set.to_list()
                    vendors_to_sync_temp = [vendors_to_sync['vendor_id'] for vendors_to_sync in
                                            vendors_to_sync_result_list]
                    self.vendors_to_sync = vendors_to_sync_temp
                else:
                    self.logger.info("No vendor sync candidates found - Looks like everything is uptodate")
        except Exception as ex:
            self.logger.error("Could not get vendor sync candidates")
            self.logger.error(ex)
            raise UC_VendorSchedulerError("Could not get vendor sync candidates")
        self.logger.info(f"vendors to sync: {self.vendors_to_sync}")
        return self

    def fetch_sync_candidates_priority(self):
        """
        Check db for priority vendor_codes to be synced
        :return: A list of priority sync candidates i.e. vendor_codes
        """
        try:
            self.logger.info("Gathering fetch sync data for priority vendor codes")
            self.logger.debug("Reading database for fetching priority sync candidate")
            with self.li_db.transaction(auto_commit=True) as self.query_set:
                vendors_to_sync_result_set = self.query_set.execute_query(
                    QUERY_FETCH_AND_PRIORITY_FETCH_SYNC_CANDIDATE_FROM_VENDORS_VENDORS_CODE_PRODUCTS)
                if vendors_to_sync_result_set:
                    vendors_to_sync_result_list = vendors_to_sync_result_set.to_list()
                    vendor_id_dict = {}
                    for each in vendors_to_sync_result_list:
                        if each['vendor_id'] not in vendor_id_dict.keys():
                            vendor_id_dict[each['vendor_id']] = []
                        vendor_id_dict[each['vendor_id']].append(each['vendor_code'])
                    self.vendors_to_sync_priority = vendor_id_dict
                else:
                    self.logger.info("No priority vendor sync candidates found - Looks like everything is uptodate")
        except Exception as ex:
            self.logger.error("Could not get vendor codes of sync candidates")
            self.logger.error(ex)
            raise UC_VendorSchedulerError("Could not get vendor codes of sync candidates")
        self.logger.info(f"fetch_sync_candidates_priority: {self.vendors_to_sync_priority}")
        return self

    def generate_fetcher_sync_command(self):
        """
        Generate command for fetcher module
        return: list(s) of commands for fetcher module
        """
        try:
            self.logger.info("Gathering config for sync candidates i.e. vendor_id ")
            self.logger.debug("Reading database for fetching config info sync candidate i.e. vendor_id")
            with self.li_db.transaction(auto_commit=True) as query_set:
                for vendor_id in self.vendors_to_sync:
                    vendor_config_data_result_set = query_set.execute_query(QUERY_FETCH_VENDORS_CONFIG,
                                                                            {'vendor_id': vendor_id,
                                                                             'config_file_path': CONFIG_FILE_PATH,
                                                                             'fetcher_write_path': FETCHER_FILE_PATH})
                    vendor_conn_type = query_set.execute_query(QUERY_FETCH_VENDORS_CONNECTION_TYPE,
                                                               {'vendor_id': vendor_id})
                    vendor_conn_type_to_list = vendor_conn_type.to_list()
                    if vendor_config_data_result_set:
                        self.vendors_to_sync_config_dict = vendor_config_data_result_set.to_list()[0][
                            'jsonb_build_object']

                        if self.internal_ids_to_sync:
                            self.vendors_to_sync_config_dict['item_codes'] = \
                                [item_code for item_code in
                                 self.vendors_to_sync_config_dict['item_codes']
                                 if item_code in
                                 [row['vendor_codes'] for row in self.vendor_internal_id_mappings_with_filter
                                  if row['vendor_id'] == vendor_id][0]]

                        # TODO: Check: The for loop below can be removed
                        for items in vendor_conn_type_to_list:
                            if (items['connection_type'] == "csv") or (items['connection_type'] == "ftp/csv"):
                                items.update({"data_file_path": DATA_FILE_PATH + f"{vendor_id}.csv"})
                            elif items['connection_type'] == "ftp/txt":
                                items.update({"data_file_path": DATA_FILE_PATH + f"{vendor_id}.txt"})
                            self.vendors_to_sync_config_dict.update(items)
                        self.vendors_to_sync_cmds.append(self.vendors_to_sync_config_dict)
                    else:
                        self.logger.error(
                            f"Could not generate fetcher sync command for vendor_id = {vendor_id} Missing config data")
        except Exception as ex:
            self.logger.error("Could not get vendor config of sync candidates i.e. vendor_id")
            self.logger.error(ex)
            raise UC_VendorSchedulerError("Could not get vendor config of sync candidates i.e. vendor_id")
        self.logger.info(f"generate_fetcher_sync_command: {self.vendors_to_sync_cmds}")

        # vendor codes in self.vendor_internal_id_mappings_without_filter will be bypassed for ondemand sync
        return self.vendors_to_sync_cmds

    def generate_fetcher_sync_priority_command(self):
        """
        Generate command for fetcher module
        return: list(s) of commands for fetcher module
        """
        try:
            self.logger.info("Gathering config for sync candidates i.e. vendor_codes ")
            self.logger.debug("Reading database for fetching config info sync candidate i.e. vendor_id")
            with self.li_db.transaction(auto_commit=True) as query_set:
                for key in self.vendors_to_sync_priority.keys():
                    vendor_id = key
                    vendor_codes_list = self.vendors_to_sync_priority[key]
                    vendor_config_priority_data_result_set = query_set.execute_query(
                        QUERY_FETCH_VENDORS_CONFIG_PRIORITY,
                        {'vendor_id': vendor_id, 'vendor_code': vendor_codes_list, 'config_file_path': CONFIG_FILE_PATH,
                         'fetcher_write_path': FETCHER_FILE_PATH})
                    vendor_conn_type = query_set.execute_query(QUERY_FETCH_VENDORS_CONNECTION_TYPE,
                                                               {'vendor_id': vendor_id})
                    vendor_conn_type_to_list = vendor_conn_type.to_list()
                    if vendor_config_priority_data_result_set:
                        self.vendors_to_sync_priority_config_dict = vendor_config_priority_data_result_set.to_list()[0][
                            'jsonb_build_object']
                        for items in vendor_conn_type_to_list:
                            if ((items['connection_type'] == "csv") or (items['connection_type'] == "ftp/csv")):
                                items.update({"data_file_path": DATA_FILE_PATH + f"{+vendor_id}.csv"})
                            elif items['connection_type'] == "ftp/txt":
                                items.update({"data_file_path": DATA_FILE_PATH + f"{+vendor_id}.txt"})
                            self.vendors_to_sync_priority_config_dict.update(items)
                        self.vendors_to_sync_priority_cmds.append(self.vendors_to_sync_priority_config_dict)
                    else:
                        self.logger.error(
                            f"Could not generate priority fetcher sync command for vendor_id = {vendor_id}")
        except Exception as ex:
            self.logger.error("Could not get vendor config of priority sync candidates i.e. vendor_code")
            self.logger.error(ex)
            raise UC_VendorSchedulerError("Could not get vendor config of priority sync candidates i.e. vendor_code")
        self.logger.info(f"generate_fetcher_sync_priority_command: {self.vendors_to_sync_priority_cmds}")
        return self.vendors_to_sync_priority_cmds

    def generate_access_token_cmd_for_sync_candidates(self):
        """
        Check db for vendor_id to sync and generate access token command if required
        :return: A list access token command for vendor_id to sync
        """
        try:
            self.logger.info("Gathering required config to generate access token for to sync vendors")
            self.logger.debug("Reading database for generating access token")
            with self.li_db.transaction(auto_commit=True) as self.query_set:
                temp_access_token_cmd = self.query_set.execute_query(QUERY_GENERATE_ACCESS_TOKEN_CMD,
                                                                     {'config_file_path': CONFIG_FILE_PATH})
                if temp_access_token_cmd is not None:
                    for i in range(len(temp_access_token_cmd)):
                        temp_access_token_cmds = temp_access_token_cmd.to_list()[i]['jsonb_build_object']
                        self.access_token_cmds.append(temp_access_token_cmds)
                else:
                    self.logger.error("Could not generate access token command for sync candidate via vendor_id")
        except Exception as ex:
            self.logger.error("Could not generate access token command for sync candidates via vendor_id")
            self.logger.error(ex)
            raise UC_VendorSchedulerError("Could not generate access token command for sync candidates via vendor_id")
        self.logger.info(f"generate_access_token_cmd_for_sync_candidates: {self.access_token_cmds}")
        return self.access_token_cmds

    def fetch_vendors_for_internal_ids(self):
        """
        Fetch pairs of vendor_id/vendor_codes for a set of internal_ids for which allows_vc_filter is true,
        grouped by vendor_id
        """
        with self.li_db.transaction(auto_commit=True) as self.query_set:
            result_set = self.query_set.execute_query(QUERY_FETCH_FILTER_ALLOWED_CODES,
                                                      {'internal_ids': self.internal_ids_to_sync})

            filter_allowed_codes = result_set.to_list()

            return filter_allowed_codes
