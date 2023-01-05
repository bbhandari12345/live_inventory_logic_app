from LiveInventoryDispatcher.common_utils.db_queries import QUERY_UPDATE_LAST_MOD_VENDORS, \
    QUERY_UPDATE_LAST_FETCH_ACCESS_TOKEN_VENDORS, QUERY_UPDATE_RESPONSE_CODE_RESPONSE_TEXT_IN_VENDORS
from LiveInventoryDispatcher.orm.li_orm_base import LIOrmBase
from LiveInventoryDispatcher.schemas.schemas import VendorsSchema
import logging
from LiveInventoryDispatcher.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventoryDispatcher.config import Config

li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)

# get the logger instance
logger = logging


class DataModtimeException(Exception):
    pass


class LIVendors(LIOrmBase):
    __table_name__ = 'vendors'
    __schema__ = VendorsSchema()

    def update_fetch_date(self, data):
        try:
            vendor_id = data
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_non_query(QUERY_UPDATE_LAST_MOD_VENDORS, {'vendor_id': vendor_id})
                logger.info(f"SUCCESS - Updated last modified time for: {vendor_id}")
        except Exception as ex:
            logger.debug(ex)
            logger.error(f"Could not update last fetch date for vendor_id = {vendor_id}")
            raise DataModtimeException(f"Could not update last fetch date for vendor_id = {vendor_id}")

    def update_token_expiry(self, data):
        try:
            vendor_id = data.get('vendor_id')
            logger.info(f"Updating expired token for: {vendor_id}")
            token_life_seconds = data.get('expires_in')
            if isinstance(token_life_seconds, int):
                token_life_seconds = str(data.get('expires_in'))
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_non_query(QUERY_UPDATE_LAST_FETCH_ACCESS_TOKEN_VENDORS,
                                            {'vendor_id': vendor_id, 'token_life_seconds': token_life_seconds})
                logger.info(f"SUCCESS - Updated token for: {vendor_id}")
        except Exception as ex:
            logger.debug(ex)
            logger.error(f"Could not update token for vendor_id = {vendor_id}")
            raise DataModtimeException(f"Could not update token for = {vendor_id}")

    def update_response_code_text_in_vendors(self, my_data):
        try:
            vendor_id = my_data.get('vendor_id')
            response_code = my_data.get('response_code')
            response_text = my_data.get('response_text')
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_non_query(QUERY_UPDATE_RESPONSE_CODE_RESPONSE_TEXT_IN_VENDORS,
                                            {'vendor_id': vendor_id,
                                             'response_code': response_code,
                                             'response_text': response_text
                                             })
                logger.info(f"SUCCESS - Updated response code and text for vendor_id: {vendor_id}")
        except Exception as ex:
            logger.debug(ex)
            logger.error(f"ERROR-Could not update response code and response text for vendor_id = {vendor_id}",
                         exc_info=True)
            raise DataModtimeException(f"Could not update response code and response text for vendor_id = {vendor_id}")
