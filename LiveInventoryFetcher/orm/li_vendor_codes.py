from LiveInventoryFetcher.orm.li_orm_base import LIOrmBase
from LiveInventoryFetcher.schemas.schemas import VendorCodesSchema
from LiveInventoryFetcher.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventoryFetcher.common_utils.db_queries import QUERY_BULK_UPDATE_LAST_FETCHED_VENDOR_CODES, \
    QUERY_UPDATE_LAST_MOD_VENDOR_CODES, UPDATE_LAST_FETCH_DATE_OF_VENDOR_CODE_FOR_SINGLE_VENDOR
from LiveInventoryFetcher.config import Config
from LiveInventoryFetcher.common_utils.db_utils import generate_insert_sql, generate_bulk_update_sql
from typing import Any
import logging

li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)
# get the global reference of logger
logger = logging


class UpdateVendorCodesLastfetchException(Exception):
    pass


class LIVendorCodes(LIOrmBase):
    __table_name__ = 'vendor_codes'
    __schema__ = VendorCodesSchema()

    def bulk_update(self, row_value: Any = None, identifier: str = 'id', vendor_id=None, vendor_codes=None):
        """
        Update info of vendor_codes on the basis of response from vendor
        """

        self.check_src_data()
        if self.is_loaded is False and self.is_dumped is False:
            logger.critical('Data should be loaded or dumped first to update.')
            raise ValueError('.load() or .dump() must be called before update')

        logger.info('Executing bulk_update_data.')

        sql = generate_bulk_update_sql(self.get_table(), row_value, identifier)
        try:
            with li_db.transaction(auto_commit=False) as qryset:
                try:
                    qryset.execute_non_query(sql, row_value)
                    logger.debug(f'Executed non Query {qryset.query}')

                    qryset.execute_non_query(QUERY_BULK_UPDATE_LAST_FETCHED_VENDOR_CODES,
                                             {'vendor_codes': vendor_codes, 'vendor_id': vendor_id})
                    logger.info(
                        f"SUCCESS - Updated last fetched date for: vendor_id = {vendor_id} and vendor_code IN "
                        f"{vendor_codes}")
                    li_db.commit()
                except Exception as e:
                    logger.error(e)
                    li_db.rollback()
        except Exception as e:
            logger.error('Error in bulk_update_data execution')
            logger.error(e)

    def bulk_update_last_fetch_date_vendor_codes(self, vendor_id: int):
        """
        Update last_fetch_date for all vendor_codes  of a particular vendor

        :param vendor_id: Id of vendor whose vendor_codes's last_fetch_date needs to be updated
        :type vendor_id: int
        """

        try:
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_non_query(UPDATE_LAST_FETCH_DATE_OF_VENDOR_CODE_FOR_SINGLE_VENDOR,
                                            {'vendor_id': vendor_id})
                logger.info(f"SUCCESS - Updated last fetched date for all vendor_codes of vendor_id = {vendor_id}")
        except Exception as ex:
            logger.warn(ex)
            logger.error(f"Could not update last fetch date for all vendor_codes of vendor_id = {vendor_id}")
            raise UpdateVendorCodesLastfetchException(
                f"Could not update last fetch date for all vendor_codes of vendor_id = {vendor_id}")

    def update_fetch_date_vendor_codes(self, vendor_id, vendor_code):
        try:
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_non_query(QUERY_UPDATE_LAST_MOD_VENDOR_CODES,
                                            {'vendor_id': vendor_id, 'vendor_code': vendor_code})
                logger.info(
                    f"SUCCESS - Updated last modified time for: vendor_id = {vendor_id} and vendor_code= {vendor_code}")
        except Exception as ex:
            logger.warn(ex)
            logger.error(f"Could not update last fetch date for vendor_id = {vendor_id} and vendor_code= {vendor_code}")
            raise UpdateVendorCodesLastfetchException(
                f"Could not update last fetch date for vendor_id = {vendor_id} and vendor_code= {vendor_code}")

    def comprehensive_save(self, src_data):
        try:
            logger.info("Updating vendor codes")
            inventory_data = src_data['vendor_code']
            vendor_codes = []

            for index, data in enumerate(inventory_data):
                inventory_data = LIVendorCodes(data).load()
                sql = generate_insert_sql(LIVendorCodes.get_table(), inventory_data, ['vendor_code'], True)
                values = list(inventory_data.values())
                with li_db.transaction(auto_commit=True) as query_set:
                    ans = query_set.execute_query(sql, values)
                    logger.info(f"Executed query: {query_set.query}")
                    vendor_codes.append(ans.to_list()[0]['vendor_code'])
                    li_db.commit()
        except Exception as ex:
            logger.error("Error", exc_info=True)
            raise ex
