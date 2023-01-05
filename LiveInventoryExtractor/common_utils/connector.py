"""
this module contains the function that
connect routes with db utils
"""
from LiveInventoryExtractor.config import Config
from LiveInventoryExtractor.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventoryExtractor.common_utils.db_utils import generate_sql_get_disabled_vendors
from LiveInventoryExtractor.common_utils.db_queries import QUERY_DELETE_OLD_RECORD_INVENTORY
import logging

li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)

# get the logger instance
logger = logging


def get_vendors_disabled(select_key, identifier, identifier_keys, identifier_type, table_name):
    try:
        get_row_vendor_config_GENERATED_SQL = generate_sql_get_disabled_vendors(
            select_key=select_key,
            identifier=identifier,
            identifier_keys=identifier_keys,
            identifier_type=identifier_type,
            table_name=table_name
        )

        with li_db.transaction(auto_commit=True) as query_set:
            result_set = query_set.execute_query(get_row_vendor_config_GENERATED_SQL)
            logger.debug(f'Executed Query {query_set.query}')
            return_set = result_set.to_list()
            return return_set
    except Exception as exc:
        logger.error('Error in get_all execution.', exc_info=True)
        raise exc


def update_last_fetch_date_vendor_codes_all(data):
    try:
        sql = "update vendor_codes set last_fetch_date = now() where (vendor_id, vendor_code, internal_id) in %(data)s;"
        with li_db.transaction(auto_commit=True) as query_set:
            query_set.execute_query(sql, {'data': data})
            logger.debug(f'Executed Query {query_set.query}')
            return True
    except Exception:
        logger.error('Error in Error in dispatching the last fetch date to the vendor_code table', exc_info=True)
        return False


# define a method that delete the old record from inventory than the given days
def delete_old_record_inventory():
    """
    it delete the old record from the inventory table
    Returns
    -------
    """
    logger.info(f'function that delete the old invalid item from inventory is called')
    try:
        with li_db.transaction(auto_commit=True) as query_set:
            query_set.execute_non_query(QUERY_DELETE_OLD_RECORD_INVENTORY, {
                'invalid_record_age': Config.TIME_INTERVAL_DELETE_INVALID_RECORD_INVENTORY
            })
            logger.info(f'Executed Query {query_set.query}')
            logger.info(f'successfully deleted the invalid old item from inventory table')

    except Exception as ex:
        logger.error("exceptions occurred while deleting the old invalid item from inventory is called", exc_info=True)
        raise ex
