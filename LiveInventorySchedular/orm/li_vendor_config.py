from LiveInventorySchedular.orm.li_orm_base import LIOrmBase
from LiveInventorySchedular.schemas.schemas import VendorConfigSchema
from LiveInventorySchedular.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventorySchedular.common_utils.db_queries import QUERY_UPDATE_BEARER_TOKEN
import logging
from LiveInventorySchedular.config import Config

li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)

# get the global reference og logger
logger = logging


class TokenUpdateException(Exception):
    pass


class LIVendorsConfig(LIOrmBase):
    __table_name__ = 'vendor_configs'
    __schema__ = VendorConfigSchema()

    def updateAuthorization(self, data):
        try:
            field_to_update = data.get('field_to_update')
            auth_data = data[field_to_update]
            vendor_id = data['vendor_id']
            logger.info('updating vendor with new token generator')
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_non_query(QUERY_UPDATE_BEARER_TOKEN, {'vendor_id': vendor_id, 'auth_data': auth_data,
                                                                        'field_to_update': field_to_update})
        except Exception as ex:
            self.logger.error(ex, "Could not update bearer token")
            raise TokenUpdateException("Could not update bearer token")
