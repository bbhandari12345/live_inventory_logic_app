from LiveInventoryFetcher.orm.li_orm_base import LIOrmBase
from LiveInventoryFetcher.schemas.schemas import InventorySchema
import logging
from LiveInventoryFetcher.utils.data_access_layer.sql_db import DBEngineFactory
from LiveInventoryFetcher.config import Config
from LiveInventoryFetcher.common_utils.db_queries import QUERY_UPSERT_INVENTORY

logger = logging
li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)


class LIInventory(LIOrmBase):
    __table_name__ = 'inventory'
    __schema__ = InventorySchema()


    def upsertInventory(self, data):
        try:
            logger.info("Upserting Inventory")
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_non_query(QUERY_UPSERT_INVENTORY, data)
                logger.info(f"SUCCESS - updated : , {data['vendor_id']}")
        except Exception as ex:
            logger.error(f"Could not upsert for vendor_id = {data['vendor_id']}")
            raise logger.error(ex)
