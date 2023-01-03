from LiveInventorySchedular.orm.li_orm_base import LIOrmBase
from LiveInventorySchedular.schemas.schemas import ProductSchema


class LIProduct(LIOrmBase):
    __table_name__ = 'products'
    __schema__ = ProductSchema()
