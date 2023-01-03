"""LI Database and other Schema definition.

Marshmallow  for schema definition/validation.
"""
from marshmallow import Schema, fields, pre_load


class VendorsSchema(Schema):
    vendor_id = fields.Int(dump_only=True)
    vendor_name = fields.Str()
    vendor_desc = fields.Str()
    vendor_url = fields.Url()
    connection_type = fields.Str()
    config_path = fields.Str()
    sync_interval = fields.TimeDelta(dump_only=True)
    last_fetch_date = fields.AwareDateTime(dump_only=True)
    token_life_seconds = fields.TimeDelta()


class VendorCodesSchema(Schema):
    vendor_code = fields.Str()
    vendor_id = fields.Int()
    last_fetch_date = fields.DateTime(dump_only=True)
    internal_id = fields.Int(allow_none=False)
    error = fields.Boolean(allow_none=True)
    error_description = fields.Str(allow_none=True)
    preferred = fields.Boolean(allow_none=True)


class InventorySchema(Schema):
    vendor_code = fields.Str(dump_only=True)
    vendor_id = fields.Int(dump_only=True)
    cost = fields.Float(allow_none=True)
    currency = fields.Str(allow_none=True)
    next_availability_date = fields.Str(dump_only=True, allow_none=True)
    availability_count = fields.Int(allow_none=True)
    availability_status = fields.Boolean(allow_none=True)

    @pre_load
    def handle_missing_data(self, data, **kwargs):
        if data.get('availability_status') is None or isinstance(data.get('availability_status'), str):
            data['availability_status'] = None
        if data.get('cost') is not None and isinstance(data.get('cost'), int):
            data['cost'] = float(data['cost'])
        if isinstance(data.get('cost'), str):
            data['cost'] = float(data['cost'].replace(",", ""))
        if data.get('availability_count') is not None:
            data['availability_count'] = int(data.get('availability_count'))
        return data


class VendorConfigSchema(Schema):
    vendor_id = fields.Int(dump_only=True)
    key_name = fields.Str(dump_only=True)
    value = fields.Str(dump_only=True)


class ProductSchema(Schema):
    internal_id = fields.Int(dump_only=True, allow_none=False)
    product_name = fields.Str()
    product_description = fields.Str()
    product_sku = fields.Str()
    created_on = fields.DateTime(dump_only=True)
    modified_on = fields.AwareDateTime(dump_only=True)
