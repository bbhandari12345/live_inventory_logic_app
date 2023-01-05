from LiveInventoryDispatcher.db_dispatcher.dispatcherbase import *
from LiveInventoryDispatcher.orm.li_inventory import LIInventory
from LiveInventoryDispatcher.orm.li_vendor_codes import LIVendorCodes
from LiveInventoryDispatcher.orm.li_vendors import LIVendors
from LiveInventoryDispatcher.schemas.schemas import InventorySchema


class UC_DataDispatchError(Exception):
    pass


class DataDispatcher(DispatcherBase):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.not_null_columns = ('internal_id', 'modified_on')

    def load_data(self) -> Any:

        try:
            self.logger.info("loading data from extractor")
            self.logger.debug("Reading extractor data")
            with open(self.kwargs['extractor_file_path'], 'r') as data_file:
                self.update_data = json.load(data_file)
        except Exception as ex:
            self.logger.error("Could not dispatch data", exc_info=True)
            self.logger.exception(ex)
            raise UC_DataDispatchError("Could not transform data")

        return self

    def dispatch(self):
        try:
            update_vendor_codes = LIVendorCodes()
            vendors_conf = LIVendors(self.update_data)
            vendor_id = self.kwargs.get('vendor_id')
            is_priority = self.kwargs.get('is_priority')
            internal_id_override_list = self.kwargs.get('internal_id_override_list')
            # for idx, items in enumerate(self.update_data):
            #     if not items.get('vendor_code') or items.get('vendor_code') is None:
            #         self.update_data.remove(items)
            # continue
            self.update_data = list(filter(lambda p: (
                        p.get('vendor_code') is not None and p['vendor_code'].lower() in [item.lower() for item in
                                                                                          self.kwargs.get(
                                                                                              'item_codes')]),
                                           self.update_data))
            for items in self.update_data:
                items.update({"vendor_id": vendor_id})
            #     this code checks the duplicate item
            new_data = []
            _ = [new_data.append(x) for x in self.update_data if (
                    {
                        "vendor_code": x.get('vendor_code'),
                        "vendor_id": x.get('vendor_id'),
                        "internal_id": x.get('internal_id'),
                    } not in [
                        {
                            "vendor_code": y.get('vendor_code'),
                            "vendor_id": y.get('vendor_id'),
                            "internal_id": y.get('internal_id'),
                        } for y in new_data])]
            self.update_data = new_data
            try:
                # inserting the null value in case if the api does not give response for that field and row
                inventory_instance = InventorySchema()
                inventory_columns = list(inventory_instance.fields.keys())
                for item in self.update_data:
                    fields_to_set_none = [x for x in inventory_columns if x not in list(item.keys()) and x not in
                                          self.not_null_columns]
                    for none_field in fields_to_set_none:
                        item[none_field] = None
                inventory = LIInventory(self.update_data)
                inventory.load(partial=True)
                inventory.upsert(self.update_data, conflict_fields="vendor_code, vendor_id, internal_id")
            except Exception as err:
                self.logger.exception(err, exc_info=True)
            # for data in self.update_data:
            #     self.logger.info(f"loading data for dispatcher {data}")
            #     data.update({'vendor_id': vendor_id})
            #     inventory = LIInventory(data)
            #     inventory.load(partial=True)
            #     self.logger.info(f"logged data: {data}")
            #     inventory.upsert(data)

            # update vendors_codes.last_fetch_date
            # update_last_fetch_date_vendor_codes_all(tuple([(x.get('vendor_id'), x.get('vendor_code'), x.get('internal_id')) for x in self.update_data]))
            try:
                # update_vendor_codes.update_fetch_date_vendor_codes(vendor_id, data['vendor_code'])
                # escaping the vendors table(last fetch date) update on ondemand api call
                if not is_priority and not internal_id_override_list:
                    vendors_conf.update_fetch_date(vendor_id)
            except Exception as ex:
                self.logger.error(ex, exc_info=True)
                self.logger.error(f"exception occurred while updating last fetch date for {vendor_id}")
                raise UC_DataDispatchError(f"Could not update data for {vendor_id}")
        except Exception as ex:
            self.logger.error(ex, exc_info=True)
            self.logger.warn(f"exception occurred while dispatching data for {vendor_id}")
            raise UC_DataDispatchError("Could not dispatch data")
        return self
