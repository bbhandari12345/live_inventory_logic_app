import logging
import json
import azure.functions as func
import logging as logger

from LiveInventoryDispatcher.db_dispatcher.json_loader import DataDispatcher


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('dispatcher function is called')
    message = json.loads(str(req.get_body(), encoding='utf-8'))
    dispatcher_sync_status = []
    for i, x in enumerate(message):
        try:
            logger.debug(f"PROCESSING - Dispatching data for vendor_id: {x.get('vendor_id')} and vendor_code(s): {x.get('item_codes')}")
            data_dispatcher = DataDispatcher(vendor_id=x.get('vendor_id'),
                                             item_codes=x.get('item_codes'),
                                             extractor_file_path=x.get('extractor_file_path')).execute()
            logger.debug(f"SUCCESS - Dispatched data for vendor_id: {x.get('vendor_id')} and vendor_code(s): {x.get('item_codes')}")
            dispatcher_sync_status.append({
                "vendor_id": data_dispatcher.kwargs.get('vendor_id'),
                "total number of item dispatched": len(data_dispatcher.update_data),
                "priority_sync": bool(data_dispatcher.kwargs.get('is_priority')),
                "ondemand_sync": bool(data_dispatcher.kwargs.get('internal_id_override_list')),
                "requested_vendor_code_length": len(data_dispatcher.kwargs.get('item_codes'))
            })
        except Exception as ex:
            dispatcher_sync_status.append({})
            logger.error(ex, exc_info=True)
    if dispatcher_sync_status:
        return func.HttpResponse(str(dispatcher_sync_status))
    else:
        return func.HttpResponse( "problem while dispatcher", status_code=200 )