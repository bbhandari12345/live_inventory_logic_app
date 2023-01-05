import logging
import json
import azure.functions as func
import logging as logger

from LiveInventoryExtractor.extractor.json_extractor import JSONExtractor


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('extractor function is called')
    message = json.loads(str(req.get_body(), encoding='utf-8'))
    #  the message should be list of dictionary
    result = []
    for index, x in enumerate(message):
        try:
            logger.debug(
                f"PROCESSING - Fetcher for vendor_id: {x.get('vendor_id')} and vendor_code(s): {x.get('item_codes')}")
            json_extractor = JSONExtractor(vendor_id=x.get('vendor_id'),
                                           config_file_path=x.get('config_file_path'),
                                           fetcher_file_path=x.get('fetcher_file_path'),
                                           item_codes=x.get('item_codes'),
                                           vendor_codes_error_status=x.get('vendor_codes_error_status'),
                                           extractor_write_path=x.get('extractor_write_path')).execute()
            logger.debug(
                f"SUCCESS - Reading and processing fetcher for vendor_id: {x.get('vendor_id')} and vendor_code(s): {x.get('item_codes')}")
            json_extractor = {
                "vendor_id": x.get('vendor_id'),
                "item_codes": x.get('item_codes'),
                "extractor_file_path": json_extractor.meta['extractor_data_file_path']
            }
        except Exception as ex:
            logger.error(f'error: {ex}', exc_info=True)
            json_extractor = {}
        result.append(json_extractor)
    if result:
        return func.HttpResponse(str(message))
    else:
        return func.HttpResponse("problem while extractor", status_code=200)
