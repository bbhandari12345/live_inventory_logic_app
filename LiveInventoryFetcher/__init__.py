import json
import azure.functions as func
import logging
from LiveInventoryFetcher.scheduler.vendor_scheduler import EXTRACTOR_FILE_PATH
from LiveInventoryFetcher.orm import li_vendors
from LiveInventoryFetcher.fetcher import RESTJSONFetcher
from LiveInventoryFetcher.fetcher.csv_fetcher import CSVFetcher
from LiveInventoryFetcher.fetcher.ftp_fetcher import FTPFetcher
from LiveInventoryFetcher.fetcher.rest_xml_fetcher import RESTXMLFetcher

logger = logging


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('fetcher function is called')
    rest_fetcher = None
    x = json.loads(str(req.get_body(), encoding='utf-8'))
    logger.info(f"Syncing vendor_id: {x.get('vendor_id')} vendor_codes: {x.get('item_codes')}")
    try:
        if x.get('connection_type') == "xml":
            rest_fetcher = RESTXMLFetcher(vendor_id=x.get('vendor_id'), config_file_path=x.get('config_file_path'),
                                          fetcher_write_path=x.get('fetcher_write_path'),
                                          item_codes=x.get('item_codes'),
                                          template_values=x.get('template_values')).execute()
        elif x.get('connection_type') == "csv":
            rest_fetcher = CSVFetcher(vendor_id=x.get('vendor_id'), config_file_path=x.get('config_file_path'),
                                      data_file_path=x.get('data_file_path'),
                                      fetcher_write_path=x.get('fetcher_write_path'),
                                      item_codes=x.get('item_codes'),
                                      template_values=x.get('template_values')).execute()
        elif (x.get('connection_type') == "ftp/csv") or (x.get('connection_type') == "ftp/txt"):
            rest_fetcher = FTPFetcher(vendor_id=x.get('vendor_id'), config_file_path=x.get('config_file_path'),
                                      data_file_path=x.get('data_file_path'),
                                      fetcher_write_path=x.get('fetcher_write_path'),
                                      item_codes=x.get('item_codes'),
                                      template_values=x.get('template_values')).execute()
        else:
            logger.debug(
                f"PROCESSING - Making REST fetcher Making API request for: {x.get('vendor_id')}, vendor_code(s): {x.get('item_codes')}")
            rest_fetcher = RESTJSONFetcher(vendor_id=x.get('vendor_id'), config_file_path=x.get('config_file_path'),
                                           fetcher_write_path=x.get('fetcher_write_path'),
                                           item_codes=x.get('item_codes'),
                                           template_values=x.get('template_values')).execute()

            logger.debug(
                f"SUCCESS - Making API request for: {x.get('vendor_id')}, vendor_code(s): {x.get('item_codes')}")

        # based on response info , update the vendors table
        # escaping the vendors table(response_code, response_text) update on ondemand api call
        my_vendors_1 = li_vendors.LIVendors()
        my_vendors_1.update_response_code_text_in_vendors(rest_fetcher.response_info)
        logger.info(f"SUCCESS -update vendors table ")

    except Exception as ex:
        logger.error(ex)
        logger.error(f"FAILURE - Failed fetching data for: {x.get('vendor_id')},"
                     f" vendor_code(s): {x.get('item_codes')} and generate fetcher file", exc_info=True)

    logger.info(rest_fetcher)
    fetcher_result = {
            "vendor_id": x.get('vendor_id'),
            "config_file_path": x.get('config_file_path'),
            "fetcher_file_path": rest_fetcher.meta['fetcher_data_file_path'],
            "item_codes": x.get('item_codes'),
            "vendor_codes_error_status": rest_fetcher.vendor_codes_error_status,
            "extractor_write_path": EXTRACTOR_FILE_PATH
    }

    if fetcher_result:
        return func.HttpResponse(json.dumps(fetcher_result))
    else:
        return func.HttpResponse("problem while fetching", status_code=200)
