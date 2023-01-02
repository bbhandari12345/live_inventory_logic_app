from LiveInventorySchedular.common_utils.connector import get_vendors_disabled
from LiveInventorySchedular.scheduler.vendor_scheduler import VendorScheduler
import json
import azure.functions as func
import logging
from LiveInventorySchedular.token_generator import BearerTokenGenerator

logger = logging


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('schedular function is called')
    refresh_access_tokens()
    sync_priority, sync = VendorScheduler().execute()

    # flag for product with priority
    for item in sync_priority:
        item['is_priority'] = True
    vendor_list = sync_priority + sync
    if vendor_list:
        return func.HttpResponse(json.dumps(vendor_list))
    else:
        return func.HttpResponse("problem while scheduling", status_code=200)


def refresh_access_tokens():
    """ Refreshes access tokens for the vendors for which access tokens need to be refreshed """

    logger.info("\n-= REFRESHING ACCESS TOKENS =-")
    token_generator_command = VendorScheduler().generate_access_token_cmd_for_sync_candidates()
    # get all the vendor_id that has field enabled=False
    select_key = 'vendor_id'
    identifier = [{'enabled': False}]
    identifier_keys = ['enabled']
    identifier_type = ['int']
    table_name = 'vendors'
    enabled_vendors = list(filter(lambda x: (x.get('vendor_id') not in ([x.get('vendor_id')
                                                                         for x in get_vendors_disabled(
            select_key, identifier, identifier_keys, identifier_type, table_name)])), token_generator_command))
    for x in enabled_vendors:
        try:
            logger.info(f"PROCESSING - Updating expired web token for: {x.get('vendor_id')}")
            token_generator = BearerTokenGenerator(vendor_id=x.get('vendor_id'),
                                                   config_file_path=x.get('config_file_path'),
                                                   template_values=x.get('template_values')). \
                execute()
            logger.info(f"SUCCESS - Re-newed web token for: {x.get('vendor_id')}")
        except Exception as ex:
            logger.error(ex)
            logger.error(
                f"FAILURE - Could not access token API request for: {x.get('vendor_id')}, and generate fetcher command")
            continue
    logger.info("\n-= SUCCESS - REFRESHED ACCESS TOKENS =-")
