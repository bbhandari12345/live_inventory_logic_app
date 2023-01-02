import logging
import random 
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('schedular function is called')
    data = list(range(1, random.randint(20,30)))
    if data:
        return func.HttpResponse(json.dumps(data))
    else:
        return func.HttpResponse("problem while schduling", status_code=200 )
