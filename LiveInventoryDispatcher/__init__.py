import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('dispatcher function is called')
    message = json.loads(str(req.get_body(), encoding='utf-8'))
    if message:
        return func.HttpResponse(json.dumps(message))
    else:
        return func.HttpResponse( "problem while dispatcher", status_code=200 )