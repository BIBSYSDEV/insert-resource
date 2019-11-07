import http

from common.constants import Constants
from common.helpers import response

from main.RequestHandler import RequestHandler


def handler(event, context):
    if event is None:
        raise ValueError(Constants.ERROR_MISSING_EVENT)
    else:
        try:
            request_handler = RequestHandler()
        except Exception as e:
            return response(http.HTTPStatus.INTERNAL_SERVER_ERROR, e.args[0])
        return request_handler.handler(event, context)
