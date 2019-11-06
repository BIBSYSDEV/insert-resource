from .constants import Constants


def remove_none_values(temp_value):
    return_value = dict()
    for key, value in temp_value.items():
        if value is not None:
            return_value[key] = value
    return return_value


def response(status_code, body):
    return {
        Constants.RESPONSE_STATUS_CODE: status_code,
        Constants.RESPONSE_BODY: body
    }
