def remove_none_values(temp_value):
    return_value = dict()
    for key, value in temp_value.items():
        if value is not None:
            return_value[key] = value
    return return_value
