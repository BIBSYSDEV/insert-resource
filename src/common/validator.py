from common.constants import Constants


def validate_resource(resource):
    if resource.resource_identifier is not None:
        raise ValueError('Resource has identifier')
    elif resource.metadata is None:
        raise ValueError('Resource has no metadata')
    elif resource.files is None:
        raise ValueError('Resource has no files')
    elif resource.owner is None:
        raise ValueError('Resource has no owner')
    elif not isinstance(resource.metadata, dict):
        raise ValueError('Resource has invalid attribute type for metadata')
    elif not isinstance(resource.files, dict):
        raise ValueError('Resource has invalid attribute type for files')
