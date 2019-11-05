from common.helpers import remove_none_values
from data.creator import Creator
from data.file_metadata import FileMetadata
from data.metadata import Metadata
from data.resource import Resource


def encode_file_metadata(instance):
    if isinstance(instance, FileMetadata):
        temp_value = {
            'filename': instance.filename,
            'mimetype': instance.mime_type,
            'checksum': instance.checksum,
            'size': instance.size
        }
        return remove_none_values(temp_value)
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def encode_files(instance):
    if instance is None:
        return None
    elif isinstance(instance, dict):
        files = dict()
        for key, value in instance.items():
            files[key] = encode_file_metadata(value)
        return files
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def encode_creator(instance):
    if isinstance(instance, Creator):
        return instance.identifier
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def encode_metadata(instance):
    if instance is None:
        return None
    elif isinstance(instance, Metadata):

        if instance.creators is None:
            creators = None
        else:
            creators = []
            for creator in instance.creators:
                creators.append(encode_creator(creator))

        if instance.titles is None:
            titles = None
        else:
            titles = dict()
            for key, value in instance.titles.items():
                if value is not None:
                    titles[key] = value
            if len(titles.keys()) is 0:
                titles = None
        temp_value = {
            'creators': creators,
            'handle': instance.handle,
            'license': instance.license_identifier,
            'publicationYear': instance.publication_year,
            'publisher': instance.publisher,
            'titles': titles,
            'type': instance.resource_type
        }
        return remove_none_values(temp_value)
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type {type_name} is not JSON serializable")


def encode_resource(instance):
    if instance is None:
        return None
    if isinstance(instance, Resource):
        temp_value = {
            'resource_identifier': instance.resource_identifier,
            'modifiedDate': instance.modified_date,
            'createdDate': instance.created_date,
            'metadata': encode_metadata(instance.metadata),
            'files': encode_files(instance.files),
            'owner': instance.owner,
            'status': instance.status,
            'publishedDate': instance.published_date,
            'indexedDate': instance.indexed_date
        }
        return remove_none_values(temp_value)
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")
