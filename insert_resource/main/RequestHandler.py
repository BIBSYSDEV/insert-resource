import http
import json
import os
import uuid

import arrow as arrow
import boto3
from boto3_type_annotations.dynamodb import Table

from .common.constants import Constants
from .common.helpers import response
from .common.validator import validate_resource
from .data.resource import Resource


class RequestHandler:

    def __init__(self, dynamodb=None):
        if dynamodb is None:
            self.dynamodb = boto3.resource('dynamodb', region_name=os.environ[Constants.ENV_VAR_REGION])
        else:
            self.dynamodb = dynamodb

        self.table_name = os.environ.get(Constants.ENV_VAR_TABLE_NAME)
        self.table: Table = self.dynamodb.Table(self.table_name)

    def get_table_connection(self):
        return self.table

    def insert_resource(self, generated_uuid, current_time, resource):
        ddb_response = self.table.put_item(
            Item={
                Constants.DDB_FIELD_RESOURCE_IDENTIFIER: generated_uuid,
                Constants.DDB_FIELD_MODIFIED_DATE: current_time,
                Constants.DDB_FIELD_CREATED_DATE: current_time,
                Constants.DDB_FIELD_METADATA: resource.metadata,
                Constants.DDB_FIELD_FILES: resource.files,
                Constants.DDB_FIELD_OWNER: resource.owner
            }
        )
        return ddb_response

    def handler(self, event, context):
        if event is None or Constants.EVENT_BODY not in event or Constants.EVENT_HTTP_METHOD not in event:
            return response(http.HTTPStatus.BAD_REQUEST, Constants.ERROR_INSUFFICIENT_PARAMETERS)

        body = json.loads(event[Constants.EVENT_BODY])
        http_method = event[Constants.EVENT_HTTP_METHOD]
        resource_dict_from_json = body.get(Constants.JSON_ATTRIBUTE_NAME_RESOURCE)

        try:
            resource = Resource.from_dict(resource_dict_from_json)
        except TypeError as e:
            return response(http.HTTPStatus.BAD_REQUEST, e.args[0])

        current_time = arrow.utcnow().isoformat()

        resource_not_none = resource is not None
        if http_method == Constants.HTTP_METHOD_POST and resource_not_none:
            try:
                validate_resource(resource)
            except ValueError as e:
                return response(http.HTTPStatus.BAD_REQUEST, e.args[0])
            generated_uuid = uuid.uuid4().__str__()
            ddb_response = self.insert_resource(generated_uuid, current_time, resource)
            ddb_response['resource_identifier'] = generated_uuid
            return response(http.HTTPStatus.CREATED, json.dumps(ddb_response))

        return response(http.HTTPStatus.BAD_REQUEST, Constants.ERROR_INSUFFICIENT_PARAMETERS)
