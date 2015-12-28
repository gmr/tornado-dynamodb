"""
DynamoDB Tornado Client
=======================

"""
import json
import logging

from tornado_aws import client


from tornado import concurrent
from tornado import ioloop

from tornado_dynamodb import exceptions
from tornado_dynamodb import utils

__version__ = '0.1.0'

LOGGER = logging.getLogger(__name__)

TABLE_ACTIVE = 'ACTIVE'
TABLE_CREATING = 'CREATING'
TABLE_DELETING = 'DELETING'
TABLE_UPDATING = 'UPDATING'

STREAM_VIEW_NEW_IMAGE = 'NEW_IMAGE'
STREAM_VIEW_OLD_IMAGE = 'OLD_IMAGE'
STREAM_VIEW_NEW_AND_OLD_IMAGES = 'NEW_AND_OLD_IMAGES'
STREAM_VIEW_KEYS_ONLY = 'KEYS_ONLY'
STREAM_VIEW_TYPES = (STREAM_VIEW_NEW_IMAGE, STREAM_VIEW_OLD_IMAGE,
                     STREAM_VIEW_NEW_AND_OLD_IMAGES, STREAM_VIEW_KEYS_ONLY)


class DynamoDB(client.AsyncAWSClient):
    """An opinionated asynchronous DynamoDB client for Tornado

    :param str profile: Specify the configuration profile name
    :param str region: The AWS region to make requests to
    :param str access_key: The access key
    :param str secret_key: The secret access key
    :param str endpoint: Override the base endpoint URL
    :param int max_clients: Max simultaneous requests (Default: ``100``)

    :raises: :py:class:`tornado_dynamodb.exceptions.ConfigNotFound`
    :raises: :py:class:`tornado_dynamodb.exceptions.ConfigParserError`
    :raises: :py:class:`tornado_dynamodb.exceptions.NoCredentialsError`
    :raises: :py:class:`tornado_dynamodb.exceptions.NoProfileError`

    """

    def __init__(self, profile=None, region=None, access_key=None,
                 secret_key=None, endpoint=None, max_clients=100):
        """Create a new DynamoDB instance"""
        super(DynamoDB, self).__init__('dynamodb', profile, region,
                                       access_key, secret_key, endpoint,
                                       max_clients)
        self.ioloop = ioloop.IOLoop.current()

    def create_table(self, name, attributes, key_schema, read_capacity_units=1,
                     write_capacity_units=1, local_secondary_indexes=None,
                     global_secondary_indexes=None, stream_enabled=False,
                     stream_view_type=None):
        """The CreateTable operation adds a new table to your account. In an
        AWS account, table names must be unique within each region. That is,
        you can have two tables with same name if you create the tables in
        different regions.

        .. note:: You can optionally define secondary indexes on the new table,
                  as part of the CreateTable operation. If you want to create
                  multiple tables with secondary indexes on them, you must
                  create the tables sequentially. Only one table with secondary
                  indexes can be in the ``CREATING`` state at any given time.

        :param str name: The table name
        :param list attributes: A list of attribute definition key/value pairs
            where the key is the name of the attribute and the value is one of
            ``S``, ``N``, or ``B`` indicating the data type of the attribute.
        :param list key_schema: A list of key definitions that specify the
            attributes that make up the primary key for a table or an index.
            Each key pair in the list consists of the attribute name as the key
            and the index type as the value.
        :param int read_capacity_units: The maximum number of strongly
            consistent reads consumed per second before DynamoDB returns a
            :exc:`~tornado_dynamodb.exceptions.ThrottlingException`
        :param int write_capacity_units: The maximum number of writes consumed
            per second before DynamoDB returns a
            :exc:`~tornado_dynamodb.exceptions.ThrottlingException`
        :param local_secondary_indexes:
        :param global_secondary_indexes:
        :param bool stream_enabled: Indicates whether DynamoDB Streams is
            enabled (``True``) or disabled (``False``) on the table.
        :param str stream_view_type: When an item in the table is modified,
            StreamViewType determines what information is written to the stream
            for this table.
        :rtype: bool

        """
        payload = {
            'TableName': name,
            'AttributeDefinitions': attributes,
            'KeySchema': key_schema,
            'ProvisionedThroughput': {
                'ReadCapacityUnits': read_capacity_units,
                'WriteCapacityUnits': write_capacity_units
            }
        }

        # Configure streams if enabled, if not, it defaults to false
        if stream_enabled:
            if stream_view_type not in STREAM_VIEW_TYPES:
                raise ValueError('Invalid stream_view_type value: {}'.format(
                    stream_view_type))
            payload['StreamSpecification'] = {
                'StreamEnabled': True,
                'StreamViewType': stream_view_type
            }

        future = concurrent.TracebackFuture()

        def on_response(response):
            try:
                body = self._process_response(response)
            except exceptions.DynamoDBException as error:
                return future.set_exception(error)
            if self._table_status(body) == TABLE_ACTIVE:
                return future.set_result(True)
            self.ioloop.add_future(self.describe_table(name), on_response)

        request = self.fetch('POST', '/',
                             headers=self._headers('CreateTable'),
                             body=json.dumps(payload))
        self.ioloop.add_future(request, on_response)
        return future

    def delete_table(self, name):
        """The DeleteTable operation deletes a table and all of its items.
        After a DeleteTable request, the specified table is in the ``DELETING``
        state until DynamoDB completes the deletion. If the table is in the
        ``ACTIVE`` state, you can delete it. If a table is in ``CREATING`` or
        ``UPDATING`` states, then DynamoDB returns a
        :exc:`~tornado_dynamodb.exceptions.ResourceInUse`. If the
        specified table does not exist, DynamoDB returns a
        :exc:`~tornado_dynamodb.exceptions.ResourceNotFound` . If table is
        already in the ``DELETING`` state, no error is returned.

        :param str name: The table name
        :rtype: dict

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            try:
                future.set_result(self._process_response(response))
            except exceptions.DynamoDBException as error:
                future.set_exception(error)

        request = self.fetch('POST', '/',
                             headers=self._headers('DeleteTable'),
                             body=json.dumps({'TableName': name}))
        self.ioloop.add_future(request, on_response)
        return future

    def describe_table(self, name):
        """Returns information about the table, including the current status of
        the table, when it was created, the primary key schema, and any indexes
        on the table.

        :param str name: The table name
        :rtype: dict

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            try:
                future.set_result(self._process_response(response).get('Table'))
            except exceptions.DynamoDBException as error:
                future.set_exception(error)

        request = self.fetch('POST', '/',
                             headers=self._headers('DescribeTable'),
                             body=json.dumps({'TableName': name}))
        self.ioloop.add_future(request, on_response)
        return future

    @staticmethod
    def _process_response(response):
        error = response.exception()
        if error:
            raise error
        http_response = response.result()
        if not http_response:
            raise exceptions.DynamoDBException('empty response')
        body = json.loads(http_response.body.decode('utf-8'))
        if http_response.code != 200:
            if body['__type'] in exceptions.MAP:
                raise exceptions.MAP[body['__type']](body['message'])
            raise ValueError('Unhandled exception!', body)
        return body

    @staticmethod
    def _table_status(value):
        """Extract the table status out of the response of CreateTable and
        Describe table with a single method.

        :param dict value: The response value
        :rtype: str
        :raises: ValueError

        """
        if 'TableStatus' in value:
            return value['TableStatus']
        elif 'TableDescription' in value:
            return value['TableDescription'].get('TableStatus')
        raise ValueError('Unsupported response structure')

    @staticmethod
    def _headers(method):
        """Return request headers for the specified API method

        :param api method: The API method
        :type: dict

        """
        return {'Content-Type': 'application/x-amz-json-1.0',
                'x-amz-target': 'DynamoDB_20120810.{}'.format(method)}

    @staticmethod
    def _marshall_items(kwargs):
        """Common marshalling of key based kwargs.

        :param dict kwargs: The results to marshall
        :rtype: dict

        """
        for key in ['ExclusiveStartKey', 'ExpressionAttributeValues']:
            if key in kwargs:
                kwargs[key] = utils.marshall(kwargs[key])
        return kwargs

    @staticmethod
    def _unmarshall_items(results):
        """Common unmarshalling for items

        :param dict results: The results to unmarshall
        :rtype: dict

        """
        for key in ['Attributes', 'ItemCollectionKey', 'LastEvaluatedKey',
                    'Responses', 'UnprocessedKeys']:
            for item in results.get(key, {}):
                results[key][item] = utils.unmarshall(results[key][item])
        for index, value in enumerate(results.get('Items', [])):
            results['Items'][index] = utils.unmarshall(value)
        return results
