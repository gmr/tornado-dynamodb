"""
DynamoDB Tornado Client
=======================

"""
import logging

from tornado_aws import client
from tornado_aws.exceptions import ConfigNotFound
from tornado_aws.exceptions import ConfigParserError
from tornado_aws.exceptions import NoCredentialsError
from tornado_aws.exceptions import NoProfileError
from tornado import concurrent

from tornado_dynamodb import utils

__version__ = '0.1.0'

LOGGER = logging.getLogger(__name__)


class DynamoDB(client.AsyncAWSClient):
    """For better or worse, :class:`~tornado_dynamodb.DynamoDB` strives to be
    as close as possible to Boto3's DynamoDB client without the overhead that
    it provides.

    One major difference, however is nstead of having to pass in attributes
    such as the ``Item`` attribute in
    :func:`~tornado_dynamodb.DynamoDB.put_item` with the nested
    ``{"key": {"TYPE": "value"}}`` structure, such data structures will
    automatically be marshalled and unmarshalled when they are returned from
    DynamoDB.

    .. note:: Deprecated request attributes such as ``QueryFilter`` in
              :py:func:`botocore.client.DynamoDB.query` are not automatically
              marshalled, as they should not be used.

    :param str profile: Specify the configuration profile name
    :param str region: The AWS region to make requests to
    :param str access_key: The access key
    :param str secret_key: The secret access key
    :param str endpoint: Override the base endpoint URL
    :param int max_clients: Max simultaneous requests (Default: ``100``)
    :raises: :py:class:`tornado_dynamodb.ConfigNotFound`
    :raises: :py:class:`tornado_dynamodb.ConfigParserError`
    :raises: :py:class:`tornado_dynamodb.NoCredentialsError`
    :raises: :py:class:`tornado_dynamodb.NoProfileError`

    """
    def __init__(self, profile=None, region=None, access_key=None,
                 secret_key=None, endpoint=None, max_clients=100):
        """Create a new DynamoDB instance

        :param str profile: Specify the configuration profile name
        :param str region: The AWS region to make requests to
        :param str access_key: The access key
        :param str secret_key: The secret access key
        :param str endpoint: Override the base endpoint URL
        :param int max_clients: Max simultaneous requests (Default: ``100``)
        :raises: :py:class:`tornado_aws.exceptions.ConfigNotFound`
        :raises: :py:class:`tornado_aws.exceptions.ConfigParserError`
        :raises: :py:class:`tornado_aws.exceptions.NoCredentialsError`
        :raises: :py:class:`tornado_aws.exceptions.NoProfileError`

        """
        super(DynamoDB, self).__init__('dynamodb', profile, region,
                                       access_key, secret_key, endpoint,
                                       max_clients)

    def create_table(self, **kwargs):
        """The CreateTable operation adds a new table to your account. In an
        AWS account, table names must be unique within each region. That is,
        you can have two tables with same name if you create the tables in
        different regions.

        CreateTable is an asynchronous operation. Upon receiving a CreateTable
        request, DynamoDB immediately returns a response with a TableStatus of
        ``CREATING`` . After the table is created, DynamoDB sets the
        TableStatus to ``ACTIVE`` . You can perform read and write operations
        only on an ``ACTIVE`` table.

        You can optionally define secondary indexes on the new table, as part
        of the CreateTable operation. If you want to create multiple tables
        with secondary indexes on them, you must create the tables
        sequentially. Only one table with secondary indexes can be in the
        ``CREATING`` state at any given time.

        You can use the DescribeTable API to check the table status.

        """
        pass

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
