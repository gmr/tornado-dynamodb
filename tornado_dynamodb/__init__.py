"""
DynamoDB Tornado Client
=======================

"""
import logging

from tornado_aws import client
from tornado import concurrent

from tornado_dynamodb import utils

__version__ = '0.1.0'

LOGGER = logging.getLogger(__name__)


class DynamoDB(client.AsyncAWSClient):
    """

    Instead of having to pass in attributes such as the ``Item`` attribute in
    :py:func:`DynamoDB.put_item` with the nested ``{"key": {"TYPE": "value"}}``
    structure, such data structures will automatically be marshalled and
    unmarshalled when they are returned from DynamoDB.

    .. note:: Deprecated request attributes such as ``QueryFilter`` in
              :py:func:`botocore.client.DynamoDB.query` are not automatically
              marshalled, as they should not be used.


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


class RequestException(Exception):
    pass


class ResourceNotFoundException(Exception):
    pass
