import os
import uuid

import mock

from tornado import concurrent
from tornado import httpclient
from tornado import testing
from tornado_aws import exceptions as aws_exceptions

import tornado_dynamodb
from tornado_dynamodb import exceptions


class AsyncTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.client = self.get_client()

    @property
    def endpoint(self):
        return os.getenv('DYNAMODB_ENDPOINT')

    def get_client(self):
        return tornado_dynamodb.DynamoDB(endpoint=self.endpoint)


class AWSClientTests(AsyncTestCase):

    @testing.gen_test
    def test_raises_config_not_found_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.ConfigNotFound(path='/test')
            with self.assertRaises(exceptions.ConfigNotFound):
                yield self.client.create_table(str(uuid.uuid4()), [], [])

    @testing.gen_test
    def test_raises_config_parser_error(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.ConfigParserError(path='/test')
            with self.assertRaises(exceptions.ConfigParserError):
                yield self.client.create_table(str(uuid.uuid4()), [], [])

    @testing.gen_test
    def test_raises_no_credentials_error(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.NoCredentialsError()
            with self.assertRaises(exceptions.NoCredentialsError):
                yield self.client.create_table(str(uuid.uuid4()), [], [])

    @testing.gen_test
    def test_raises_no_profile_error(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.NoProfileError(profile='test-1',
                                                              path='/test')
            with self.assertRaises(exceptions.NoProfileError):
                yield self.client.create_table(str(uuid.uuid4()), [], [])

    @testing.gen_test
    def test_raises_request_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = httpclient.HTTPError(500, 'uh-oh')
            with self.assertRaises(exceptions.RequestException):
                yield self.client.create_table(str(uuid.uuid4()), [], [])

    @testing.gen_test
    def test_raises_timeout_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = httpclient.HTTPError(599)
            with self.assertRaises(exceptions.TimeoutException):
                yield self.client.create_table(str(uuid.uuid4()), [], [])

    @testing.gen_test
    def test_fetch_future_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            future = concurrent.Future()
            fetch.return_value = future
            future.set_exception(exceptions.DynamoDBException())
            with self.assertRaises(exceptions.DynamoDBException):
                yield self.client.create_table(str(uuid.uuid4()), [], [])

    @testing.gen_test
    def test_empty_fetch_response_raises_dynamodb_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            future = concurrent.Future()
            fetch.return_value = future
            future.set_result(None)
            with self.assertRaises(exceptions.DynamoDBException):
                yield self.client.create_table(str(uuid.uuid4()), [], [])


class CreateTableTests(AsyncTestCase):

    @testing.gen_test
    def test_simple_table(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        response = yield self.client.create_table(table, attrs, schema)
        self.assertEqual(response['TableName'], table)
        self.assertIn(response['TableStatus'],
                      [tornado_dynamodb.TABLE_ACTIVE,
                       tornado_dynamodb.TABLE_CREATING])

    @testing.gen_test
    def test_complex_table(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'},
                 {'AttributeName': 'legacy', 'AttributeType': 'N'},
                 {'AttributeName': 'type', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        gsi = [{'IndexName': str(uuid.uuid4()),
                'KeySchema': [{'AttributeName': 'legacy', 'KeyType': 'HASH'},
                              {'AttributeName': 'type', 'KeyType': 'RANGE'}],
                'Projection': {
                     'ProjectionType': 'KEYS_ONLY'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }}]
        response = yield self.client.create_table(table, attrs, schema,
                                                  10, 10, gsi)
        self.assertEqual(response['TableName'], table)
        self.assertIn(response['TableStatus'],
                      [tornado_dynamodb.TABLE_ACTIVE,
                       tornado_dynamodb.TABLE_CREATING])

    @testing.gen_test
    def test_table_with_lsi(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'},
                 {'AttributeName': 'legacy', 'AttributeType': 'N'},
                 {'AttributeName': 'type', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'legacy', 'KeyType': 'HASH'},
                  {'AttributeName': 'type', 'KeyType': 'RANGE'}]
        lsi = [{'IndexName': str(uuid.uuid4()),
                'KeySchema': [{'AttributeName': 'legacy', 'KeyType': 'HASH'},
                              {'AttributeName': 'id', 'KeyType': 'RANGE'}],
                'Projection': {
                     'ProjectionType': 'KEYS_ONLY'
                }}]
        response = yield self.client.create_table(table, attrs, schema,
                                                  10, 10, None, lsi)
        self.assertEqual(response['TableName'], table)
        self.assertIn(response['TableStatus'],
                      [tornado_dynamodb.TABLE_ACTIVE,
                       tornado_dynamodb.TABLE_CREATING])

    @testing.gen_test
    def test_invalid_stream_type(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        with self.assertRaises(ValueError):
            yield self.client.create_table(table, attrs, schema,
                                           stream_enabled=True,
                                           stream_view_type='INVALID')

    @testing.gen_test
    def test_stream_enabled(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        response = yield self.client.create_table(
            table, attrs, schema, stream_enabled=True,
            stream_view_type=tornado_dynamodb.STREAM_VIEW_NEW_AND_OLD_IMAGES)
        self.assertEqual(response['TableName'], table)
        self.assertEqual(response['StreamSpecification']['StreamViewType'],
                         tornado_dynamodb.STREAM_VIEW_NEW_AND_OLD_IMAGES)
        self.assertTrue(response['StreamSpecification']['StreamEnabled'])

    @testing.gen_test
    def test_invalid_request(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id'}]
        with self.assertRaises(exceptions.ValidationException):
            yield self.client.create_table(table, attrs, [])


class DeleteTableTests(AsyncTestCase):

    @testing.gen_test
    def test_delete_table(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        response = yield self.client.create_table(table, attrs, schema)
        self.assertEqual(response['TableName'], table)
        yield self.client.delete_table(table)
        with self.assertRaises(exceptions.ResourceNotFound):
            yield self.client.describe_table(table)

    @testing.gen_test
    def test_table_not_found(self):
        table = str(uuid.uuid4())
        with self.assertRaises(exceptions.ResourceNotFound):
            yield self.client.delete_table(table)


class DescribeTableTests(AsyncTestCase):

    @testing.gen_test
    def test_describe_table(self):
        # Create the table first
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        response = yield self.client.create_table(table, attrs, schema)
        self.assertTrue(response)

        # Describe the table
        response = yield self.client.describe_table(table)
        self.assertEqual(response['TableName'], table)
        self.assertEqual(response['TableStatus'],
                         tornado_dynamodb.TABLE_ACTIVE)

    @testing.gen_test
    def test_table_not_found(self):
        table = str(uuid.uuid4())
        with self.assertRaises(exceptions.ResourceNotFound):
            yield self.client.describe_table(table)
