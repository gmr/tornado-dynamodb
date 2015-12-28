import os
import uuid

from tornado import testing

import tornado_dynamodb
from tornado_dynamodb import exceptions


class AsyncTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.client = \
            tornado_dynamodb.DynamoDB(endpoint=os.getenv('DYNAMODB_ENDPOINT'))


class CreateTableTests(AsyncTestCase):

    @testing.gen_test
    def test_create_table(self):
        table = str(uuid.uuid4())
        attrs = [{'AttributeName': 'id', 'AttributeType': 'S'}]
        schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        response = yield self.client.create_table(table, attrs, schema)
        self.assertTrue(response)
        response = yield self.client.delete_table(table)
        self.assertEqual(response['TableDescription']['TableName'], table)


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
        self.assertEqual(response['TableStatus'],
                         tornado_dynamodb.TABLE_ACTIVE)

        # Remove the table
        response = yield self.client.delete_table(table)
        self.assertEqual(response['TableDescription']['TableName'], table)

    @testing.gen_test
    def test_table_not_found(self):
        table = str(uuid.uuid4())
        with self.assertRaises(exceptions.ResourceNotFound):
            yield self.client.describe_table(table)
