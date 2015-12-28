"""
DynamoDB Exceptions
===================

"""
from tornado_aws.exceptions import ConfigNotFound
from tornado_aws.exceptions import ConfigParserError
from tornado_aws.exceptions import NoCredentialsError
from tornado_aws.exceptions import NoProfileError


class DynamoDBException(Exception):
    pass


class InternalFailure(DynamoDBException):
    pass


class ResourceInUse(DynamoDBException):
    pass


class ResourceNotFound(DynamoDBException):
    pass


class ThrottlingException(DynamoDBException):
    pass


class ValidationException(DynamoDBException):
    pass


MAP = {
    'com.amazonaws.dynamodb.v20120810#InternalFailure': InternalFailure,
    'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException':
        ResourceNotFound,
    'com.amazonaws.dynamodb.v20120810#ResourceInUseException': ResourceInUse,
    'com.amazon.coral.validate#ValidationException': ValidationException
}
