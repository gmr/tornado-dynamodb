"""
DynamoDB Exceptions
===================

"""


class DynamoDBException(Exception):
    """Base exception that is extended by all exceptions raised by
    tornado_dynamodb.

    """
    pass


class ConfigNotFound(DynamoDBException):
    """The configuration file could not be parsed.

    :ivar str path: The path to the config file

    """
    fmt = 'The config file could not be found ({path})'


class ConfigParserError(DynamoDBException):
    """Error raised when parsing a configuration file with
    :class:`~configparser.RawConfigParser`

    :ivar str path: The path to the config file

    """
    fmt = 'Unable to parse config file ({path})'


class InternalFailure(DynamoDBException):
    """The request processing has failed because of an unknown error, exception
    or failure.

    """
    pass


class InvalidAction(DynamoDBException):
    """The action or operation requested is invalid. Verify that the action is
    typed correctly.

    """
    pass


class InvalidParameterCombination(DynamoDBException):
    """Parameters that must not be used together were used together."""
    pass


class InvalidParameterValue(DynamoDBException):
    """An invalid or out-of-range value was supplied for the input parameter."""
    pass


class InvalidQueryParameter(DynamoDBException):
    """The AWS query string is malformed or does not adhere to AWS standards."""
    pass


class LimitExceeded(DynamoDBException):
    """The number of concurrent table requests (cumulative number of tables in
    the ``CREATING``, ``DELETING`` or ``UPDATING`` state) exceeds the maximum
    allowed of ``10``.

    Also, for tables with secondary indexes, only one of those tables can be in
    the ``CREATING`` state at any point in time. Do not attempt to create more
    than one such table simultaneously.

    The total limit of tables in the ``ACTIVE`` state is ``250``.

    """
    pass


class MalformedQueryString(DynamoDBException):
    """The query string contains a syntax error."""
    pass


class MissingParameter(DynamoDBException):
    """A required parameter for the specified action is not supplied."""
    pass


class NoCredentialsError(DynamoDBException):
    """Raised when the credentials could not be located."""
    fmt = 'Credentials not found'


class NoProfileError(DynamoDBException):
    """Raised when the specified profile could not be located.

    :ivar str path: The path to the config file
    :ivar str profile: The profile that was specified

    """
    fmt = 'Profile ({profile}) not found ({path})'


class OptInRequired(DynamoDBException):
    """The AWS access key ID needs a subscription for the service."""
    pass


class RequestExpired(DynamoDBException):
    """The request reached the service more than 15 minutes after the date
    stamp on the request or more than 15 minutes after the request expiration
    date (such as for pre-signed URLs), or the date stamp on the request is
    more than 15 minutes in the future.

    """
    pass


class ResourceInUse(DynamoDBException):
    """he operation conflicts with the resource's availability. For example,
    you attempted to recreate an existing table, or tried to delete a table
    currently in the ``CREATING`` state.

    """
    pass


class ResourceNotFound(DynamoDBException):
    """The operation tried to access a nonexistent table or index. The resource
    might not be specified correctly, or its status might not be ``ACTIVE``.

    """
    pass


class ServiceUnavailable(DynamoDBException):
    """The request has failed due to a temporary failure of the server."""
    pass


class ThrottlingException(DynamoDBException):
    """The request was denied due to request throttling."""
    pass


class ValidationException(DynamoDBException):
    """The input fails to satisfy the constraints specified by an AWS service.

    """
    pass


MAP = {
    'com.amazonaws.dynamodb.v20120810#InternalFailure': InternalFailure,
    'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException':
        ResourceNotFound,
    'com.amazonaws.dynamodb.v20120810#ResourceInUseException': ResourceInUse,
    'com.amazon.coral.validate#ValidationException': ValidationException
}
