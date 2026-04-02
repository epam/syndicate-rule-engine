"""
Executor specific constants
"""

from helpers.constants import Cloud


ENV_AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION"

ENV_GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"
ENV_CLOUDSDK_CORE_PROJECT = "CLOUDSDK_CORE_PROJECT"

AWS_DEFAULT_REGION = "us-east-1"


INVALID_CREDENTIALS_ERROR_CODES = {
    Cloud.AWS: {
        "AuthFailure",
        "InvalidToken",
        "UnrecognizedClientException",
        "ExpiredToken",
        "ExpiredTokenException",
    },  # add 'InvalidClientTokenId'
    Cloud.AZURE: {
        "InvalidAuthenticationTokenTenant",
        "AuthorizationFailed",
        "ClientAuthenticationError",
        "Azure Error: AuthorizationFailed",
    },
    Cloud.GOOGLE: set(),
}
ACCESS_DENIED_ERROR_CODE = {
    Cloud.AWS: {
        "AccessDenied",
        "AccessDeniedException",
        "UnauthorizedOperation",
        "AuthorizationError",
        "AccessDeniedException, AccessDeniedException",
    },
    Cloud.AZURE: set(),
    Cloud.GOOGLE: set(),
}

CACHE_FILE = "cloud-custodian.cache"
