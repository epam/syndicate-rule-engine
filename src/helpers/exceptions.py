from functools import wraps
from typing import Callable, TypeVar, ParamSpec

from helpers.lambda_response import LambdaForceExit
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

T = TypeVar("T")
P = ParamSpec("P")


class CloudNotSupportedError(KeyError):
    pass


def safe_call(func: Callable[P, T]) -> Callable[P, T | None]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | None:
        try:
            return func(*args, **kwargs)
        except LambdaForceExit as e:
            response = e.response
            _LOG.info(
                "LambdaForceExit exception occurred with status "
                f"code {response.code} and content {response.content}"
            )
        except Exception as e:
            _LOG.exception(f"Unexpected error occurred: {e}")

        return None

    return wrapper
