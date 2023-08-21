from datetime import datetime
from functools import wraps

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


def timeit(handler_func):
    @wraps(handler_func)
    def timed(*args, **kwargs):
        ts = datetime.now()
        result = handler_func(*args, **kwargs)
        te = datetime.now()
        _LOG.info(f'Stage {handler_func.__name__}, elapsed time: {te - ts}')
        return result
    return timed
