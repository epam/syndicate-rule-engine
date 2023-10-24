from functools import wraps
from http import HTTPStatus
from typing import Callable, Dict, Any, TypeVar

from pydantic import BaseModel, ValidationError

from helpers import build_response

T = TypeVar('T')


def validate_pydantic(model: type, value: dict) -> BaseModel:
    try:
        return model(**value)
    except ValidationError as e:
        # return build_response(
        #     code=HTTPStatus.BAD_REQUEST,
        #     content='Validation error',
        #     meta={
        #         PARAM_ERRORS: e.errors()
        #     }
        # )
        errors = {}
        for error in e.errors():
            loc = error.get("loc")[-1]
            msg = error.get("msg")
            errors.update({loc: msg})
        errors = '; '.join(f"{k} - {v}" for k, v in errors.items())
        return build_response(
            code=HTTPStatus.BAD_REQUEST,
            content=f'The following parameters do not match '
                    f'the schema requirements: {errors}'
        )


def validate_type(_type: type(T), value: Any) -> T:
    try:
        return _type(value) if not isinstance(value, _type) else value
    except (ValueError, TypeError) as e:
        return build_response(
            code=HTTPStatus.BAD_REQUEST,
            content=f'Validation error: \'{value}\' cannot be casted to '
                    f'{_type.__name__}'
        )


def _validate(kwargs: Dict[str, Any], types: Dict[str, type],
              cast: bool = True) -> Dict[str, Any]:
    """
    Received keys and values in `kwargs`, keys and expected values' types
    in `types`. Returns a dict with keys and validated values
    :param kwargs:
    :param types:
    :param cast:
    :return:
    """
    validated = {}
    for key, value in kwargs.items():
        if key not in types:
            validated[key] = value
            continue
        _type = types[key]
        if issubclass(_type, BaseModel):
            valid = validate_pydantic(_type, value)
            if not cast:
                valid = valid.dict()
            validated[key] = valid
        else:
            # supposedly here will be only dynamic url params. Their
            # rightness depends not on the user but on the developer. Value
            # shall be always cast-able
            valid = validate_type(_type, value)
            if not cast:
                valid = value
            validated[key] = valid
    return validated


def validate_kwargs(func: Callable) -> Callable:
    """
    Simply tries to cast async function's arguments to their annotated params.
    An argument must be a dict in case you annotate it with pydantic model
    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        validated = _validate(kwargs, func.__annotations__)
        result = func(*args, **validated)
        # _return = func.__annotations__.get('return')
        # if issubclass(_return, BaseModel):
        #     result['body'] = _return.parse_raw(result['body']).json()
        #     # in case validation fails here, it's developer's error
        return result

    return wrapper


def validate_kwargs_by(**types) -> Callable:
    """
    The same as a decorator above but instead of func.__annotations__ it
    uses given by the developer keyword arguments `types`. Also, it
    does not cast attributes to the required values `cause it's not
    annotations. It would look odd. If you still want this method to cast
    attributes just set _cast=True
    :param types:
    :return:
    """
    _cast = False
    if types.get('_cast') is True:
        _cast = types.get('_cast')

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            validated = _validate(kwargs, types, cast=_cast)
            return func(*args, **validated)

        return wrapper

    return decorator
