"""
Each folder in this directory should contain one function "register"
"""
import importlib
import pkgutil

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


def register_all() -> None:
    from c7n.resources import load_available
    _LOG.info('Going to load all available resources')
    load_available(True)

    parent = importlib.import_module(__name__)
    for info in pkgutil.iter_modules(parent.__path__):
        mod = importlib.import_module(f'{parent.__name__}.{info.name}')
        assert hasattr(mod, 'register') and callable(mod.register), (
            f'Module {mod.__name__} does not have a register function'
        )
        _LOG.info(f'Going to register plugin {mod.__name__}')
        mod.register()

    load_available(True)
