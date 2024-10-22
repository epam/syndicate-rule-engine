import pytest

try:
    import c7n
except ImportError:
    pytest.skip('Cloud Custodian is not installed', allow_module_level=True)
