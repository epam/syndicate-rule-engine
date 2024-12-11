import pytest

try:
    import c7n
    import c7n_gcp
    import c7n_azure
except ImportError:
    pytest.skip('Some of Cloud Custodian modules is not installed',
                allow_module_level=True)

# some magic import that fixes c7n.resources.load_resources
from c7n.policy import Policy, PolicyCollection  # noqa
