"""
For saas just import this variable.
For on-prem it's important that all the necessary envs (at least MongoDB's)
are set before importing from here. Otherwise, it could lead to timeout or
an undesirable request to AWS.
"""
from services import SERVICE_PROVIDER

# One and sole and onliest SYSTEM customer variable. Don't you dare use
# somewhere string: 'SYSTEM' or define one more such a variable :)
SYSTEM_CUSTOMER = SERVICE_PROVIDER.settings_service(). \
    get_system_customer_name()
print(f'SYSTEM Customer name: \'{SYSTEM_CUSTOMER}\'')
