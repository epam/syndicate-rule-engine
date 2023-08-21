# from azure.mgmt.resource import SubscriptionClient
# from azure.common.credentials import ServicePrincipalCredentials
# from msrest.exceptions import AuthenticationError
# from msrestazure.azure_exceptions import CloudError
#
# from helpers import CustodianException, RESPONSE_BAD_REQUEST_CODE, \
#     RESPONSE_INTERNAL_SERVER_ERROR
# from helpers.log_helper import get_logger
#
# _LOG = get_logger(__name__)
#
#
# class AzureSubscriptionsService:
#
#     @staticmethod
#     def validate_credentials(tenant_id, client_id, client_secret,
#                              subscription_id):
#         _LOG.debug(f'Validating azure credentials')
#         try:
#             credential = ServicePrincipalCredentials(tenant=tenant_id,
#                                                      client_id=client_id,
#                                                      secret=client_secret)
#             subscription_client = SubscriptionClient(credential)
#             subscription_client.subscriptions.get(
#                 subscription_id=subscription_id)
#         except AuthenticationError as e:
#             _LOG.error(f'Invalid credentials: {str(e)}')
#             raise CustodianException(
#                 code=RESPONSE_BAD_REQUEST_CODE,
#                 content=f'Invalid auth credentials provided'
#             )
#
#         except CloudError as e:
#             _LOG.debug(f'Error occurred on subscription id '
#                        f'\'{subscription_id}\' validation: {e.error.message}')
#             raise CustodianException(
#                 code=RESPONSE_BAD_REQUEST_CODE,
#                 content=f'Invalid subscription id provided: '
#                         f'\'{subscription_id}\''
#             )
#         except Exception as e:
#             _LOG.debug(f'Unexpected error occurred on azure credentials '
#                        f'validation: {str(e)}')
#             raise CustodianException(
#                 code=RESPONSE_INTERNAL_SERVER_ERROR,
#                 content=f'Unexpected error occurred on azure credentials '
#                         f'validation. Please check your credentials and'
#                         f'try again.'
#             )
