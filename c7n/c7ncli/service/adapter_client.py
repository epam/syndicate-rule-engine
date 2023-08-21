import json
import os
from typing import Optional, Dict, List

import requests

from c7ncli.service.config import AbstractCustodianConfig
from c7ncli.service.constants import *
from c7ncli.service.logger import get_logger, get_user_logger
from c7ncli.version import check_version_compatibility

HTTP_GET = 'get'
HTTP_POST = 'post'
HTTP_PATCH = 'patch'
HTTP_DELETE = 'delete'

ALLOWED_METHODS = {HTTP_GET, HTTP_POST, HTTP_PATCH, HTTP_DELETE}

SYSTEM_LOG = get_logger(__name__)
USER_LOG = get_user_logger(__name__)


class AdapterClient:  # why not ApiClient?
    def __init__(self, config: Optional[AbstractCustodianConfig] = None):
        self._config = config
        SYSTEM_LOG.info('API Client object has has been created')

    @property
    def config(self) -> Optional[AbstractCustodianConfig]:
        return self._config

    @config.setter
    def config(self, value: AbstractCustodianConfig):
        assert isinstance(value, AbstractCustodianConfig)
        self._config = value

    def __make_request(self, resource: str, method: str, payload: dict = None):
        assert method in ALLOWED_METHODS, 'Not supported method'
        parameters = dict(
            method=method.upper(),
            url=f'{self.config.api_link}/{resource}',
            headers={'authorization': self.config.access_token}
        )
        # config.api_link existence is validated in cli_response decorator
        if method == HTTP_GET:
            parameters.update(params=payload)
        else:
            parameters.update(json=payload)
        SYSTEM_LOG.debug(f'API request info: {parameters}; Method: '
                         f'{method.upper()}')
        try:
            response = requests.request(**parameters)
        except requests.exceptions.ConnectionError:
            response = {
                'message': 'Provided Custodian api_link is invalid '
                           'or outdated. Please contact the tool support team.'
            }
            SYSTEM_LOG.exception(response)
            return response
        SYSTEM_LOG.debug(f'API response info: status {response.status_code}; '
                         f'text {response.text}')
        return response

    @staticmethod
    def sifted(request: dict) -> dict:
        return {k: v for k, v in request.items() if isinstance(
            v, (bool, int)) or v}

    # application entity management
    def application_post(self, **kwargs):
        return self.__make_request(
            resource=API_APPLICATION,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def application_patch(self, application_id: str, **kwargs):
        return self.__make_request(
            resource=API_APPLICATION + f'/{application_id}',
            method=HTTP_PATCH,
            payload=self.sifted(kwargs)
        )

    def application_list(self, **kwargs):
        return self.__make_request(
            resource=API_APPLICATION,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def application_get(self, application_id: str):
        return self.__make_request(
            resource=API_APPLICATION + f'/{application_id}',
            method=HTTP_GET,
            payload={}
        )

    def application_delete(self, application_id: str,
                           customer_id: Optional[str]):
        return self.__make_request(
            resource=API_APPLICATION + f'/{application_id}',
            method=HTTP_DELETE,
            payload={PARAM_CUSTOMER: customer_id}
        )

    def access_application_post(self, **kwargs):
        return self.__make_request(
            resource=API_ACCESS_APPLICATION,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def access_application_patch(self, application_id: str, **kwargs):
        return self.__make_request(
            resource=API_ACCESS_APPLICATION + f'/{application_id}',
            method=HTTP_PATCH,
            payload=self.sifted(kwargs)
        )

    def access_application_list(self, **kwargs):
        return self.__make_request(
            resource=API_ACCESS_APPLICATION,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def access_application_get(self, application_id: str):
        return self.__make_request(
            resource=API_ACCESS_APPLICATION + f'/{application_id}',
            method=HTTP_GET,
            payload={}
        )

    def access_application_delete(self, application_id: str,
                                  customer_id: Optional[str]):
        return self.__make_request(
            resource=API_ACCESS_APPLICATION + f'/{application_id}',
            method=HTTP_DELETE,
            payload={PARAM_CUSTOMER: customer_id}
        )

    def dojo_application_post(self, **kwargs):
        return self.__make_request(
            resource=API_DOJO_APPLICATION,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def dojo_application_patch(self, application_id: str, **kwargs):
        return self.__make_request(
            resource=API_DOJO_APPLICATION + f'/{application_id}',
            method=HTTP_PATCH,
            payload=self.sifted(kwargs)
        )

    def dojo_application_list(self, **kwargs):
        return self.__make_request(
            resource=API_DOJO_APPLICATION,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def dojo_application_get(self, application_id: str):
        return self.__make_request(
            resource=API_DOJO_APPLICATION + f'/{application_id}',
            method=HTTP_GET,
            payload={}
        )

    def dojo_application_delete(self, application_id: str,
                                customer_id: Optional[str]):
        return self.__make_request(
            resource=API_DOJO_APPLICATION + f'/{application_id}',
            method=HTTP_DELETE,
            payload={PARAM_CUSTOMER: customer_id}
        )

    # parent entity management
    def parent_post(self, **kwargs):
        return self.__make_request(
            resource=API_PARENT,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def parent_patch(self, parent_id, **kwargs):
        return self.__make_request(
            resource=API_PARENT + f'/{parent_id}',
            method=HTTP_PATCH,
            payload=self.sifted(kwargs)
        )

    def parent_get(self, parent_id: str):
        return self.__make_request(
            resource=API_PARENT + f'/{parent_id}',
            method=HTTP_GET, payload={}
        )

    def parent_delete(self, parent_id: str):
        return self.__make_request(
            resource=API_PARENT + f'/{parent_id}',
            method=HTTP_DELETE, payload={}
        )

    def parent_list(self, **kwargs):
        return self.__make_request(
            resource=API_PARENT,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def parent_link_tenant(self, **kwargs):
        return self.__make_request(
            resource=API_PARENT_TENANT_LINK,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def parent_unlink_tenant(self, **kwargs):
        return self.__make_request(
            resource=API_PARENT_TENANT_LINK,
            method=HTTP_DELETE,
            payload=self.sifted(kwargs)
        )

    # Customer entity management
    def customer_get(self, name: str, complete: bool):
        query = self.sifted({PARAM_NAME: name, PARAM_COMPLETE: complete})
        return self.__make_request(
            resource=API_CUSTOMER, method=HTTP_GET, payload=query
        )

    # Tenant entity management
    def tenant_get(self, tenant_name: str, customer_name: str,
                   cloud_identifier: str, complete: bool,
                   limit: int, next_token: str):
        return self.__make_request(
            resource=API_TENANT, method=HTTP_GET, payload=self.sifted({
                PARAM_TENANT_NAME: tenant_name,
                PARAM_CUSTOMER: customer_name,
                PARAM_CLOUD_IDENTIFIER: cloud_identifier,
                PARAM_COMPLETE: complete,
                PARAM_LIMIT: limit,
                PARAM_NEXT_TOKEN: next_token
            })
        )

    def tenant_post(self, **kwargs):
        return self.__make_request(
            resource=API_TENANT,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def tenant_patch(self, **kwargs):
        return self.__make_request(
            resource=API_TENANT,
            method=HTTP_PATCH,
            payload=self.sifted(kwargs)
        )

    def tenant_region_post(self, **kwargs):
        return self.__make_request(
            resource=API_TENANT_REGIONS,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    # Tenant License Priority management

    def tenant_license_priority_get(
            self, tenant_name: str, customer_name: str,
            governance_entity_type: str, governance_entity_id: str,
            management_id: str
    ):
        query = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_CUSTOMER: customer_name,
            PARAM_GOVERNANCE_ENTITY_TYPE: governance_entity_type,
            PARAM_GOVERNANCE_ENTITY_ID: governance_entity_id,
            PARAM_MANAGEMENT_ID: management_id
        }
        return self.__make_request(
            resource=API_TENANT_LICENSE_PRIORITIES, method=HTTP_GET,
            payload=self.sifted(query)
        )

    def tenant_license_priority_post(
            self, tenant_name: str, license_key_list: list,
            governance_entity_type: str, governance_entity_id: str,
            management_id: str = None
    ):
        payload = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_GOVERNANCE_ENTITY_TYPE: governance_entity_type,
            PARAM_GOVERNANCE_ENTITY_ID: governance_entity_id,
            PARAM_LICENSE_KEYS: license_key_list,
            PARAM_MANAGEMENT_ID: management_id
        }
        return self.__make_request(
            resource=API_TENANT_LICENSE_PRIORITIES, method=HTTP_POST,
            payload=self.sifted(payload)
        )

    def tenant_license_priority_patch(
            self, tenant_name: str, license_keys_to_prepend: list,
            license_keys_to_append: list, license_keys_to_detach: list,
            governance_entity_type: str, governance_entity_id: str,
            management_id: str
    ):
        payload = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_GOVERNANCE_ENTITY_TYPE: governance_entity_type,
            PARAM_GOVERNANCE_ENTITY_ID: governance_entity_id,
            PARAM_LICENSE_KEYS_TO_PREPEND: license_keys_to_prepend,
            PARAM_LICENSE_KEYS_TO_APPEND: license_keys_to_append,
            PARAM_LICENSE_KEYS_TO_DETACH: license_keys_to_detach,
            PARAM_MANAGEMENT_ID: management_id
        }
        return self.__make_request(
            resource=API_TENANT_LICENSE_PRIORITIES, method=HTTP_PATCH,
            payload=self.sifted(payload)
        )

    def tenant_license_priority_delete(
            self, tenant_name: str, governance_entity_type: str,
            governance_entity_id: str, management_id: str
    ):
        payload = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_GOVERNANCE_ENTITY_TYPE: governance_entity_type,
            PARAM_GOVERNANCE_ENTITY_ID: governance_entity_id,
            PARAM_MANAGEMENT_ID: management_id
        }
        return self.__make_request(
            resource=API_TENANT_LICENSE_PRIORITIES, method=HTTP_DELETE,
            payload=self.sifted(payload)
        )

    def credentials_manager_get(self, cloud, cloud_identifier):
        request = {
            PARAM_CLOUD: cloud,
            PARAM_CLOUD_IDENTIFIER: cloud_identifier
        }

        return self.__make_request(resource=API_CREDENTIALS_MANAGER,
                                   method=HTTP_GET,
                                   payload=request)

    def credentials_manager_post(self,
                                 cloud: str,
                                 cloud_identifier: str,
                                 trusted_role_arn: str,
                                 enabled: bool):
        request = {
            PARAM_CLOUD: cloud,
            PARAM_CLOUD_IDENTIFIER: cloud_identifier,
            PARAM_TRUSTED_ROLE_ARN: trusted_role_arn,
            PARAM_ENABLED: enabled,
        }

        return self.__make_request(resource=API_CREDENTIALS_MANAGER,
                                   method=HTTP_POST,
                                   payload=request)

    def credentials_manager_patch(self,
                                  cloud: str,
                                  cloud_identifier: str,
                                  trusted_role_arn: str,
                                  enabled: bool):
        request = {
            PARAM_CLOUD: cloud,
            PARAM_CLOUD_IDENTIFIER: cloud_identifier,
            PARAM_TRUSTED_ROLE_ARN: trusted_role_arn,
            PARAM_ENABLED: enabled,
        }
        request = {k: v for k, v in request.items() if v is not None}

        return self.__make_request(resource=API_CREDENTIALS_MANAGER,
                                   method=HTTP_PATCH,
                                   payload=request)

    def credentials_manager_delete(self, cloud, cloud_identifier):
        request = {
            PARAM_CLOUD: cloud,
            PARAM_CLOUD_IDENTIFIER: cloud_identifier
        }

        return self.__make_request(resource=API_CREDENTIALS_MANAGER,
                                   method=HTTP_DELETE,
                                   payload=request)

    # ruleset entity management
    def ruleset_get(self, **kwargs):
        return self.__make_request(
            resource=API_RULESET,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def ruleset_post(self, **kwargs):
        return self.__make_request(resource=API_RULESET,
                                   method=HTTP_POST,
                                   payload=self.sifted(kwargs))

    def ruleset_update(self, customer: str, ruleset_name: str,
                       version: float, rules_to_attach: list,
                       rules_to_detach: list, active: bool,
                       tenant_allowance: list,
                       tenant_restriction: list):
        request = {
            PARAM_CUSTOMER: customer,
            PARAM_NAME: ruleset_name,
            PARAM_VERSION: version,
            RULES_TO_ATTACH: rules_to_attach,
            RULES_TO_DETACH: rules_to_detach,
            PARAM_TENANT_ALLOWANCE: tenant_allowance,
            PARAM_TENANT_RESTRICTION: tenant_restriction
        }
        if isinstance(active, bool):
            request[PARAM_ACTIVE] = active
        return self.__make_request(resource=API_RULESET,
                                   method=HTTP_PATCH,
                                   payload=request)

    def ruleset_delete(self, customer: str, ruleset_name: str,
                       version: float):
        request = {
            PARAM_CUSTOMER: customer,
            PARAM_NAME: ruleset_name,
            PARAM_VERSION: version,
        }
        return self.__make_request(resource=API_RULESET,
                                   method=HTTP_DELETE,
                                   payload=request)

    def ed_ruleset_add(self, **kwargs):
        return self.__make_request(
            resource=API_ED_RULESET,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def ed_ruleset_get(self, cloud: Optional[str], get_rules: Optional[bool]):
        return self.__make_request(
            resource=API_ED_RULESET,
            method=HTTP_GET,
            payload=self.sifted({
                'cloud': cloud,
                'get_rules': get_rules
            })
        )

    def ed_ruleset_delete(self, **kwargs):
        return self.__make_request(
            resource=API_ED_RULESET,
            method=HTTP_DELETE,
            payload=self.sifted(kwargs)
        )

    # Rule Source management
    def rule_source_get(self, rule_source_id: str, customer: str,
                        git_project_id: str):
        if rule_source_id:
            query = {PARAM_ID: rule_source_id}
        else:
            query = {
                PARAM_CUSTOMER: customer,
                PARAM_GIT_PROJECT_ID: git_project_id
            }
            query = self.sifted(request=query)
        return self.__make_request(resource=API_RULE_SOURCE,
                                   method=HTTP_GET,
                                   payload=query)

    def rule_source_post(self, **kwargs):
        return self.__make_request(
            resource=API_RULE_SOURCE, method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def rule_source_patch(self, **kwargs):
        return self.__make_request(resource=API_RULE_SOURCE, method=HTTP_PATCH,
                                   payload=self.sifted(kwargs))

    def rule_source_delete(self, rule_source_id: str, customer: str):
        request = {
            PARAM_ID: rule_source_id,
            PARAM_CUSTOMER: customer
        }
        return self.__make_request(
            resource=API_RULE_SOURCE, method=HTTP_DELETE,
            payload=self.sifted(request))

    # Role entity management

    def role_get(self, **kwargs):
        return self.__make_request(
            resource=API_ROLE, method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def role_post(self, **kwargs):
        return self.__make_request(
            resource=API_ROLE,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def role_patch(self, **kwargs):
        return self.__make_request(
            resource=API_ROLE,
            method=HTTP_PATCH,
            payload=self.sifted(kwargs)
        )

    def role_delete(self, **kwargs):
        return self.__make_request(
            resource=API_ROLE,
            method=HTTP_DELETE,
            payload=self.sifted(kwargs)
        )

    def role_clean_cache(self, **kwargs):
        return self.__make_request(
            resource=API_ROLE_CACHE,
            method=HTTP_DELETE,
            payload=self.sifted(kwargs)
        )

    # Policy entity management

    def policy_get(self, customer_display_name, policy_name):
        request = {PARAM_CUSTOMER: customer_display_name}
        if policy_name:
            request[PARAM_NAME] = policy_name
        return self.__make_request(resource=API_POLICY, method=HTTP_GET,
                                   payload=request)

    def policy_post(self, **kwargs):
        return self.__make_request(
            resource=API_POLICY, method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    @staticmethod
    def __get_permissions_from_file(path_to_permissions):
        try:
            with open(path_to_permissions, 'r') as file:
                content = json.loads(file.read())
        except json.decoder.JSONDecodeError:
            return {'message': 'Invalid file content'}
        if not isinstance(content, list):
            return {'message': 'Invalid file content'}
        return content

    def policy_patch(self, **kwargs):
        return self.__make_request(
            resource=API_POLICY,
            method=HTTP_PATCH,
            payload=self.sifted(kwargs)
        )

    def policy_delete(self, customer_display_name, policy_name):
        request = {PARAM_CUSTOMER: customer_display_name,
                   PARAM_NAME: policy_name}
        return self.__make_request(resource=API_POLICY, method=HTTP_DELETE,
                                   payload=request)

    def policy_clean_cache(self, **kwargs):
        return self.__make_request(
            resource=API_POLICY_CACHE,
            method=HTTP_DELETE,
            payload=self.sifted(kwargs)
        )

    # Rule entity management

    def rule_get(self, **kwargs):
        return self.__make_request(resource=API_RULE,
                                   method=HTTP_GET,
                                   payload=self.sifted(kwargs))

    def rule_delete(self, **kwargs):
        return self.__make_request(
            resource=API_RULE,
            method=HTTP_DELETE,
            payload=self.sifted(kwargs)
        )

    # Account Region entity management
    def region_get(self, display_name: str, tenant_name: str,
                   region_name: str):
        request = {
            PARAM_DISPLAY_NAME: display_name,
            PARAM_TENANT_NAME: tenant_name,
            PARAM_NAME: region_name,
        }
        return self.__make_request(resource=API_ACCOUNT_REGION,
                                   method=HTTP_GET,
                                   payload=self.sifted(request))

    def region_post(self, display_name: str, tenant_name: str,
                    region_name: str, state: str,
                    all_regions: bool):
        request = {
            PARAM_DISPLAY_NAME: display_name,
            PARAM_TENANT_NAME: tenant_name,
            PARAM_NAME: region_name,
            PARAM_REGION_STATE: state,
            PARAM_ALL_REGIONS: all_regions
        }
        return self.__make_request(resource=API_ACCOUNT_REGION,
                                   method=HTTP_POST,
                                   payload=request)

    def region_patch(self, display_name: str, tenant_name: str,
                     region_name: str, state: str):
        request = {
            PARAM_DISPLAY_NAME: display_name,
            PARAM_TENANT_NAME: tenant_name,
            PARAM_NAME: region_name,
            PARAM_REGION_STATE: state
        }
        return self.__make_request(resource=API_ACCOUNT_REGION,
                                   method=HTTP_PATCH,
                                   payload=self.sifted(request))

    def region_delete(self, display_name: str, tenant_name: str,
                      regions_names: list):
        request = {
            PARAM_DISPLAY_NAME: display_name,
            PARAM_TENANT_NAME: tenant_name,
            PARAM_NAME: regions_names
        }
        return self.__make_request(resource=API_ACCOUNT_REGION,
                                   method=HTTP_DELETE,
                                   payload=self.sifted(request))

    def trigger_backup(self):
        return self.__make_request(resource=API_BACKUPPER, method=HTTP_POST,
                                   payload={})

    def metrics_status(self):
        return self.__make_request(resource=API_METRICS_STATUS,
                                   method=HTTP_GET,
                                   payload={})

    def trigger_metrics_update(self):
        return self.__make_request(resource=API_METRICS_UPDATER,
                                   method=HTTP_POST, payload={})

    def trigger_rule_meta_updater(self, **kwargs):
        return self.__make_request(
            resource=API_RULE_META_UPDATER, method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def job_list(self, **kwargs):
        return self.__make_request(
            resource=API_JOB,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def job_get(self, job_id: str):
        return self.__make_request(
            resource=API_JOB + f'/{job_id}',
            method=HTTP_GET, payload={}
        )

    def job_post(self, **kwargs):
        api = API_JOB
        if os.environ.get('C7N_STANDARD_JOBS'):
            kwargs.pop('check_permission', None)
            api += '/standard'
        return self.__make_request(
            resource=api,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def job_delete(self, job_id: str):
        return self.__make_request(
            resource=API_JOB + f'/{job_id}',
            method=HTTP_DELETE,
            payload={}
        )

    def scheduled_job_post(self, schedule: str,
                           target_ruleset: list, target_region: str,
                           tenant: str = None, name: str = None,
                           customer: str = None):
        return self.__make_request(
            resource=API_SCHEDULED_JOBS,
            method=HTTP_POST,
            payload=self.sifted({
                PARAM_TENANT_NAME: tenant,
                PARAM_TARGET_RULESETS: target_ruleset,
                PARAM_TARGET_REGIONS: target_region,
                PARAM_SCHEDULE_EXPRESSION: schedule,
                PARAM_NAME: name,
                PARAM_CUSTOMER: customer
            })
        )

    def scheduled_job_get(self, name: str = None):
        return self.__make_request(
            resource=API_SCHEDULED_JOBS + f'/{name}',
            method=HTTP_GET,
            payload={}
        )

    def scheduled_job_query(self, **kwargs):
        return self.__make_request(
            resource=API_SCHEDULED_JOBS,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def scheduled_job_delete(self, name: str):
        return self.__make_request(
            resource=API_SCHEDULED_JOBS + f'/{name}',
            method=HTTP_DELETE,
            payload={}
        )

    def scheduled_job_update(self, name: str, schedule: str = None,
                             enabled: bool = None, customer: str = None):
        return self.__make_request(
            resource=API_SCHEDULED_JOBS + f'/{name}',
            method=HTTP_PATCH,
            payload=self.sifted({
                PARAM_SCHEDULE_EXPRESSION: schedule,
                PARAM_ENABLED: enabled,
                PARAM_CUSTOMER: customer
            })
        )

    def report_get(self, job_id: str, account_display_name: str,
                   tenant_name: str,
                   detailed: bool, get_url: bool):
        request = {
            PARAM_JOB_ID: job_id,
            PARAM_ACCOUNT: account_display_name,
            PARAM_TENANT_NAME: tenant_name,
            PARAM_DETAILED: detailed,
            PARAM_GET_URL: get_url
        }
        return self.__make_request(
            resource=API_REPORT,
            method=HTTP_GET,
            payload=self.sifted(request)
        )

    def operational_report_get(self, tenant_name: str, report_type: str,
                               customer: str):
        request = {
            PARAM_TENANT_NAMES: tenant_name,
            PARAM_TYPE: report_type,
            PARAM_CUSTOMER: customer
        }
        return self.__make_request(
            resource=API_OPERATIONAL_REPORT,
            method=HTTP_GET,
            payload=self.sifted(request)
        )

    def project_report_get(self, tenant_display_name: str, report_type: str,
                           customer: str):
        request = {
            PARAM_TENANT_DISPLAY_NAMES: tenant_display_name,
            PARAM_TYPE: report_type,
            PARAM_CUSTOMER: customer
        }
        return self.__make_request(
            resource=API_PROJECT_REPORT,
            method=HTTP_GET,
            payload=self.sifted(request)
        )

    def department_report_get(self, report_type: str, customer: str):
        request = {
            PARAM_TYPE: report_type,
            PARAM_CUSTOMER: customer
        }
        return self.__make_request(
            resource=API_DEPARTMENT_REPORT,
            method=HTTP_GET,
            payload=self.sifted(request)
        )

    def c_level_report_get(self, report_type: str, customer: str):
        request = {
            PARAM_TYPE: report_type,
            PARAM_CUSTOMER: customer
        }
        return self.__make_request(
            resource=API_C_LEVEL_REPORT,
            method=HTTP_GET,
            payload=self.sifted(request)
        )

    def push_dojo_by_job_id(self, job_id: str):
        return self.__make_request(
            resource=API_REPORTS_PUSH_DOJO + '/' + job_id,
            method=HTTP_POST,
            payload={}
        )

    def push_security_hub_by_job_id(self, job_id: str):
        return self.__make_request(
            resource=API_REPORTS_PUSH_SECURITY_HUB + '/' + job_id,
            method=HTTP_POST,
            payload={}
        )

    def push_dojo_multiple(self, **kwargs):
        return self.__make_request(
            resource=API_REPORTS_PUSH_DOJO,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def push_security_hub_multiple(self, **kwargs):
        return self.__make_request(
            resource=API_REPORTS_PUSH_SECURITY_HUB,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def login(self, username, password):
        request = {
            PARAM_USERNAME: username,
            PARAM_PASSWORD: password
        }
        response = self.__make_request(
            resource=API_SIGNIN,
            method=HTTP_POST,
            payload=request
        )
        if isinstance(response, dict):
            return response
        if response.status_code != 200:
            if 'Incorrect username and/or' in response.text:
                message = 'Provided credentials are invalid.'
                SYSTEM_LOG.warning(message)
                return {'message': message}
            else:
                SYSTEM_LOG.error(f'Error: {response.text}')
                return {'message': 'Malformed response obtained. '
                                   'Please contact support team '
                                   'for assistance.'}
        response = response.json()
        SYSTEM_LOG.debug(f'Response: {response}')
        check_version_compatibility(
            response.get('items')[0].pop(PARAM_API_VERSION, None))
        return response.get('items')[0].get('id_token')

    def signup(self, username: str, password: str, customer: str, role: str,
               tenants: list) -> dict:
        return self.__make_request(
            resource=API_SIGNUP,
            method=HTTP_POST,
            payload=self.sifted({
                PARAM_USERNAME: username,
                PARAM_PASSWORD: password,
                PARAM_CUSTOMER: customer,
                PARAM_ROLE: role,
                PARAM_TENANTS: tenants
            })
        )

    def user_delete(self, username: Optional[str] = None) -> dict:
        return self.__make_request(
            resource=API_USERS,
            method=HTTP_DELETE,
            payload=self.sifted({PARAM_USERNAME: username})
        )

    def health_check_list(self, **kwargs):
        return self.__make_request(
            resource=API_HEALTH_CHECK,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def health_check_get(self, _id: str):
        return self.__make_request(
            resource=API_HEALTH_CHECK + f'/{_id}',
            method=HTTP_GET,
            payload={}
        )

    def siem_get(self, tenant_name, configuration_type):
        request = {
            PARAM_TENANT_NAME: tenant_name
        }
        if configuration_type == PARAM_DOJO:
            return self.__make_request(
                resource=API_SIEM_DOJO,
                method=HTTP_GET,
                payload=request
            )
        else:
            return self.__make_request(
                resource=API_SIEM_SECURITY_HUB,
                method=HTTP_GET,
                payload=request
            )

    def siem_delete(self, tenant_name, configuration_type):
        request = {
            PARAM_TENANT_NAME: tenant_name
        }
        if configuration_type == PARAM_DOJO:
            return self.__make_request(
                resource=API_SIEM_DOJO,
                method=HTTP_DELETE,
                payload=request
            )
        else:
            return self.__make_request(
                resource=API_SIEM_SECURITY_HUB,
                method=HTTP_DELETE,
                payload=request
            )

    def siem_dojo_post(self, tenant_name,
                       configuration, entities_mapping=None):
        request = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_CONFIGURATION: configuration,
            PARAM_ENTITIES_MAPPING: entities_mapping
        }
        return self.__make_request(
            resource=API_SIEM_DOJO,
            method=HTTP_POST,
            payload=self.sifted(request)
        )

    def siem_security_hub_post(self, tenant_name, configuration):
        request = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_CONFIGURATION: configuration
        }
        return self.__make_request(
            resource=API_SIEM_SECURITY_HUB,
            method=HTTP_POST,
            payload=request
        )

    def siem_dojo_patch(self, tenant_name, configuration,
                        entities_mapping=None, clear_existing_mapping=None):
        request = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_CONFIGURATION: configuration
        }
        if entities_mapping:
            request[PARAM_ENTITIES_MAPPING] = entities_mapping
        if clear_existing_mapping is not None:
            request[PARAM_CLEAR_EXISTING_MAPPING] = clear_existing_mapping
        return self.__make_request(
            resource=API_SIEM_DOJO,
            method=HTTP_PATCH,
            payload=request
        )

    def siem_security_hub_patch(self, tenant_name, configuration):
        request = {
            PARAM_TENANT_NAME: tenant_name,
            PARAM_CONFIGURATION: configuration
        }
        return self.__make_request(
            resource=API_SIEM_SECURITY_HUB,
            method=HTTP_PATCH,
            payload=request
        )

    def license_get(self, license_key, customer):
        request = {
            PARAM_LICENSE_HASH_KEY: license_key,
            PARAM_CUSTOMER: customer
        }
        return self.__make_request(
            resource=API_LICENSE,
            method=HTTP_GET,
            payload=self.sifted(request)
        )

    def license_post(self, tenant: str, tenant_license_key: str):
        return self.__make_request(
            resource=API_LICENSE,
            method=HTTP_POST,
            payload=self.sifted({
                PARAM_TENANT_NAME: tenant,
                PARAM_TENANT_LICENSE_KEY: tenant_license_key,
            })
        )

    def license_delete(self,
                       customer_name,
                       license_key):
        request = {
            PARAM_CUSTOMER: customer_name,
            PARAM_LICENSE_HASH_KEY: license_key,
        }
        return self.__make_request(
            resource=API_LICENSE,
            method=HTTP_DELETE,
            payload=request
        )

    def license_sync(self, license_key):
        request = {
            PARAM_LICENSE_HASH_KEY: license_key,
        }
        return self.__make_request(
            resource=API_LICENSE_SYNC,
            method=HTTP_POST,
            payload=request
        )

    def findings_get(self, tenant_name: str,
                     filter_dict: dict, dependent: bool, expansion: str,
                     format_dict: dict, get_url: bool, raw: bool):
        _query = {PARAM_EXPAND_ON: expansion, PARAM_RAW: raw}
        _query.update(filter_dict)
        _query.update(format_dict)
        if tenant_name:
            _query.update({PARAM_TENANT_NAME: tenant_name})
        if dependent:
            _query.update({PARAM_DEPENDENT_INCLUSION: dependent})
        if get_url:
            _query.update({PARAM_GET_URL: get_url})
        return self.__make_request(
            resource=API_FINDINGS,
            method=HTTP_GET,
            payload=self.sifted(_query)
        )

    def findings_delete(self, tenant_name: str):
        _payload = {PARAM_TENANT_NAME: tenant_name}
        return self.__make_request(
            resource=API_FINDINGS,
            method=HTTP_DELETE,
            payload=self.sifted(_payload)
        )

    def user_assign_tenants(self, username: str, tenants: list):
        request = {
            PARAM_TARGET_USER: username,
            PARAM_TENANTS: tenants
        }
        return self.__make_request(resource=API_USER_TENANTS,
                                   method=HTTP_PATCH, payload=request)

    def user_unassign_tenants(self, username: str, tenants: list,
                              all_tenants: bool):
        request = {
            PARAM_TARGET_USER: username,
            PARAM_TENANTS: tenants,
            PARAM_ALL: all_tenants
        }
        return self.__make_request(resource=API_USER_TENANTS,
                                   method=HTTP_DELETE, payload=request)

    def user_describe_tenants(self, username: str):
        request = {
            PARAM_TARGET_USER: username
        }
        return self.__make_request(resource=API_USER_TENANTS,
                                   method=HTTP_GET, payload=request)

    def mail_setting_get(self, disclose: bool):
        query = {PARAM_DISCLOSE: disclose}
        return self.__make_request(
            resource=API_MAIL_SETTING, method=HTTP_GET,
            payload=self.sifted(query)
        )

    def mail_setting_post(
            self, username: str, password: str, host: str, port: int,
            password_alias: str, use_tls: bool, default_sender: str,
            max_emails: int
    ):
        payload = {
            PARAM_USERNAME: username,
            PARAM_PASSWORD: password,
            PARAM_HOST: host,
            PARAM_PORT: port,
            PARAM_PASSWORD_ALIAS: password_alias,
            PARAM_USE_TLS: use_tls,
            PARAM_DEFAULT_SENDER: default_sender,
            PARAM_MAX_EMAILS: max_emails
        }
        return self.__make_request(
            resource=API_MAIL_SETTING, method=HTTP_POST, payload=payload
        )

    def mail_setting_delete(self):
        return self.__make_request(
            resource=API_MAIL_SETTING, method=HTTP_DELETE, payload={}
        )

    def lm_config_setting_get(self):
        return self.__make_request(
            resource=API_LM_CONFIG_SETTING, method=HTTP_GET, payload={}
        )

    def lm_config_setting_post(self, **kwargs):
        return self.__make_request(
            resource=API_LM_CONFIG_SETTING,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def lm_config_setting_delete(self):
        return self.__make_request(
            resource=API_LM_CONFIG_SETTING, method=HTTP_DELETE, payload={}
        )

    def lm_client_setting_get(self, frmt: str):
        query = {
            PARAM_FORMAT: frmt
        }
        return self.__make_request(
            resource=API_LM_CLIENT_SETTING,
            method=HTTP_GET, payload=query
        )

    def lm_client_setting_post(
            self, key_id: str, algorithm: str, private_key: str, frmt: str,
            b64encoded: bool
    ):
        payload = {
            PARAM_KEY_ID: key_id,
            PARAM_ALGORITHM: algorithm,
            PARAM_PRIVATE_KEY: private_key,
            PARAM_FORMAT: frmt,
            PARAM_B64ENCODED: b64encoded
        }
        return self.__make_request(
            resource=API_LM_CLIENT_SETTING, method=HTTP_POST, payload=payload
        )

    def lm_client_setting_delete(self, key_id: str):
        return self.__make_request(
            resource=API_LM_CLIENT_SETTING, method=HTTP_DELETE, payload={
                PARAM_KEY_ID: key_id
            }
        )

    def event_action(self, vendor: Optional[str], events: List[Dict]):
        return self.__make_request(
            resource=EVENT_RESOURCE, method=HTTP_POST, payload=self.sifted({
                PARAM_VENDOR: vendor,
                PARAM_VERSION: '1.0.0',
                PARAM_EVENTS: events
            })
        )

    def batch_results_get(self, br_id: str):
        return self.__make_request(
            resource=API_BATCH_RESULTS + f'/{br_id}',
            method=HTTP_GET, payload={}
        )

    def batch_results_query(
            self, customer: Optional[str],
            tenant: Optional[str], start_date: Optional[str],
            end_date: Optional[str], next_token: Optional[str],
            limit: Optional[int]
    ):
        payload = {
            PARAM_TENANT_NAME: tenant,
            PARAM_CUSTOMER: customer,
            PARAM_START: start_date,
            PARAM_END: end_date,
            PARAM_LIMIT: limit,
            PARAM_NEXT_TOKEN: next_token
        }
        return self.__make_request(
            resource=API_BATCH_RESULTS,
            method=HTTP_GET, payload=self.sifted(request=payload)
        )

    def report_digests_get(
            self, tenant_name: str, start_date: str,
            end_date: str, job_type: str, href: bool,
            jobs: bool = False, job_id: Optional[str] = None
    ):
        resource = API_DIGESTS_REPORTS
        jobs_resource = f'/{PARAM_JOBS}'

        payload = {
            PARAM_HREF: href,
            PARAM_TYPE: job_type
        }

        if job_id:
            jobs_resource += f'/{job_id}'
            jobs = True
        else:
            payload[PARAM_START_ISO] = start_date
            payload[PARAM_END_ISO] = end_date
            resource += f'/{PARAM_TENANTS}/{tenant_name}'

        if jobs:
            resource += jobs_resource

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_digests_query(
            self, customer: str, start_date: str, end_date: str,
            job_type: str, href: bool, jobs: bool = False
    ):
        payload = {
            PARAM_START_ISO: start_date,
            PARAM_END_ISO: end_date,
            PARAM_HREF: href,
            PARAM_TYPE: job_type,
            PARAM_CUSTOMER: customer
        }
        resource = API_DIGESTS_REPORTS + f'/{PARAM_TENANTS}'
        if jobs:
            resource += f'/{PARAM_JOBS}'

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_details_get(
            self, tenant_name: str, start_date: str,
            end_date: str, job_type: str, href: bool,
            jobs: bool = False, job_id: Optional[str] = None
    ):
        resource = API_DETAILS_REPORTS
        jobs_resource = f'/{PARAM_JOBS}'

        payload = {
            PARAM_HREF: href,
            PARAM_TYPE: job_type
        }

        if job_id:
            jobs_resource += f'/{job_id}'
            jobs = True
        else:
            payload[PARAM_START_ISO] = start_date
            payload[PARAM_END_ISO] = end_date
            resource += f'/{PARAM_TENANTS}/{tenant_name}'

        if jobs:
            resource += jobs_resource

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_details_query(
            self, customer: str, start_date: str, end_date: str,
            job_type: str, href: bool, jobs: bool = False
    ):
        payload = {
            PARAM_START_ISO: start_date,
            PARAM_END_ISO: end_date,
            PARAM_HREF: href,
            PARAM_TYPE: job_type,
            PARAM_CUSTOMER: customer
        }
        resource = API_DETAILS_REPORTS + f'/{PARAM_TENANTS}'
        if jobs:
            resource += f'/{PARAM_JOBS}'

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_compliance_get(self, tenant_name: str = None,
                              href: bool = None, job_type: str = None,
                              jobs: bool = False,
                              job_id: Optional[str] = None):
        resource = API_COMPLIANCE_REPORTS
        jobs_resource = f'/{PARAM_JOBS}'

        payload = {
            PARAM_HREF: href,
            PARAM_TYPE: job_type,
        }

        if job_id:
            jobs_resource += f'/{job_id}'
            jobs = True
        else:
            resource += f'/{PARAM_TENANTS}'
            if tenant_name:
                resource += f'/{tenant_name}'

        if jobs:
            resource += jobs_resource

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_errors_get(
            self, job_id: str, job_type: str, href: bool,
            frmt: Optional[str] = None, subtype: str = None
    ):
        resource = API_ERROR_REPORTS
        jobs_resource = f'/{PARAM_JOBS}'

        payload = {
            PARAM_HREF: href,
            PARAM_TYPE: job_type,
            PARAM_FORMAT: frmt
        }
        if subtype:
            resource += f'/{subtype}'

        jobs_resource += f'/{job_id}'
        jobs = True

        if jobs and job_id:
            resource += jobs_resource

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_errors_query(
            self, end_date: str = None,
            customer: Optional[str] = None,
            tenant_name: Optional[str] = None,
            account_name: Optional[str] = None,
            start_date: str = None, job_type: str = None, href: bool = None,
            frmt: Optional[str] = None, subtype: str = None
    ):
        resource = API_ERROR_REPORTS
        payload = {
            PARAM_HREF: href,
            PARAM_TYPE: job_type,
            PARAM_FORMAT: frmt,
            PARAM_START_ISO: start_date,
            PARAM_END_ISO: end_date,
            PARAM_CUSTOMER: customer
        }
        if subtype:
            resource += f'/{subtype}'

        resource += f'/{PARAM_TENANTS}'
        if tenant_name:
            resource += f'/{tenant_name}'
            if account_name:
                resource += f'/{PARAM_ACCOUNTS}/{account_name}'

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_rules_get(
            self, tenant_name: str = None, job_type: str = None,
            href: bool = None,
            start_date: str = None, end_date: str = None,
            target_rule: Optional[str] = None,
            frmt: Optional[str] = None,
            jobs: bool = False, job_id: Optional[str] = None
    ):
        resource = API_RULES_REPORTS
        jobs_resource = f'/{PARAM_JOBS}'

        payload = {
            PARAM_HREF: href,
            PARAM_FORMAT: frmt,
            PARAM_TYPE: job_type,
            PARAM_RULE: target_rule
        }

        if job_id:
            jobs_resource += f'/{job_id}'
            jobs = True
        else:
            payload[PARAM_START_ISO] = start_date
            payload[PARAM_END_ISO] = end_date
            resource += f'/{PARAM_TENANTS}/{tenant_name}'

        if jobs:
            resource += jobs_resource

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def report_rules_query(
            self, customer: str, start_date: str, end_date: str,
            job_type: str, href: bool, jobs: bool = False,
            frmt: Optional[str] = None, target_rule: Optional[str] = None
    ):
        payload = {
            PARAM_START_ISO: start_date,
            PARAM_END_ISO: end_date,
            PARAM_HREF: href,
            PARAM_TYPE: job_type,
            PARAM_CUSTOMER: customer,
            PARAM_FORMAT: frmt,
            PARAM_RULE: target_rule
        }
        resource = API_RULES_REPORTS + f'/{PARAM_TENANTS}'
        if jobs:
            resource += f'/{PARAM_JOBS}'

        return self.__make_request(
            resource=resource, method=HTTP_GET, payload=self.sifted(payload)
        )

    def rabbitmq_get(self, **kwargs):
        return self.__make_request(
            resource=API_RABBITMQ,
            method=HTTP_GET,
            payload=self.sifted(kwargs)
        )

    def rabbitmq_post(self, **kwargs):
        return self.__make_request(
            resource=API_RABBITMQ,
            method=HTTP_POST,
            payload=self.sifted(kwargs)
        )

    def rabbitmq_delete(self, **kwargs):
        return self.__make_request(
            resource=API_RABBITMQ,
            method=HTTP_DELETE,
            payload=self.sifted(kwargs)
        )
