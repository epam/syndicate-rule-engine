import json
import urllib
import urllib.error
import urllib.request
from functools import partial
from http import HTTPStatus
from http.client import HTTPResponse
from typing import Generator, Iterable
from urllib.parse import quote, urlencode

from srecli.service.config import AbstractSREConfig
from srecli.service.constants import (
    CONF_ACCESS_TOKEN,
    CONF_REFRESH_TOKEN,
    DATA_ATTR,
    ITEMS_ATTR,
    LAMBDA_INVOCATION_TRACE_ID_HEADER,
    MESSAGE_ATTR,
    SERVER_VERSION_HEADER,
    Endpoint,
    HTTPMethod,
)
from srecli.service.helpers import JWTToken, catch, sifted, urljoin
from srecli.service.logger import get_logger


_LOG = get_logger(__name__)


class ApiClient:
    """
    Simple JSON API client which is enough to cover our needs. It does not own
    sre-bound logic, i.e, access/refresh tokens and other. It knows
    how to build urls and make requests without handling sre-specific
    exceptions
    """
    __slots__ = ('_api_link',)

    def __init__(self, api_link: str):
        """
        :param api_link: pre-built link, can contain some prefix
        """
        self._api_link = api_link

    def build_url(self, path: str, params: dict | None = None,
                  query: dict | None = None) -> str:
        """
        The methods return full built url which can be used to make request
        :param path: some sre resource. One variable from
        Endpoints class
        :param params: path params
        :param query: dict with query params
        :return:
        """
        url = path.format(**(params or {}))
        url = quote(urljoin(url))  # to remove /
        if query:
            url += f'?{urlencode(sifted(query))}'
        return urljoin(self._api_link, url)

    @staticmethod
    def prepare_request(url: str, method: HTTPMethod, data: dict | None = None
                        ) -> urllib.request.Request:
        """
        Prepares request instance. Url must be built beforehand
        :param url:
        :param method:
        :param data:
        :return:
        """
        if isinstance(data, dict):
            return urllib.request.Request(
                url=url,
                method=method.value,
                data=json.dumps(data, separators=(',', ':')).encode(),
                headers={'Content-Type': 'application/json'}
            )
        return urllib.request.Request(url=url, method=method.value)

    def open_request(self, *args, **kwargs) -> HTTPResponse:
        return urllib.request.urlopen(*args, **kwargs)


class SREResponse:
    __slots__ = ('method', 'path', 'code', 'data', 'trace_id', 'api_version',
                 'exc')

    def __init__(self, method: HTTPMethod | None = None,
                 path: Endpoint | None = None,
                 code: HTTPStatus | None = None, data: dict | None = None,
                 trace_id: str | None = None, api_version: str | None = None,
                 exc: Exception | None = None):
        self.method = method
        self.path = path
        self.code = code
        self.data = data
        self.trace_id = trace_id
        self.api_version = api_version

        # JsonDecodeError | urllib.error.URLError - don't know how to handle
        # properly
        self.exc = exc

    def iter_items(self) -> Generator[dict, None, None]:
        d = self.data
        if not self.ok or not d:
            return
        if DATA_ATTR in d:
            yield d[DATA_ATTR]
        if ITEMS_ATTR in d:
            yield from d[ITEMS_ATTR]

    @property
    def was_sent(self) -> bool:
        """
        Tells whether the request was sent
        :return:
        """
        return self.code is not None

    @classmethod
    def build(cls, content: str | list | dict | Iterable,
              code: HTTPStatus = HTTPStatus.OK
              ) -> 'SREResponse':
        body = {}
        if isinstance(content, str):
            body.update({MESSAGE_ATTR: content})
        elif isinstance(content, dict) and content:
            body.update(content)
        elif isinstance(content, list):
            body.update({ITEMS_ATTR: content})
        elif isinstance(content, Iterable):
            body.update(({ITEMS_ATTR: list(content)}))
        return cls(data=body, code=code)

    @property
    def ok(self) -> bool:
        return self.code is not None and 200 <= self.code < 400


class SREApiClient:
    """
    This api client contains sre-specific logic. It uses the ApiClient
    from above for making requests
    """
    __slots__ = '_config', '_client', '_auto_refresh'

    def __init__(self, config: AbstractSREConfig):
        # api_link and access_token presence is validated before
        self._config = config
        self._client = ApiClient(api_link=config.api_link)

        self._auto_refresh = True

    def add_token(self, rec: urllib.request.Request,
                  header: str = 'Authorization'):
        """
        Adds token to the given request instance. Refreshes the token if needed
        :param header:
        :param rec:
        :return:
        """
        # access token should definitely exist here because we check its
        # presence before creating this class

        at = self._config.access_token
        rt = self._config.refresh_token
        if JWTToken(at).is_expired() and rt and self._auto_refresh:
            _LOG.info('Trying to auto-refresh token')
            resp = self.refresh(rt)
            if resp.ok:
                _LOG.info('Token was refreshed successfully. Updating config')
                at = resp.data.get('access_token')
                rt = resp.data.get('refresh_token')
                dct = {CONF_ACCESS_TOKEN: at}
                if rt:
                    # if new one. This probably won't happen because Cognito
                    # does not return a new refresh token. But just in case
                    dct[CONF_REFRESH_TOKEN] = rt
                self._config.update(dct)

        rec.add_header(header, at)

    def _sre_open(self, request: urllib.request.Request,
                        response: SREResponse) -> None:
        """
        Sends the given request instance. Fills the response instance with data
        :param request:
        :param response:  will be filled with some response data
        """
        try:
            resp = self._client.open_request(request)
        except urllib.error.HTTPError as e:
            resp = e
        except urllib.error.URLError as e:
            _LOG.exception('Cannot make a request')
            response.exc = e
            return
        response.code = HTTPStatus(resp.getcode())
        if response.code != HTTPStatus.NO_CONTENT:
            data, exc = catch(partial(json.load, resp), json.JSONDecodeError)
            response.data = data
            response.exc = exc

        response.trace_id = resp.headers.get(LAMBDA_INVOCATION_TRACE_ID_HEADER)
        response.api_version = resp.headers.get(SERVER_VERSION_HEADER)
        resp.close()
        return

    def make_request(
        self,
        path: Endpoint,
        method: HTTPMethod | None = None,
        path_params: dict | None = None,
        query: dict | None = None,
        data: dict | None = None,
    ) -> SREResponse:
        """
        High-level request method. Adds token.
        :param path:
        :param method:
        :param path_params:
        :param query:
        :param data:
        :return:
        """
        if not method:
            method = HTTPMethod.POST if data else HTTPMethod.GET
        req = self._client.prepare_request(
            url=self._client.build_url(path.value, path_params, query),
            method=method,
            data=data
        )
        self.add_token(req)
        response = SREResponse(method=method, path=path)
        self._sre_open(req, response)
        return response

    def refresh(self, token: str):
        req = self._client.prepare_request(
            url=self._client.build_url(Endpoint.REFRESH.value),
            method=HTTPMethod.POST,
            data={'refresh_token': token}
        )
        response = SREResponse(HTTPMethod.POST,
                                     Endpoint.REFRESH)
        self._sre_open(req, response)
        return response

    def login(self, username: str, password: str):
        req = self._client.prepare_request(
            url=self._client.build_url(Endpoint.SIGNIN.value),
            method=HTTPMethod.POST,
            data={'username': username, 'password': password}
        )
        response = SREResponse(HTTPMethod.POST, Endpoint.SIGNIN)
        self._sre_open(req, response)
        return response

    def whoami(self):
        return self.make_request(
            path=Endpoint.USERS_WHOAMI,
            method=HTTPMethod.GET
        )

    def customer_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.CUSTOMERS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def customer_get_excluded_rules(self, **kwargs):
        return self.make_request(
            path=Endpoint.CUSTOMERS_EXCLUDED_RULES,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def customer_set_excluded_rules(self, **kwargs):
        if kwargs.get('customer_id') is None:
            kwargs.pop('customer_id', None)
        return self.make_request(
            path=Endpoint.CUSTOMERS_EXCLUDED_RULES,
            method=HTTPMethod.PUT,
            data=dict(kwargs)
        )

    def tenant_get(self, tenant_name: str, **kwargs):
        return self.make_request(
            path=Endpoint.TENANTS_TENANT_NAME,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def tenant_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.TENANTS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def tenant_get_excluded_rules(self, tenant_name: str, **kwargs):
        return self.make_request(
            path=Endpoint.TENANTS_TENANT_NAME_EXCLUDED_RULES,
            method=HTTPMethod.GET,
            path_params={'tenant_name': tenant_name},
            query=sifted(kwargs)
        )

    def tenant_set_excluded_rules(self, tenant_name: str, **kwargs):
        # we cannot use sifted here because an empty list is accepted
        if kwargs.get('customer_id') is None:
            kwargs.pop('customer_id', None)
        return self.make_request(
            path=Endpoint.TENANTS_TENANT_NAME_EXCLUDED_RULES,
            method=HTTPMethod.PUT,
            path_params={'tenant_name': tenant_name},
            data=dict(kwargs)
        )

    def tenant_get_active_licenses(self, tenant_name: str, **kwargs):
        return self.make_request(
            path=Endpoint.TENANTS_TENANT_NAME_ACTIVE_LICENSES,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def ruleset_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULESETS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def ruleset_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULESETS,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def ruleset_update(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULESETS,
            method=HTTPMethod.PATCH,
            data=sifted(kwargs)
        )

    def ruleset_delete(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULESETS,
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def ruleset_release(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULESETS_RELEASE,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def rule_source_get(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.RULE_SOURCES_ID,
            path_params={'id': id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def rule_source_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULE_SOURCES,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def rule_source_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULE_SOURCES,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def rule_source_patch(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.RULE_SOURCES_ID,
            path_params={'id': id},
            method=HTTPMethod.PATCH,
            data=sifted(kwargs)
        )

    def rule_source_delete(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.RULE_SOURCES_ID,
            path_params={'id': id},
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def rule_source_sync(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.RULE_SOURCES_ID_SYNC,
            path_params={'id': id},
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def rule_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULES,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def rule_delete(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULES,
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def role_get(self, name, **kwargs):
        return self.make_request(
            path=Endpoint.ROLES_NAME,
            method=HTTPMethod.GET,
            path_params={'name': name},
            query=sifted(kwargs)
        )

    def role_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.ROLES,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def role_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.ROLES,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def role_patch(self, name, **kwargs):
        return self.make_request(
            path=Endpoint.ROLES_NAME,
            method=HTTPMethod.PATCH,
            path_params={'name': name},
            data=sifted(kwargs)
        )

    def role_delete(self, name, **kwargs):
        return self.make_request(
            path=Endpoint.ROLES_NAME,
            method=HTTPMethod.DELETE,
            path_params={'name': name},
            data=sifted(kwargs)
        )

    def policy_get(self, name, **kwargs):
        return self.make_request(
            path=Endpoint.POLICIES_NAME,
            method=HTTPMethod.GET,
            path_params={'name': name},
            query=sifted(kwargs)
        )

    def policy_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.POLICIES,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def policy_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.POLICIES,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def policy_patch(self, name, **kwargs):
        return self.make_request(
            path=Endpoint.POLICIES_NAME,
            method=HTTPMethod.PATCH,
            path_params={'name': name},
            data=sifted(kwargs)
        )

    def policy_delete(self, name, **kwargs):
        return self.make_request(
            path=Endpoint.POLICIES_NAME,
            method=HTTPMethod.DELETE,
            path_params={'name': name},
            data=sifted(kwargs)
        )

    def service_operations_status(self, service_operation_type: str, **kwargs):
        return self.make_request(
            path=Endpoint.SERVICE_OPERATIONS_STATUS,
            method=HTTPMethod.GET,
            query=sifted({'type': service_operation_type, **kwargs})
        )

    def trigger_metrics_update(self):
        return self.make_request(
            path=Endpoint.METRICS_UPDATE,
            method=HTTPMethod.POST,
            data={}
        )

    def trigger_metadata_update(self, **kwargs):
        return self.make_request(
            path=Endpoint.METADATA_UPDATE,
            method=HTTPMethod.POST,
            data=sifted(kwargs),
        )

    def trigger_rule_meta_updater(self, **kwargs):
        return self.make_request(
            path=Endpoint.RULE_META_UPDATER,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def job_list(self, **kwargs):
        return self.make_request(
            path=Endpoint.JOBS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def job_get(self, job_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.JOBS_JOB,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def job_post(self, **kwargs):
        api = Endpoint.JOBS
        return self.make_request(
            path=api,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def job_delete(self, job_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.JOBS_JOB,
            path_params={'job_id': job_id},
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def scheduled_job_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.SCHEDULED_JOB,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def scheduled_job_get(self, name: str, **kwargs):
        return self.make_request(
            path=Endpoint.SCHEDULED_JOB_NAME,
            path_params={'name': name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def scheduled_job_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.SCHEDULED_JOB,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def scheduled_job_delete(self, name: str, **kwargs):
        return self.make_request(
            path=Endpoint.SCHEDULED_JOB_NAME,
            path_params={'name': name},
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def scheduled_job_update(self, name, **kwargs):
        return self.make_request(
            path=Endpoint.SCHEDULED_JOB_NAME,
            path_params={'name': name},
            method=HTTPMethod.PATCH,
            data=sifted(kwargs)
        )

    def operational_report_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_OPERATIONAL,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def project_report_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_PROJECT,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def department_report_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_DEPARTMENT,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def c_level_report_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_CLEVEL,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def diagnostic_report_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_DIAGNOSTIC,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_status_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_STATUS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def push_dojo_by_job_id(self, job_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_PUSH_DOJO_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def push_dojo_multiple(self, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_PUSH_DOJO,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def push_chronicle_by_job_id(self, job_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_PUSH_CHRONICLE_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def health_check_list(self, **kwargs):
        return self.make_request(
            path=Endpoint.HEALTH,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def health_check_get(self, _id: str):
        return self.make_request(
            path=Endpoint.HEALTH_ID,
            path_params={'id': _id},
            method=HTTPMethod.GET,
        )

    def license_get(self, license_key, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSES_LICENSE_KEY,
            path_params={'license_key': license_key},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def license_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSES,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def license_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSES,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def license_delete(self, license_key: str, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSES_LICENSE_KEY,
            path_params={'license_key': license_key},
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def license_sync(self, license_key: str, overwrite_rulesets: bool = False, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSES_LICENSE_KEY_SYNC,
            path_params={'license_key': license_key},
            method=HTTPMethod.POST,
            data=sifted({'overwrite_rulesets': overwrite_rulesets, **kwargs})
        )

    def mail_setting_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_MAIL,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def mail_setting_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_MAIL,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def mail_setting_delete(self):
        return self.make_request(
            path=Endpoint.SETTINGS_MAIL,
            method=HTTPMethod.DELETE,
        )

    def reports_sending_setting_enable(self):
        return self.make_request(
            path=Endpoint.SETTINGS_SEND_REPORTS,
            method=HTTPMethod.POST,
            data={'enable': True}
        )

    def reports_sending_setting_disable(self):
        return self.make_request(
            path=Endpoint.SETTINGS_SEND_REPORTS,
            method=HTTPMethod.POST,
            data={'enable': False}
        )

    def lm_config_setting_get(self):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
            method=HTTPMethod.GET,
        )

    def lm_config_setting_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def lm_config_setting_patch(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
            method=HTTPMethod.PATCH,
            data=sifted(kwargs)
        )

    def lm_config_setting_delete(self):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
            method=HTTPMethod.DELETE,
        )

    def lm_client_setting_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def lm_client_setting_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def lm_client_setting_patch(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
            method=HTTPMethod.PATCH,
            data=sifted(kwargs)
        )

    def lm_client_setting_delete(self, **kwargs):
        return self.make_request(
            path=Endpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def event_action(self, **kwargs):
        return self.make_request(
            path=Endpoint.EVENT,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def report_digest_jobs(self, job_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_DIGESTS_JOBS_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_digest_tenants(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_DIGESTS_TENANTS_TENANT_NAME_JOBS,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_details_jobs(self, job_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_DETAILS_JOBS_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_details_tenants(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_DETAILS_TENANTS_TENANT_NAME_JOBS,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_findings_jobs(self, job_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_FINDINGS_JOBS_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_findings_tenants(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_FINDINGS_TENANTS_TENANT_NAME_JOBS,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_compliance_jobs(self, job_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_COMPLIANCE_JOBS_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_compliance_tenants(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_COMPLIANCE_TENANTS_TENANT_NAME,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_errors_job(self, job_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_ERRORS_JOBS_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_rules_get(self, job_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_RULES_JOBS_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_rules_query(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_RULES_TENANTS_TENANT_NAME,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_resource_latest(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_LATEST,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def platform_report_resource_latest(self, platform_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_RESOURCES_PLATFORMS_K8S_PLATFORM_ID_LATEST,
            path_params={'platform_id': platform_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_resource_jobs(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_JOBS,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_resource_job(self, job_id, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_RESOURCES_JOBS_JOB_ID,
            path_params={'job_id': job_id},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def report_raw_tenant(self, tenant_name, **kwargs):
        return self.make_request(
            path=Endpoint.REPORTS_RAW_TENANTS_TENANT_NAME_STATE_LATEST,
            path_params={'tenant_name': tenant_name},
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def rabbitmq_get(self, **kwargs):
        return self.make_request(
            path=Endpoint.CUSTOMERS_RABBITMQ,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def rabbitmq_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.CUSTOMERS_RABBITMQ,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def rabbitmq_delete(self, **kwargs):
        return self.make_request(
            path=Endpoint.CUSTOMERS_RABBITMQ,
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def platform_k8s_create(self, **kwargs):
        return self.make_request(
            path=Endpoint.PLATFORMS_K8S,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def platform_k8s_delete(self, platform_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.PLATFORMS_K8S_ID,
            path_params={'platform_id': platform_id},
            method=HTTPMethod.DELETE,
            data=sifted(kwargs)
        )

    def platform_k8s_list(self, **kwargs):
        return self.make_request(
            path=Endpoint.PLATFORMS_K8S,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def k8s_job_post(self, **kwargs):
        return self.make_request(
            path=Endpoint.JOBS_K8S,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def resources_get(self, **kwargs) -> SREResponse:
        return self.make_request(
            path=Endpoint.RESOURCES,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def resources_by_arn_get(self, **kwargs) -> SREResponse:
        return self.make_request(
            path=Endpoint.RESOURCES_ARN,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def resource_exception_get(self, **kwargs) -> SREResponse:
        return self.make_request(
            path=Endpoint.RESOURCES_EXCEPTIONS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def resource_exception_get_by_id(
        self,
        exception_id: str,
        customer_id: str | None = None,
    ) -> SREResponse:
        return self.make_request(
            path=Endpoint.RESOURCES_EXCEPTIONS_ID,
            method=HTTPMethod.GET,
            path_params={'id': exception_id},
            query=sifted({'customer_id': customer_id})
        )

    def resource_exception_add(self, **kwargs) -> SREResponse:
        return self.make_request(
            path=Endpoint.RESOURCES_EXCEPTIONS,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def resource_exception_update(
        self,
        exception_id: str,
        **kwargs,
    ) -> SREResponse:
        return self.make_request(
            path=Endpoint.RESOURCES_EXCEPTIONS_ID,
            method=HTTPMethod.PUT,
            path_params={'id': exception_id},
            data=sifted(kwargs)
        )

    def resource_exception_delete(
        self,
        exception_id: str,
        customer_id: str | None = None,
    ) -> SREResponse:
        return self.make_request(
            path=Endpoint.RESOURCES_EXCEPTIONS_ID,
            method=HTTPMethod.DELETE,
            path_params={'id': exception_id},
            query=sifted({'customer_id': customer_id}),
        )

    def dojo_add(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_DEFECT_DOJO,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def dojo_delete(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_DEFECT_DOJO_ID,
            method=HTTPMethod.DELETE,
            path_params={'id': id},
            data=sifted(kwargs)
        )

    def dojo_get(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_DEFECT_DOJO_ID,
            method=HTTPMethod.GET,
            path_params={'id': id},
            query=sifted(kwargs)
        )

    def dojo_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_DEFECT_DOJO,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def dojo_activate(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
            method=HTTPMethod.PUT,
            path_params={'id': id},
            data=sifted(kwargs)
        )

    def dojo_deactivate(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
            method=HTTPMethod.DELETE,
            path_params={'id': id},
            data=sifted(kwargs)
        )

    def dojo_get_activation(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
            method=HTTPMethod.GET,
            path_params={'id': id},
            query=sifted(kwargs)
        )

    def sre_add(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_SELF,
            method=HTTPMethod.PUT,
            data=sifted(kwargs)
        )

    def sre_update(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_SELF,
            method=HTTPMethod.PATCH,
            data=sifted(kwargs)
        )

    def sre_describe(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_SELF,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def sre_delete(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_SELF,
            method=HTTPMethod.DELETE,
            query=sifted(kwargs)
        )

    def license_activate(self, license_key: str, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSE_LICENSE_KEY_ACTIVATION,
            method=HTTPMethod.PUT,
            path_params={'license_key': license_key},
            data=sifted(kwargs)
        )

    def license_deactivate(self, license_key: str, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSE_LICENSE_KEY_ACTIVATION,
            method=HTTPMethod.DELETE,
            path_params={'license_key': license_key},
            data=sifted(kwargs)
        )

    def license_get_activation(self, license_key: str, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSE_LICENSE_KEY_ACTIVATION,
            method=HTTPMethod.GET,
            path_params={'license_key': license_key},
            query=sifted(kwargs)
        )

    def license_update_activation(self, license_key: str, **kwargs):
        return self.make_request(
            path=Endpoint.LICENSE_LICENSE_KEY_ACTIVATION,
            method=HTTPMethod.PATCH,
            path_params={'license_key': license_key},
            data=sifted(kwargs)
        )

    def get_credentials(self, application_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.CREDENTIALS_ID,
            method=HTTPMethod.GET,
            path_params={'id': application_id},
            query=sifted(kwargs)
        )

    def query_credentials(self, **kwargs):
        return self.make_request(
            path=Endpoint.CREDENTIALS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def credentials_bind(self, application_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.CREDENTIALS_ID_BINDING,
            method=HTTPMethod.PUT,
            path_params={'id': application_id},
            data=sifted(kwargs)
        )

    def credentials_unbind(self, application_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.CREDENTIALS_ID_BINDING,
            method=HTTPMethod.DELETE,
            path_params={'id': application_id},
            data=sifted(kwargs)
        )

    def credentials_get_binding(self, application_id: str, **kwargs):
        return self.make_request(
            path=Endpoint.CREDENTIALS_ID_BINDING,
            method=HTTPMethod.GET,
            path_params={'id': application_id},
            query=sifted(kwargs)
        )

    def get_user(self, username: str):
        return self.make_request(
            path=Endpoint.USERS_USERNAME,
            path_params={'username': username},
            method=HTTPMethod.GET,
        )

    def update_user(self, username: str, **kwargs):
        return self.make_request(
            path=Endpoint.USERS_USERNAME,
            path_params={'username': username},
            method=HTTPMethod.PATCH,
            data=sifted(kwargs)
        )

    def delete_user(self, username: str, **kwargs):
        return self.make_request(
            path=Endpoint.USERS_USERNAME,
            path_params={'username': username},
            method=HTTPMethod.DELETE,
            query=sifted(kwargs)
        )

    def query_user(self, **kwargs):
        return self.make_request(
            path=Endpoint.USERS,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def create_user(self, **kwargs):
        return self.make_request(
            path=Endpoint.USERS,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def reset_password(self, **kwargs):
        return self.make_request(
            path=Endpoint.USERS_RESET_PASSWORD,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def chronicle_add(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_CHRONICLE,
            method=HTTPMethod.POST,
            data=sifted(kwargs)
        )

    def chronicle_delete(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_CHRONICLE_ID,
            method=HTTPMethod.DELETE,
            path_params={'id': id},
            data=sifted(kwargs)
        )

    def chronicle_get(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_CHRONICLE_ID,
            method=HTTPMethod.GET,
            path_params={'id': id},
            query=sifted(kwargs)
        )

    def chronicle_query(self, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_CHRONICLE,
            method=HTTPMethod.GET,
            query=sifted(kwargs)
        )

    def chronicle_activate(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_CHRONICLE_ID_ACTIVATION,
            method=HTTPMethod.PUT,
            path_params={'id': id},
            data=sifted(kwargs)
        )

    def chronicle_deactivate(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_CHRONICLE_ID_ACTIVATION,
            method=HTTPMethod.DELETE,
            path_params={'id': id},
            data=sifted(kwargs)
        )

    def chronicle_get_activation(self, id: str, **kwargs):
        return self.make_request(
            path=Endpoint.INTEGRATIONS_CHRONICLE_ID_ACTIVATION,
            method=HTTPMethod.GET,
            path_params={'id': id},
            query=sifted(kwargs)
        )
