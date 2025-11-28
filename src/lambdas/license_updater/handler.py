from http import HTTPStatus
import operator
from itertools import chain
from typing import Any, Generator, Mapping

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.commons.trace_helper import tracer_decorator
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.services.parent_service import ParentService

from helpers.constants import ServiceJobType, Env
from helpers import RequestContext, download_url
from helpers.__version__ import __version__
from helpers.lambda_response import build_response, ResponseFactory
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER, ServiceProvider
from services.abs_lambda import EventProcessorLambdaHandler
from services.clients.lm_client import LMRulesetDTO
from services.license_manager_service import LicenseManagerService
from services.license_service import License, LicenseService, SUCCESS_SYNC
from services.reports_bucket import RulesetsBucketKeys
from services.ruleset_service import RulesetService

_LOG = get_logger(__name__)


class LicenseSyncError(Exception):
    pass


class LicenseSync:
    __slots__ = '_sp', '_cache'

    def __init__(self, sp: ServiceProvider, cache_rulesets: bool = False):
        self._sp = sp
        self._cache = {} if cache_rulesets else None

    def _store_ruleset(self, rs: LMRulesetDTO):
        s3 = self._sp.s3
        bucket = Env.RULESETS_BUCKET_NAME.as_str()
        name, version = rs['name'], rs['version']
        key = RulesetsBucketKeys.licensed_ruleset_key(name, version)
        if s3.gz_object_exists(bucket, key):
            _LOG.info(f'Ruleset {name}:{version} already exists in S3')
            return

        url = rs.get('download_url')
        if not url:
            _LOG.warning('License sync did not return url')
            return

        data = download_url(url)
        if not data:
            _LOG.warning(f'Could not download from url: {url}')
            return
        data.seek(0)
        if self._cache is not None:
            self._cache[(name, version)] = data.getvalue()
        s3.gz_put_object(
            bucket=bucket,
            key=key,
            body=data,
            content_type='application/json',
            content_encoding='gzip',
        )

    def get_cached(self, name: str, version: str) -> bytes | None:
        if self._cache is None:
            return
        return self._cache.get((name, version))

    def _update_rulesets(
        self,
        license_key: str,
        old_rulesets: set[str],
        new_rulesets: list[LMRulesetDTO],
    ) -> None:
        """
        Changes the given items
        """
        rs = self._sp.ruleset_service
        ls = self._sp.license_service
        lms = self._sp.license_manager_service

        for ruleset in new_rulesets:
            name = ruleset['name']
            old_rulesets.discard(name)

            existing = rs.get_licensed(
                name,
                attributes_to_get=(Ruleset.license_keys, Ruleset.versions),
            )
            license_keys = [license_key]
            versions = []
            if existing:
                _LOG.info(
                    f'Ruleset {name} already exists in DB, Updating its attributes'
                )
                license_keys.extend(existing.license_keys)
                versions.extend(existing.versions)

            new_item = lms.parse_ruleset_dto(
                dto=ruleset,
                license_keys=license_keys,  # duplicates are handled inside
                versions=versions,
            )
            new_item.save()  # overrides the existing
        while old_rulesets:
            name = old_rulesets.pop()
            ls.remove_ruleset_for_license(name, license_key)

    def __call__(self, lic: License):
        """
        Syncs the given license
        """
        data = self._sp.license_manager_service.client.sync_license(
            license_key=lic.license_key,
            customer=lic.customer,
            installation_version=__version__,
            include_ruleset_links=True,
        )
        if isinstance(data, str):
            self._sp.license_service.update(
                item=lic,
                latest_sync=utc_iso(),
                latest_sync_result=data
            )
            raise LicenseSyncError('Request to the License manager failed')

        new_rulesets = data.get('rulesets', [])
        old_rulesets = set(lic.ruleset_ids)

        _LOG.debug('Storing new rulesets to s3')
        for rs in new_rulesets:
            self._store_ruleset(rs)

        _LOG.debug('Updating license item')
        ed = data.get('event_driven') or {}
        if quota := ed.get('quota'):  # fixing some bug in cslm
            ed['quota'] = int(quota)
        self._sp.license_service.update(
            item=lic,
            description=data.get('description'),
            allowance=data.get('allowance'),
            customers=data.get('customers'),
            event_driven=ed,
            rulesets=sorted([rs['name'] for rs in new_rulesets]),
            latest_sync=utc_iso(),
            valid_until=data.get('valid_until'),
            valid_from=data.get('valid_from'),
            latest_sync_result=SUCCESS_SYNC,
        )

        _LOG.debug('Updating rulesets')
        self._update_rulesets(lic.license_key, old_rulesets, new_rulesets)


class LicenseUpdater(EventProcessorLambdaHandler):
    processors = ()

    def __init__(
        self,
        license_service: LicenseService,
        license_manager_service: LicenseManagerService,
        ruleset_service: RulesetService,
        customer_service: CustomerService,
        application_service: ApplicationService,
        parent_service: ParentService,
    ):
        self.license_service = license_service
        self.license_manager_service = license_manager_service
        self.ruleset_service = ruleset_service
        self.application_service = application_service
        self.parent_service = parent_service
        self.customer_service = customer_service

    @classmethod
    def build(cls) -> 'LicenseUpdater':
        return cls(
            license_service=SERVICE_PROVIDER.license_service,
            license_manager_service=SERVICE_PROVIDER.license_manager_service,
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            customer_service=SERVICE_PROVIDER.modular_client.customer_service(),
            application_service=SERVICE_PROVIDER.modular_client.application_service(),
            parent_service=SERVICE_PROVIDER.modular_client.parent_service(),
        )

    def iter_licenses(
        self, license_keys: list[str]
    ) -> Generator[License, None, None]:
        if license_keys:
            yield from self.license_service.iter_by_ids(license_keys)
            return
        _LOG.info('Iterating over all the licenses')
        customers = map(
            operator.attrgetter('name'), self.customer_service.i_get_customer()
        )
        apps = chain.from_iterable(
            self.application_service.i_get_application_by_customer(
                name, ApplicationType.CUSTODIAN_LICENSES.value, deleted=False
            )
            for name in customers
        )
        yield from self.license_service.to_licenses(apps)

    @tracer_decorator(
        is_job=True, 
        component=ServiceJobType.LICENSE_SYNC.value,
    )
    def handle_request(
        self,
        event: Mapping[str, Any],
        context: RequestContext,
    ):
        license_keys: list[str] = list(event.get('license_keys', ()))
        
        if not license_keys:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                'No license keys provided'
            ).exc()
        
        licenses = list(self.iter_licenses(license_keys))
        total = len(licenses)
        
        if total != len(license_keys):
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'No licenses found for keys: {", ".join(license_keys)}'
            ).exc()
        
        _LOG.info(f'Starting sync for {total} license(s)')
        
        successful_licenses = []
        failed_licenses = []
        
        for idx, lic in enumerate(licenses, start=1):
            license_key = lic.license_key
            _LOG.info(f'[{idx}/{total}] Syncing license: {license_key}')
            
            try:
                sync = LicenseSync(SERVICE_PROVIDER)
                sync(lic)
                _LOG.info(
                    f'[{idx}/{total}] Successfully synced: {license_key}',
                )
                successful_licenses.append(license_key)
            except LicenseSyncError as e:
                _LOG.warning(
                    f'[{idx}/{total}] Sync error for {license_key}: {e}',
                )
                failed_licenses.append(license_key)
            except Exception:
                _LOG.exception(
                    f'[{idx}/{total}] Unexpected error for {license_key}'
                )
                failed_licenses.append(license_key)
        
        _LOG.info(
            f'Sync completed: {len(successful_licenses)}/{total} successful.',
        )
        
        if failed_licenses:
            raise ResponseFactory(HTTPStatus.INTERNAL_SERVER_ERROR).message(
                f'Failed to sync {len(failed_licenses)}/{total} license(s). '
                f'Failed: {", ".join(failed_licenses)}'
            ).exc()
        
        return build_response(
            content=f'All {total} license(s) synced successfully',
        )


def lambda_handler(event, context):
    return LicenseUpdater.build().lambda_handler(event=event, context=context)
