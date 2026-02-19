import operator
from http import HTTPStatus
from io import BytesIO
from itertools import chain
from typing import Generator

import msgspec
from modular_sdk.commons.constants import ApplicationType
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.services.parent_service import ParentService

from helpers.constants import Env
from helpers import download_url
from helpers.__version__ import __version__
from helpers.fingerprint import compute_rule_fingerprint
from helpers.lambda_response import build_response
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
    __slots__ = '_sp', '_cache', '_overwrite_rulesets'

    def __init__(
        self,
        sp: ServiceProvider,
        cache_rulesets: bool = False,
        overwrite_rulesets: bool = False,
    ):
        self._sp = sp
        self._cache = {} if cache_rulesets else None
        self._overwrite_rulesets = overwrite_rulesets

    def _store_ruleset(self, rs: LMRulesetDTO):
        s3 = self._sp.s3
        bucket = Env.RULESETS_BUCKET_NAME.as_str()
        name, version = rs['name'], rs['version']
        key = RulesetsBucketKeys.licensed_ruleset_key(name, version)
        if s3.gz_object_exists(bucket, key):
            common_msg = f'Ruleset {name}:{version} already exists in S3.'
            if not self._overwrite_rulesets:
                _LOG.info(f'{common_msg} Skipping...')
                return
            _LOG.info(f'{common_msg} Overwriting...')

        url = rs.get('download_url')
        if not url:
            _LOG.warning('License sync did not return url')
            return

        data = download_url(url)
        if not data:
            _LOG.warning(f'Could not download from url: {url}')
            return
        data.seek(0)
        raw_content = data.getvalue()

        # Validate that downloaded data is valid JSON before storing
        try:
            content = msgspec.json.decode(raw_content)
        except msgspec.DecodeError as e:
            raise LicenseSyncError(
                f'Downloaded ruleset {name}:{version} is not valid JSON: {e}'
            ) from e

        # Add fingerprint to policies that don't have it
        policies = content.get('policies', [])
        for policy in policies:
            if 'fingerprint' not in policy:
                resource = policy.get('resource', '')
                filters = policy.get('filters', [])
                if resource:
                    policy['fingerprint'] = compute_rule_fingerprint(
                        resource, filters
                    )
                    _LOG.debug(
                        f'Added fingerprint to policy {policy.get("name", "unknown")} '
                        f'in ruleset {name}:{version}'
                    )

        # Re-encode the updated content
        updated_content = msgspec.json.encode(content)
        if self._cache is not None:
            self._cache[(name, version)] = updated_content
        
        # Create a new BytesIO object with updated content
        updated_data = BytesIO(updated_content)
        s3.gz_put_object(
            bucket=bucket,
            key=key,
            body=updated_data,
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
        data, status_code = \
            self._sp.license_manager_service.client.sync_license(
            license_key=lic.license_key,
            customer=lic.customer,
            installation_version=__version__,
            include_ruleset_links=True,
        )
        if isinstance(data, str):
            now = utc_iso()
            valid_until = now if status_code == HTTPStatus.NOT_FOUND else None
            self._sp.license_service.update(
                item=lic,
                latest_sync=now,
                latest_sync_result=data,
                valid_until=valid_until,
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

    def handle_request(self, event, context):
        it = self.iter_licenses(event.get('license_keys', ()))
        overwrite_rulesets = event.get('overwrite_rulesets', False)
        for lic in it:
            _LOG.info(f'Going to sync license: {lic.license_key}')
            try:
                sync = LicenseSync(
                    sp=SERVICE_PROVIDER, 
                    overwrite_rulesets=overwrite_rulesets,
                )
                sync(lic)
                _LOG.info('License was synced')
            except LicenseSyncError as e:
                _LOG.warning(f'Error occurred: {e}')
            except Exception:
                _LOG.exception('Unexpected error occurred')
        return build_response()


def lambda_handler(event, context):
    return LicenseUpdater.build().lambda_handler(event=event, context=context)
