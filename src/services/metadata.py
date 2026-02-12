import gzip
import io
import tempfile
import datetime
from enum import Enum
from typing import TYPE_CHECKING, Generator, Protocol, cast
from dateutil.relativedelta import relativedelta

import msgspec

from helpers import Version
from helpers.__version__ import __version__
from helpers.constants import (
    TACTICS_ID_MAPPING,
    RemediationComplexity,
    Severity,
)
from helpers.log_helper import get_logger
from helpers.reports import Standard, service_from_resource_type
from models.rule import RuleIndex
from services import cache
from services.reports_bucket import ReportMetaBucketsKeys

if TYPE_CHECKING:
    from services.clients.s3 import S3Client
    from services.environment_service import EnvironmentService
    from services.license_manager_service import LicenseManagerService
    from services.license_service import License

_LOG = get_logger(__name__)


class MitreAttack(msgspec.Struct, frozen=True, eq=True, kw_only=True):
    """
    Represents one specific MITRE Attack
    """

    tactic_name: str
    tactic_id: str
    technique_name: str
    technique_id: str
    sub_technique_name: str | None = msgspec.field(default=None)
    sub_technique_id: str | None = msgspec.field(default=None)

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

    def __post_init__(self):
        if bool(self.sub_technique_name) ^ bool(self.sub_technique_id):
            raise ValueError(
                'Both sub technique id and name must be specified'
            )

class Deprecation(msgspec.Struct, frozen=True, kw_only=True):
    date: datetime.date | msgspec.UnsetType = msgspec.field(default=msgspec.UNSET)
    _is_deprecated: bool | msgspec.UnsetType = msgspec.field(default=msgspec.UNSET, name='is_deprecated')
    link: str | msgspec.UnsetType = msgspec.field(default=msgspec.UNSET)

    @property
    def is_deprecated(self) -> bool:
        """
        is_deprecated is a dynamic attribute that depends on the current date so we must calculate it each time
        unless we don't know the date
        """
        if self.date:
            return datetime.date.today() >= self.date
        return self._is_deprecated if isinstance(self._is_deprecated, bool) else False

    @property
    def is_outdated(self) -> bool:
        return not self.date and not self.is_deprecated

    @property
    def severity(self) -> Severity:
        """
        Calculates deprecation severity based on deprecation date:

        now     1m     2m     3m     4m     5m     6m     7m     8m
        ------------------------------------------------------------
        High    High   High   High   Medium Medium Medium  Low   Low
        """
        if self.is_deprecated:
            return Severity.HIGH
        if self.is_outdated:
            return Severity.INFO
        # not deprecated and not outdated, means date exists, and it is bigger than now
        now = datetime.date.today()
        assert self.date and self.date > now, 'date must be present if not is_deprecated and not is_outdated'
        # TODO: revise and improve difference calculation if needed
        diff = relativedelta(self.date, now)
        months = diff.years * 12 + diff.months
        if diff.days > 0:
            months += 1

        if months <= 3:
            return Severity.HIGH
        elif 3 < months <= 6:
            return Severity.MEDIUM
        else:  # 6 < months
            return Severity.LOW


class RuleMetadata(
    msgspec.Struct, kw_only=True, array_like=True, frozen=True, eq=False
):
    """
    Represents the formation in which per rule metadata is returned from LM and
    is stored
    """

    source: str
    category: str
    service_section: str
    service: str
    severity: Severity = msgspec.field(default=Severity.UNKNOWN)
    report_fields: tuple[str, ...] = ()
    periodic: bool = False
    article: str
    impact: str
    remediation: str
    standard: dict[str, dict[str, tuple[str, ...]]] = msgspec.field(
        default_factory=dict
    )
    mitre: dict[str, list[dict]] = msgspec.field(default_factory=dict)
    waf: dict = msgspec.field(default_factory=dict)
    remediation_complexity: RemediationComplexity = msgspec.field(
        default=RemediationComplexity.UNKNOWN
    )
    deprecation: Deprecation = msgspec.field(default=Deprecation())  # it's immutable so can a default
    cloud: str
    events: dict | None = msgspec.field(default=None)

    def __repr__(self) -> str:
        return (
            f'{__name__}.{self.__class__.__name__} object at {hex(id(self))}'
        )

    def is_finops(self) -> bool:
        return 'finops' in self.category.lower()

    def finops_category(self) -> str | None:
        if not self.is_finops():
            return
        return self.category.split('>')[-1].strip()

    def is_deprecation(self) -> bool:
        return 'deprecation' in self.category.lower()

    def deprecation_category(self) -> str | None:
        if not self.is_deprecation():
            return
        return self.category.split('>')[-1].strip()

    def iter_mitre_attacks(self) -> Generator[MitreAttack, None, None]:
        for tactic_name, techniques in self.mitre.items():
            tactic_id = TACTICS_ID_MAPPING.get(tactic_name)
            if not tactic_id:
                _LOG.warning(f'Not known tactic name: {tactic_name}')
                continue
            for technique in techniques:
                tn_id = technique.get('tn_id')
                tn_name = technique.get('tn_name')
                if not tn_id or not tn_name:
                    _LOG.warning(
                        f'Technique name or id not found: {technique}'
                    )
                    continue

                if sub := technique.get('st'):
                    for s in sub:
                        st_id = s.get('st_id')
                        st_name = s.get('st_name')
                        if not st_id or not st_name:
                            _LOG.warning(
                                f'Sub technique name or id not found: {s}'
                            )
                            continue
                        yield MitreAttack(
                            tactic_name=tactic_name,
                            tactic_id=tactic_id,
                            technique_name=tn_name,
                            technique_id=tn_id,
                            sub_technique_name=st_name,
                            sub_technique_id=st_id,
                        )
                else:
                    yield MitreAttack(
                        tactic_name=tactic_name,
                        tactic_id=tactic_id,
                        technique_name=tn_name,
                        technique_id=tn_id,
                    )


class DomainMetadata(
    msgspec.Struct, kw_only=True, array_like=True, frozen=True, eq=False
):
    """
    Represents the formation in which per domain metadata is returned from
    LM and is stored
    """

    # dict[Standard, dict[str, int]] after loading
    tech_cov: dict = msgspec.field(default_factory=dict)
    full_cov: dict = msgspec.field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f'{__name__}.{self.__class__.__name__} object at {hex(id(self))}'
        )

    def __post_init__(self):
        for cov in (self.tech_cov, self.full_cov):
            for name in tuple(cov):
                for version, data in cov[name].items():
                    cov[Standard(name, version)] = data
                cov.pop(name)


EMPTY_RULE_METADATA = RuleMetadata(
    cloud='',
    source='',
    category='',
    service_section='',
    service='',
    article='',
    impact='',
    remediation='',
)
EMPTY_DOMAIN_METADATA = DomainMetadata()


DEFAULT_VERSION = Version(__version__)


class Metadata(msgspec.Struct, frozen=True, eq=False):
    rules: dict[str, RuleMetadata] = msgspec.field(default_factory=dict)
    domains: dict[str, DomainMetadata] = msgspec.field(default_factory=dict)

    def rule(
        self,
        name: str,
        /,
        *,
        comment: str | None = None,
        resource: str | None = None,
    ) -> RuleMetadata:
        if item := self.rules.get(name):
            return item
        if not comment and not resource:
            return EMPTY_RULE_METADATA
        index = RuleIndex(comment)
        return RuleMetadata(
            cloud=index.cloud or '',
            source=index.source or '',
            category=index.category or '',
            service_section=index.service_section or '',
            service=service_from_resource_type(resource) if resource else '',
            article='',
            impact='',
            remediation='',
            standard={index.source: {'null': ()}} if index.source else {},
        )

    def domain(self, name: str | Enum, /) -> DomainMetadata:
        if isinstance(name, Enum):
            key = name.value
        else:
            key = name
        if key == 'GCP':
            key = 'GOOGLE'
        return self.domains.get(key.upper(), EMPTY_DOMAIN_METADATA)

    @classmethod
    def empty(cls) -> 'Metadata':
        return cls()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(...{len(self.rules)} rules)'


EMPTY_METADATA = Metadata.empty()


def merge_metadata(*metadata: Metadata) -> Metadata:
    rules = {}
    domains = {}
    for item in metadata:
        rules.update(item.rules)
        domains.update(item.domains)
    return Metadata(rules=rules, domains=domains)


class MetadataRefreshHook(Protocol):
    """
    Hook to be called when metadata is refreshed from LM.
    """

    def on_refresh(
        self,
        metadata: Metadata,
        license_key: str,
        version: Version,
    ) -> None:
        """
        Called when metadata is refreshed from LM.
        Can be used to collect mappings from metadata.

        Args:
            metadata: Metadata object that was refreshed from LM.
            license_key: License key that was used to save metadata.
            version: Version of metadata that was saved.
        """
        pass


class MetadataProvider:
    __slots__ = '_lm', '_s3', '_env', '_cache', '_hooks'
    _dec = msgspec.msgpack.Decoder(type=Metadata)

    def __init__(
        self,
        lm_service: 'LicenseManagerService',
        s3_client: 'S3Client',
        environment_service: 'EnvironmentService',
        hooks: list[MetadataRefreshHook] | None = None,
    ):
        self._lm = lm_service
        self._s3 = s3_client
        self._env = environment_service
        self._hooks: list[MetadataRefreshHook] = hooks or []

        # cache for 30 minutes
        self._cache = cache.factory(ttu=lambda a, b, now: now + 1800)

    def _get_from_s3(
        self, license_key: str, version: Version
    ) -> Metadata | None:
        _LOG.info(f'Trying to load metadata for {license_key} from s3')
        buf = self._s3.gz_get_object(
            bucket=self._env.default_reports_bucket_name(),
            key=ReportMetaBucketsKeys.meta_key(license_key, version),
            gz_buffer=tempfile.TemporaryFile(),
        )
        if buf is None:
            return
        return self._dec.decode(cast(io.BytesIO, buf).getvalue())

    def _get_from_cache(
        self, license_key: str, version: Version
    ) -> Metadata | None:
        _LOG.info(f'Trying to get metadata for {license_key} from cache')
        return self._cache.get((license_key, version))

    def _save_to_cache(
        self, license_key: str, version: Version, meta: Metadata
    ) -> None:
        _LOG.info('Saving metadata to in-memory cache')
        self._cache[(license_key, version)] = meta

    def get_no_cache(
        self, lic: 'License', version: Version = DEFAULT_VERSION
    ) -> Metadata:
        """
        Tries to get from s3 locally. If our s3 does not have it, makes
        request to lm
        """
        meta = self._get_from_s3(lic.license_key, version)
        if meta:
            _LOG.info('Metadata is found in local s3. Returning')
            return meta

        cst = lic.first_customer
        tlk = lic.tenant_license_key(cst)
        if not tlk:
            _LOG.warning(
                f'Tenant license key not found when getting metadata for license {lic.license_key}'
            )
            return Metadata.empty()
        data = self._lm.cl.get_all_metadata(
            customer=cst,
            tenant_license_key=tlk,
            installation_version=version.to_str(),
        )
        # NOTE: lm not necessarily returns the specified version of meta.
        # it returns <= installation_version, but we store it under exactly
        # installation_version. Not a critical thing but could be improved
        if not data:
            _LOG.warning('Unsuccessful request to lm. No metadata returned')
            return Metadata.empty()
        # NOTE: lm returns metadata in gzipped format the same way we store
        # it, so we can hack this one and save response as is
        _LOG.info('Storing metadata returned by LM in S3')
        self._s3.put_object(
            bucket=self._env.default_reports_bucket_name(),
            key=ReportMetaBucketsKeys.meta_key(lic.license_key, version),
            body=data,
            content_encoding='gzip',
        )
        return self._dec.decode(gzip.decompress(data))

    def set(
        self,
        metadata: dict,
        lic: 'License',
        version: Version = DEFAULT_VERSION,
    ):
        self._s3.put_object(
            bucket=self._env.default_reports_bucket_name(),
            key=ReportMetaBucketsKeys.meta_key(lic.license_key, version),
            body=gzip.compress(msgspec.msgpack.encode(metadata)),
            content_encoding='gzip',
        )

    def get(
        self, lic: 'License', /, *, version: Version = DEFAULT_VERSION
    ) -> Metadata:
        meta = self._get_from_cache(lic.license_key, version)
        if meta:
            _LOG.info('Metadata is found in cache. Returning')
            return meta
        meta = self.get_no_cache(lic, version=version)
        _LOG.info('Saving metadata to cache')
        self._save_to_cache(lic.license_key, version, meta)
        return meta

    def refresh(
        self, 
        lic: 'License', 
        version: Version = DEFAULT_VERSION,
    ) -> Metadata:
        """
        Refreshes metadata by fetching from LM, ignoring S3 and cache.
        Updates both S3 storage and in-memory cache with fresh data.
        """
        _LOG.info(f'Refreshing metadata for {lic.license_key} from LM')
        cst = lic.first_customer
        tlk = lic.tenant_license_key(cst)
        if not tlk:
            _LOG.warning(
                f'Tenant license key not found when refreshing metadata for license {lic.license_key}'
            )
            return Metadata.empty()
        
        data = self._lm.cl.get_all_metadata(
            customer=cst,
            tenant_license_key=tlk,
            installation_version=version.to_str(),
        )
        
        if not data:
            _LOG.warning('Unsuccessful request to lm. No metadata returned')
            return Metadata.empty()
        
        _LOG.info('Storing refreshed metadata from LM in S3')
        self._s3.put_object(
            bucket=self._env.default_reports_bucket_name(),
            key=ReportMetaBucketsKeys.meta_key(
                license_key=lic.license_key, 
                version=version,
            ),
            body=data,
            content_encoding='gzip',
        )

        meta = self._dec.decode(gzip.decompress(data))

        _LOG.info('Updating cache with refreshed metadata')
        self._save_to_cache(
            license_key=lic.license_key, 
            version=version, 
            meta=meta,
        )
        for hook in self._hooks:
            hook.on_refresh(
                metadata=meta,
                license_key=lic.license_key,
                version=version,
            )
        return meta
