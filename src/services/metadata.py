import io
import tempfile
from typing import TYPE_CHECKING, cast
import gzip

import msgspec

from helpers import Version
from helpers.__version__ import __version__
from helpers.constants import Severity
from helpers.log_helper import get_logger
from helpers.reports import service_from_resource_type
from models.rule import RuleIndex
from services.reports_bucket import ReportMetaBucketsKeys
from services import cache

if TYPE_CHECKING:
    from services.clients.s3 import S3Client
    from services.environment_service import EnvironmentService
    from services.license_manager_service import LicenseManagerService
    from services.license_service import License

_LOG = get_logger(__name__)


class RuleMetadata(msgspec.Struct, kw_only=True, array_like=True, frozen=True, eq=False):
    """
    Represents the formation in which metadata is returned from LM and
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
    standard: dict = msgspec.field(default_factory=dict)
    mitre: dict = msgspec.field(default_factory=dict)
    waf: dict = msgspec.field(default_factory=dict)

    def __repr__(self) -> str:
        return f'{__name__}.{self.__class__.__name__} object at {hex(id(self))}'


EMPTY = RuleMetadata(
    source='',
    category='',
    service_section='',
    service='',
    article='',
    impact='',
    remediation='',
)

DEFAULT_VERSION = Version(__version__)


class Metadata(msgspec.Struct, frozen=True, eq=False):
    rules: dict[str, RuleMetadata] = msgspec.field(default_factory=dict)
    # TODO: other mappings

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
            return EMPTY
        index = RuleIndex(comment)
        return RuleMetadata(
            source=index.source or '',
            category=index.category or '',
            service_section=index.service_section or '',
            service=service_from_resource_type(resource) if resource else '',
            article='',
            impact='',
            remediation='',
        )

    def is_empty(self) -> bool:
        return bool(self.rules)

    @classmethod
    def empty(cls) -> 'Metadata':
        return cls()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(...{len(self.rules)} rules)'


class MetadataProvider:
    __slots__ = '_lm', '_s3', '_env', '_cache'
    _dec = msgspec.msgpack.Decoder(type=Metadata)

    def __init__(
        self,
        lm_service: 'LicenseManagerService',
        s3_client: 'S3Client',
        environment_service: 'EnvironmentService',
    ):
        self._lm = lm_service
        self._s3 = s3_client
        self._env = environment_service

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
        _LOG.info('Trying to get metadata for {license_key} from cache')
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

    def get(
        self, lic: 'License', /, *, version: Version = DEFAULT_VERSION
    ) -> Metadata:
        meta = self._get_from_cache(lic.license_key, version)
        if meta:
            _LOG.info('Metadata is found in cache. Returning')
            return meta
        meta = self.get_no_cache(lic)
        _LOG.info('Saving metadata to cache')
        self._save_to_cache(lic.license_key, version, meta)
        return meta
