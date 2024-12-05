from typing import TYPE_CHECKING

import msgspec

from helpers.constants import Severity
from helpers.reports import service_from_resource_type
from models.rule import RuleIndex

if TYPE_CHECKING:
    from services.clients.s3 import S3Client
    from services.license_manager_service import LicenseManagerService
    from services.license_service import License


class RuleMetadata(msgspec.Struct, kw_only=True, array_like=True, frozen=True):
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


EMPTY = RuleMetadata(
    source='',
    category='',
    service_section='',
    service='',
    article='',
    impact='',
    remediation='',
)


class Metadata(msgspec.Struct, frozen=True):
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


class MetadataProvider:
    __slots__ = '_lm', '_s3'

    def __init__(
        self, lm_service: 'LicenseManagerService', s3_client: 'S3Client'
    ):
        self._lm = lm_service
        self._s3 = s3_client

    def get_for(self, lic: License) -> Metadata:
        pass
