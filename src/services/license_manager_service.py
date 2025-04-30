from functools import cached_property

from helpers.log_helper import get_logger
from helpers.system_customer import SystemCustomer
from helpers.time_helper import utc_iso
from models.ruleset import EMPTY_VERSION, Ruleset
from services.clients.lm_client import LMClient, LMClientFactory, LMRulesetDTO
from services.clients.ssm import AbstractSSMClient
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService

_LOG = get_logger(__name__)


class LicenseManagerService:
    def __init__(
        self,
        settings_service: SettingsService,
        ssm: AbstractSSMClient,
        ruleset_service: RulesetService,
    ):
        self.settings_service = settings_service
        self.ssm = ssm
        self.ruleset_service = ruleset_service

    @cached_property
    def client(self) -> LMClient:
        _LOG.debug('Creating license manager client inside LM service')
        return LMClientFactory(
            settings_service=self.settings_service, ssm=self.ssm
        ).create()

    @property
    def cl(self) -> LMClient:
        return self.client

    def parse_ruleset_dto(
        self,
        dto: LMRulesetDTO,
        license_keys: list[str],
        versions: list[str] | tuple[str, ...] = (),
    ) -> Ruleset:
        """
        This is LM api version bound logic, so I put it here even though it
        seems like there is a better place for it
        :param dto:
        :param license_keys:
        :param versions: additional versions to include
        :return:
        """
        # NOTE: cannot put its version to version attribute because it's
        # composed with other attributes. We keep only one entity for licensed
        # ruleset that represents all its versions
        all_versions = {
            dto.get('version') or None,  # this is the current one
            *(dto.get('versions') or ()),
            *versions,
        }
        return self.ruleset_service.create(
            customer=SystemCustomer.get_name(),
            name=dto['name'],
            version=EMPTY_VERSION,
            cloud=dto['cloud'],
            rules=dto.get('rules') or [],
            status={'last_update_time': utc_iso()},
            licensed=True,
            license_keys=sorted(set(license_keys)),
            versions=sorted(all_versions, reverse=True),
            created_at=dto.get('creation_date'),
            description=dto.get('description'),
        )
