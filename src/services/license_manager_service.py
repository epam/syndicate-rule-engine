from functools import cached_property

from helpers.log_helper import get_logger
from helpers.system_customer import SystemCustomer
from helpers.time_helper import utc_iso
from models.ruleset import Ruleset, EMPTY_VERSION
from services.clients.lm_client import LMClient, LMClientFactory
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger(__name__)


class LicenseManagerService:
    def __init__(self, settings_service: SettingsService,
                 ssm: AbstractSSMClient,
                 ruleset_service: RulesetService):
        self.settings_service = settings_service
        self.ssm = ssm
        self.ruleset_service = ruleset_service

    @cached_property
    def client(self) -> LMClient:
        _LOG.debug('Creating license manager client inside LM service')
        return LMClientFactory(
            settings_service=self.settings_service,
            ssm=self.ssm
        ).create()

    @property
    def cl(self) -> LMClient:
        return self.client

    def parse_ruleset_dto(self, dto: dict, license_keys: list[str]) -> Ruleset:
        """
        This is LM api version bound logic, so I put it here even though it
        seems like there is a better place for it
        :param dto:
        :param license_keys:
        :return:
        """
        return self.ruleset_service.create(
            customer=SystemCustomer.get_name(),
            name=dto['name'],
            version=EMPTY_VERSION,
            cloud=dto['cloud'],
            rules=dto.get('rules') or [],
            event_driven=False,
            status={'last_update_time': utc_iso()},
            licensed=True,
            license_keys=license_keys,
            license_manager_id=dto.get('id') or dto.get('name'),
            versions=dto.get('versions') or [],
            created_at=dto.get('creation_date'),
            description=dto.get('description')
        )
