from functools import cached_property
from typing import Generator, Iterable, Optional

from requests import RequestException, get

from helpers.constants import GOOGLE, GCP, ED_AWS_RULESET_NAME, \
    ED_GOOGLE_RULESET_NAME, ED_AZURE_RULESET_NAME, AWS, AZURE, \
    COMPOUND_KEYS_SEPARATOR
from helpers.log_helper import get_logger
from models.ruleset import Ruleset, RULESET_STANDARD
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class RulesetService:
    def __init__(self, environment_service: EnvironmentService):
        self._environment = environment_service

    def pull_ruleset_content(self, path: str, encoding: str = None):
        """
        Pulls streamable content from a given URI path and respectively
        decodes the data according to the provided notation
        :return: Union[str, Type[None]]
        """
        content = self._download_ruleset_content(path)
        if content is None:
            return
        try:
            content = content.decode(encoding) if encoding else content
        except (UnicodeDecodeError, UnicodeError, Exception) as e:
            _LOG.warning(
                f'Ruleset content from \'{path}\' could not be \'{encoding}\''
                f' decoded, due to the exception "{e}".'
            )
        return content

    @staticmethod
    def _download_ruleset_content(path: str):
        """Mandates action of requesting streamable data from a given path."""
        content = None
        try:
            with get(path, stream=True) as response:
                content = response.content
        except RequestException as e:
            _LOG.warning(f'Ruleset content from \'{path}\' could not be '
                         f'pulled, due to the following "{e}".')
        return content

    @staticmethod
    def get_ruleset_by_id(ruleset_id: str, attributes_to_get: list = None):
        return Ruleset.get_nullable(ruleset_id,
                                    attributes_to_get=attributes_to_get)

    def target_rulesets(self) -> Generator[Ruleset, None, None]:
        yield from self.i_rulesets_by_ids(self._environment.target_rulesets())

    def i_rulesets_by_ids(self, ids: Iterable[str]
                          ) -> Generator[Ruleset, None, None]:
        for _id in ids:
            item = self.get_ruleset_by_id(_id)
            if not item:
                _LOG.warning(f'Ruleset with id {_id} not found')
            yield item

    @cached_property
    def cloud_to_ed_ruleset_name(self) -> dict:
        return {
            GCP: ED_GOOGLE_RULESET_NAME,
            GOOGLE: ED_GOOGLE_RULESET_NAME,
            AWS: ED_AWS_RULESET_NAME,
            AZURE: ED_AZURE_RULESET_NAME
        }

    def get_ed_ruleset(self, cloud: str) -> Optional[Ruleset]:
        """
        Event driven rule-sets belong to SYSTEM.
        """
        # backward compatibility. Cloud comes from tenant
        name = self.cloud_to_ed_ruleset_name[cloud.upper()]
        sort_key = f'{self._environment.system_customer()}' \
                   f'{COMPOUND_KEYS_SEPARATOR}{RULESET_STANDARD}' \
                   f'{COMPOUND_KEYS_SEPARATOR}{name}{COMPOUND_KEYS_SEPARATOR}'
        return next(Ruleset.customer_id_index.query(
            hash_key=self._environment.system_customer(),
            range_key_condition=Ruleset.id.startswith(sort_key),
            filter_condition=(Ruleset.event_driven == True),
            limit=1,
            scan_index_forward=False
        ), None)
