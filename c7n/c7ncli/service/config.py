from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from pathlib import Path
from typing import Optional, Union, Dict, List, Generator, Tuple

from botocore.credentials import JSONFileCache
try:
    # just for typing
    from modular_cli_sdk.services.credentials_manager import \
        AbstractCredentialsManager
except ImportError:
    pass
from c7ncli.service.constants import CONFIG_FOLDER, CONF_ACCESS_TOKEN, \
    CONF_API_LINK, CONF_ITEMS_PER_COLUMN
from c7ncli.service.logger import get_logger, get_user_logger

SYSTEM_LOG = get_logger(__name__)
USER_LOG = get_user_logger(__name__)

Json = Union[Dict, List, int, str, bool, None]


class AbstractCustodianConfig(ABC):
    @property
    @abstractmethod
    def api_link(self) -> Optional[str]:
        ...

    @api_link.setter
    @abstractmethod
    def api_link(self, value):
        ...

    @property
    @abstractmethod
    def access_token(self) -> Optional[str]:
        ...

    @access_token.setter
    @abstractmethod
    def access_token(self, value):
        ...

    @property
    @abstractmethod
    def items_per_column(self) -> Optional[int]:
        ...

    @items_per_column.setter
    @abstractmethod
    def items_per_column(self, value: int):
        ...

    @abstractmethod
    def items(self) -> Generator[Tuple[str, Json], None, None]:
        ...

    @abstractmethod
    def clear(self):
        ...


class CustodianCLIConfig(JSONFileCache, AbstractCustodianConfig):
    """
    The inner implementation can be rewritten to YAML. Or i you want, you
    can same all the data in one file. The important thin is, data must be
    read from file each time the property is invoked.
    """
    CACHE_DIR = Path.home() / CONFIG_FOLDER

    def __init__(self, prefix: str = 'root', working_dir: Path = CACHE_DIR):
        super().__init__(working_dir=working_dir / prefix)

    def get(self, cache_key: str) -> Optional[Json]:
        if cache_key in self:
            return self[cache_key]

    @property
    def items_per_column(self) -> Optional[int]:
        """
        If None, all the items is shown. It's the default behaviour
        :return:
        """
        return self.get(CONF_ITEMS_PER_COLUMN)

    @items_per_column.setter
    def items_per_column(self, value: int):
        assert isinstance(value, (int, type(None)))
        self[CONF_ITEMS_PER_COLUMN] = value

    @items_per_column.deleter
    def items_per_column(self):
        if CONF_ITEMS_PER_COLUMN in self:
            del self[CONF_ITEMS_PER_COLUMN]

    @property
    def api_link(self) -> Optional[str]:
        return self.get(CONF_API_LINK)

    @api_link.setter
    def api_link(self, value: str):
        self[CONF_API_LINK] = value

    @api_link.deleter
    def api_link(self):
        if CONF_API_LINK in self:
            del self[CONF_API_LINK]

    @property
    def access_token(self) -> Optional[str]:
        return self.get(CONF_ACCESS_TOKEN)

    @access_token.setter
    def access_token(self, value: str):
        self[CONF_ACCESS_TOKEN] = value

    @access_token.deleter
    def access_token(self):
        if CONF_ACCESS_TOKEN in self:
            del self[CONF_ACCESS_TOKEN]

    @classmethod
    def public_config_params(cls) -> List[property]:
        return [
            cls.api_link,
            cls.items_per_column
        ]

    @classmethod
    def private_config_params(cls) -> List[property]:
        return [
            cls.access_token
        ]

    def items(self) -> Generator[Tuple[str, Json], None, None]:
        with ThreadPoolExecutor() as executor:  # it reads a lot of files
            futures = {
                executor.submit(prop.fget, self): prop.fget.__name__
                for prop in self.public_config_params()
            }
            for future in as_completed(futures):
                yield futures[future], future.result()

    def clear(self):
        it = chain(self.public_config_params(), self.private_config_params())
        with ThreadPoolExecutor() as executor:
            for prop in it:
                executor.submit(prop.fdel, self)


class CustodianWithCliSDKConfig(AbstractCustodianConfig):
    """
    For integration with modular cli sdk
    """

    def __init__(self, credentials_manager: 'AbstractCredentialsManager'):
        self._credentials_manager = credentials_manager
        self._config_dict = {}

    @property
    def config_dict(self) -> dict:
        from modular_cli_sdk.commons.exception import ModularCliSdkBaseException
        # in order to be able to use other classes from this module
        # without cli_sdk installed
        if not self._config_dict:
            try:
                SYSTEM_LOG.info('Getting creds from credentials manager')
                self._config_dict = self._credentials_manager.extract()
            except ModularCliSdkBaseException:
                pass
        return self._config_dict

    def set(self, key: str, value: Json):
        config_dict = self.config_dict
        config_dict[key] = value
        self._credentials_manager.store(config_dict)

    @property
    def api_link(self) -> Optional[str]:
        return self.config_dict.get(CONF_API_LINK)

    @api_link.setter
    def api_link(self, value: str):
        self.set(CONF_API_LINK, value)

    @property
    def access_token(self) -> Optional[str]:
        return self.config_dict.get(CONF_ACCESS_TOKEN)

    @access_token.setter
    def access_token(self, value: str):
        self.set(CONF_ACCESS_TOKEN, value)

    @property
    def items_per_column(self) -> Optional[int]:
        return self.config_dict.get(CONF_ITEMS_PER_COLUMN)

    @items_per_column.setter
    def items_per_column(self, value: int):
        assert isinstance(value, (int, type(None)))
        self.set(CONF_ITEMS_PER_COLUMN, value)

    def items(self) -> Generator[Tuple[str, Json], None, None]:
        yield CONF_API_LINK, self.api_link
        yield CONF_ITEMS_PER_COLUMN, self.items_per_column

    def clear(self):
        self._credentials_manager.clean_up()
