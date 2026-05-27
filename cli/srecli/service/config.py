from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Generator

from botocore.credentials import JSONFileCache
from srecli.service.constants import (
    CONF_ACCESS_TOKEN,
    CONF_API_LINK,
    CONF_ITEMS_PER_COLUMN,
    CONF_REFRESH_TOKEN,
    CONF_SYSTEM_CUSTOMER_NAME,
    CONFIG_FOLDER,
)
from srecli.service.logger import get_logger


if TYPE_CHECKING:
    from modular_cli_sdk.services.credentials_manager import (
        AbstractCredentialsManager,
    )

SYSTEM_LOG = get_logger(__name__)

Json = dict | list | float | int | str | bool


class AbstractSREConfig(ABC):
    @property
    @abstractmethod
    def api_link(self) -> str | None:
        ...

    @api_link.setter
    @abstractmethod
    def api_link(self, value):
        ...

    @property
    @abstractmethod
    def access_token(self) -> str | None:
        ...

    @access_token.setter
    @abstractmethod
    def access_token(self, value):
        ...

    @property
    @abstractmethod
    def refresh_token(self) -> str | None:
        ...

    @refresh_token.setter
    @abstractmethod
    def refresh_token(self, value):
        ...

    @property
    @abstractmethod
    def items_per_column(self) -> int | None:
        ...

    @items_per_column.setter
    @abstractmethod
    def items_per_column(self, value: int):
        ...

    @property
    @abstractmethod
    def system_customer_name(self) -> str | None:
        ...

    @system_customer_name.setter
    @abstractmethod
    def system_customer_name(self, value: str | None):
        ...

    @abstractmethod
    def items(self) -> Generator[tuple[str, Json], None, None]:
        ...

    @abstractmethod
    def clear(self):
        ...

    @abstractmethod
    def set(self, key: str, value: Json):
        ...

    @abstractmethod
    def update(self, dct: dict):
        ...


class SRECLIConfig(JSONFileCache, AbstractSREConfig):
    """
    The inner implementation can be rewritten to YAML. Or i you want, you
    can same all the data in one file. The important thin is, data must be
    read from file each time the property is invoked.
    """
    CACHE_DIR = Path.home() / CONFIG_FOLDER

    def __init__(self, prefix: str = 'root', working_dir: Path = CACHE_DIR):
        super().__init__(working_dir=working_dir / prefix)

    def get(self, cache_key: str) -> Json | None:
        if cache_key in self:
            return self[cache_key]

    def set(self, key: str, value: Json):
        self[key] = value

    def update(self, dct: dict):
        for key, value in dct.items():
            self.set(key, value)

    @property
    def items_per_column(self) -> int | None:
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
    def api_link(self) -> str | None:
        return self.get(CONF_API_LINK)

    @api_link.setter
    def api_link(self, value: str):
        # Clear system_customer_name when API link changes
        if CONF_SYSTEM_CUSTOMER_NAME in self:
            del self[CONF_SYSTEM_CUSTOMER_NAME]
        self[CONF_API_LINK] = value

    @api_link.deleter
    def api_link(self):
        if CONF_API_LINK in self:
            del self[CONF_API_LINK]

    @property
    def system_customer_name(self) -> str | None:
        return self.get(CONF_SYSTEM_CUSTOMER_NAME)

    @system_customer_name.setter
    def system_customer_name(self, value: str | None):
        if value is None:
            if CONF_SYSTEM_CUSTOMER_NAME in self:
                del self[CONF_SYSTEM_CUSTOMER_NAME]
        else:
            self[CONF_SYSTEM_CUSTOMER_NAME] = value

    @system_customer_name.deleter
    def system_customer_name(self):
        if CONF_SYSTEM_CUSTOMER_NAME in self:
            del self[CONF_SYSTEM_CUSTOMER_NAME]

    @property
    def access_token(self) -> str | None:
        return self.get(CONF_ACCESS_TOKEN)

    @access_token.setter
    def access_token(self, value: str):
        self[CONF_ACCESS_TOKEN] = value

    @access_token.deleter
    def access_token(self):
        if CONF_ACCESS_TOKEN in self:
            del self[CONF_ACCESS_TOKEN]

    @property
    def refresh_token(self) -> str | None:
        return self.get(CONF_REFRESH_TOKEN)

    @refresh_token.setter
    def refresh_token(self, value: str):
        self[CONF_REFRESH_TOKEN] = value

    @classmethod
    def public_config_params(cls) -> list[property]:
        return [
            cls.api_link,
            cls.items_per_column
        ]

    @classmethod
    def private_config_params(cls) -> list[property]:
        return [
            cls.access_token,
            cls.refresh_token
        ]

    def items(self) -> Generator[tuple[str, Json], None, None]:
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


class SREWithCliSDKConfig(AbstractSREConfig):
    """
    For integration with modular cli sdk
    """

    def __init__(self, credentials_manager: 'AbstractCredentialsManager'):
        self._credentials_manager = credentials_manager
        self._config_dict = {}

    @property
    def config_dict(self) -> dict:
        from modular_cli_sdk.commons.exception import (
            ModularCliSdkBaseException,
        )

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

    def update(self, dct: dict):
        config_dict = self.config_dict
        config_dict.update(dct)
        self._credentials_manager.store(config_dict)

    @property
    def api_link(self) -> str | None:
        return self.config_dict.get(CONF_API_LINK)

    @api_link.setter
    def api_link(self, value: str):
        # Clear system_customer_name when API link changes
        if CONF_SYSTEM_CUSTOMER_NAME in self.config_dict:
            del self.config_dict[CONF_SYSTEM_CUSTOMER_NAME]
            self._credentials_manager.store(self.config_dict)
        self.set(CONF_API_LINK, value)

    @property
    def access_token(self) -> str | None:
        return self.config_dict.get(CONF_ACCESS_TOKEN)

    @access_token.setter
    def access_token(self, value: str):
        self.set(CONF_ACCESS_TOKEN, value)

    @property
    def refresh_token(self) -> str | None:
        return self.config_dict.get(CONF_REFRESH_TOKEN)

    @refresh_token.setter
    def refresh_token(self, value: str):
        self.set(CONF_REFRESH_TOKEN, value)

    @property
    def items_per_column(self) -> int | None:
        return self.config_dict.get(CONF_ITEMS_PER_COLUMN)

    @items_per_column.setter
    def items_per_column(self, value: int):
        assert isinstance(value, (int, type(None)))
        self.set(CONF_ITEMS_PER_COLUMN, value)

    @property
    def system_customer_name(self) -> str | None:
        return self.config_dict.get(CONF_SYSTEM_CUSTOMER_NAME)

    @system_customer_name.setter
    def system_customer_name(self, value: str | None):
        if value is None:
            config_dict = self.config_dict
            config_dict.pop(CONF_SYSTEM_CUSTOMER_NAME, None)
            self._credentials_manager.store(config_dict)
        else:
            self.set(CONF_SYSTEM_CUSTOMER_NAME, value)

    def items(self) -> Generator[tuple[str, Json], None, None]:
        yield CONF_API_LINK, self.api_link
        yield CONF_ITEMS_PER_COLUMN, self.items_per_column

    def clear(self):
        self._credentials_manager.clean_up()
