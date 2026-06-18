from functools import lru_cache
from typing import Literal
from typing_extensions import Self

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

CloudName = Literal['AWS', 'AZURE', 'GCP']


class SmokeSettings(BaseSettings):
    """
    Smoke settings for the Syndicate Rule Engine
    """

    model_config = SettingsConfigDict(
        env_prefix='SMOKE_SRE_',
        case_sensitive=False,
        extra='ignore',
    )

    cli_entrypoint: str = 'sre'
    username: str | None = None
    password: str | None = None
    customer: str | None = None
    api_link: str = 'http://0.0.0.0:8000/caas'
    system_customer: str = 'CUSTODIAN_SYSTEM'
    step_delay: int = Field(default=0, validation_alias='TEST_DELAY')


class RuleSourceSettings(BaseModel):
    """
    Rule source settings for rules management
    """

    cloud: CloudName
    pid: str
    ref: str = 'main'
    url: str = 'https://api.github.com'
    prefix: str = 'policies/'
    secret: str | None = None

    @classmethod
    def for_cloud(cls, cloud: CloudName) -> Self | None:
        env_prefix = f'SMOKE_SRE_{cloud}_RULE_SOURCE_'

        class _EnvRuleSourceSettings(BaseSettings):
            model_config = SettingsConfigDict(
                env_prefix=env_prefix,
                case_sensitive=False,
                extra='ignore',
            )

            secret: str | None = None
            pid: str | None = None
            ref: str = 'main'
            url: str = 'https://api.github.com'
            prefix: str = 'policies/'

        env = _EnvRuleSourceSettings()
        if not env.pid:
            return None
        return cls(
            cloud=cloud,
            pid=env.pid,
            ref=env.ref,
            url=env.url,
            prefix=env.prefix,
            secret=env.secret,
        )


@lru_cache
def get_settings() -> SmokeSettings:
    return SmokeSettings()


def get_rule_source(cloud: CloudName) -> RuleSourceSettings | None:
    return RuleSourceSettings.for_cloud(cloud)
