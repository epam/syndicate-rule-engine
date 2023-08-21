import re
from typing import Optional

from services.clients.sts import StsClient


class IAMClient:
    def __init__(self, sts_client: StsClient):
        self._sts_client = sts_client

    def build_role_arn(self, maybe_arn: str,
                       account_id: Optional[str] = None) -> str:
        if self.is_role_arn(maybe_arn):
            return maybe_arn
        account_id = account_id or self._sts_client.get_account_id()
        return f'arn:aws:iam::{account_id}:role/{maybe_arn}'

    @staticmethod
    def is_role_arn(arn: str) -> bool:
        return bool(re.match(r'^arn:aws:iam::\d{12}:role/[A-Za-z0-9_-]+$',
                             arn))
