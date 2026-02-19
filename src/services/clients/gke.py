from __future__ import annotations

from pathlib import Path
from google.cloud import container_v1
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

class GKEClient:
    _scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    def __init__(self, credentials_path: str | Path):
        self._credentials_path = Path(credentials_path)
        self._creds = service_account.Credentials.from_service_account_file(
            filename=str(self._credentials_path),
            scopes=self._scopes,
        )
        self._creds.refresh(Request())
        self._client = container_v1.ClusterManagerClient(credentials=self._creds)

    @property
    def token(self) -> str | None:
        """Returns the access token for the service account credentials."""
        return self._creds.token

    def get_cluster(
        self,
        project_id: str,
        location: str,
        cluster_name: str,
    ) -> container_v1.types.Cluster | None:
        """
        Fetches a GKE cluster object using the initialized credentials.
        :param project_id: GCP project ID
        :param location: GKE cluster region/zone
        :param cluster_name: GKE cluster name
        :return: Cluster object
        """
        cluster_name_full = \
            f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"
        try:
            cluster = self._client.get_cluster(name=cluster_name_full)
        except Exception as e:
            _LOG.error(
                f'Failed to fetch GKE cluster "{cluster_name}" in project '
                f'"{project_id}", location "{location}". Error: {e}'
            )
            return
        return cluster
