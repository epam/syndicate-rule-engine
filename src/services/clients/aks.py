from __future__ import annotations

from pathlib import Path
from typing import cast
import base64
import tempfile

from azure.identity import CertificateCredential, ClientSecretCredential
from azure.mgmt.containerservice import ContainerServiceClient
from azure.core.credentials import TokenCredential
from modular_sdk.services.impl.maestro_credentials_service import \
    AZURECredentials, AZURECertificate

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class AKSClient:
    """
    Azure Kubernetes Service client that can authenticate with either
    client-secret credentials (AZURECredentials) or certificate-based
    credentials (AZURECertificate), and retrieve kubeconfig for a cluster.
    """

    def __init__(
        self,
        creds: AZURECredentials | AZURECertificate,
        subscription_id: str | None = None,
    ):
        self._creds = self._resolve_credentials(creds)
        self._subscription_id = creds.AZURE_SUBSCRIPTION_ID or subscription_id

        if not self._subscription_id:
            raise ValueError(
                "Azure subscription ID must be provided via "
                "creds.AZURE_SUBSCRIPTION_ID or constructor")

        self._client = ContainerServiceClient(self._creds, self._subscription_id)

    @staticmethod
    def _resolve_credentials(
        creds: AZURECredentials | AZURECertificate,
    ) -> TokenCredential:
        tenant_id = creds.AZURE_TENANT_ID
        client_id = creds.AZURE_CLIENT_ID

        credential: TokenCredential
        if hasattr(creds, "AZURE_CLIENT_CERTIFICATE_PATH"):
            cert_path = creds.AZURE_CLIENT_CERTIFICATE_PATH
            cert_password = creds.AZURE_CLIENT_CERTIFICATE_PASSWORD
            credential = cast(TokenCredential, CertificateCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                certificate_path=str(cert_path),
                password=cert_password,
            ))
        else:
            client_secret = creds.AZURE_CLIENT_SECRET
            credential = cast(TokenCredential, ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            ))
        return credential

    def get_kubeconfig_bytes(
        self,
        resource_group: str,
        cluster_name: str,
    ) -> bytes:
        """
        Fetch the kubeconfig for the given AKS cluster and return it as raw bytes (YAML content).
        """
        kube_result = self._client.managed_clusters.list_cluster_user_credentials(
            resource_group_name=resource_group,
            resource_name=cluster_name,
        )

        kubeconfigs = getattr(kube_result, "kubeconfigs", None)
        if not kubeconfigs:
            kube_result = self._client.managed_clusters.list_cluster_admin_credentials(
                resource_group_name=resource_group,
                resource_name=cluster_name,
            )

        kubeconfigs = getattr(kube_result, "kubeconfigs", None)
        if not kubeconfigs:
            _LOG.error(
                f"Kubeconfigs for AKS cluster '{cluster_name}' in resource "
                f"group '{resource_group}' not found."
            )

        raw_value = kubeconfigs[0].value
        return base64.b64decode(raw_value)

    @staticmethod
    def to_temp_file(data) -> Path:
        """
        Write the kubeconfig to a temporary file and return its Path.
        """
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as fp:
            fp.write(data)
            return Path(fp.name)

