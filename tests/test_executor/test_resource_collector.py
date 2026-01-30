from unittest.mock import patch, MagicMock

class TestCustodianResourceCollector:
    """Tests for CustodianResourceCollector class."""

    def test_collector_type(self):
        """Collector has correct type."""
        from executor.job.resource_collector import CustodianResourceCollector
        from helpers.constants import ResourcesCollectorType

        assert (
            CustodianResourceCollector.collector_type
            == ResourcesCollectorType.CUSTODIAN
        )

    @patch("executor.job.resource_collector.collector.SP")
    def test_build_creates_instance(self, mock_sp):
        """build() creates a properly configured instance."""
        from executor.job.resource_collector import CustodianResourceCollector

        mock_sp.modular_client = MagicMock()
        mock_sp.resources_service = MagicMock()
        mock_sp.license_service = MagicMock()
        mock_sp.modular_client.tenant_settings_service.return_value = MagicMock()

        collector = CustodianResourceCollector.build()

        assert isinstance(collector, CustodianResourceCollector)
        assert collector._ms == mock_sp.modular_client
        assert collector._rs == mock_sp.resources_service
        assert collector._ls == mock_sp.license_service
