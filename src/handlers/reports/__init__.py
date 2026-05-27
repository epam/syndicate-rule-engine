"""
Handlers for reports.

This module contains handlers for reports endpoints.
"""

from .compliance_handler import ComplianceReportHandler
from .details_handler import DetailedReportHandler
from .digest_handler import DigestReportHandler
from .errors_handler import ErrorsReportHandler
from .findings_handler import FindingsReportHandler
from .high_level_reports_handler import HighLevelReportsHandler
from .push_handler import SiemPushHandler
from .raw_report_handler import RawReportHandler
from .report_status_handler import ReportStatusHandlerHandler
from .resource_report_handler import ResourceReportHandler
from .rules_handler import JobsRulesHandler


__all__ = (
    "DetailedReportHandler",
    "DigestReportHandler",
    "FindingsReportHandler",
    "ComplianceReportHandler",
    "ErrorsReportHandler",
    "ResourceReportHandler",
    "JobsRulesHandler",
    "RawReportHandler",
    "HighLevelReportsHandler",
    "ReportStatusHandlerHandler",
    "SiemPushHandler",
)
