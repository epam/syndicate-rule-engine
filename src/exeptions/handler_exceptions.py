# src/exceptions/handler_exceptions.py


class ReportHandlerException(Exception):
    """Base exception for report handler"""
    pass

class UnknownReceiversException(ReportHandlerException):
    """Exception for  data loss prevention,
    forbidden send reports to unknown receivers"""

    def __init__(self, unknown_receivers: list) -> None:
        self.receivers = unknown_receivers
