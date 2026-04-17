"""
Entrypoint for event sources consumer: python -m onprem.event_sources_consumer
"""

from __future__ import annotations

from services import SP

from .constants import EventConsumerEnv
from .consumer_loop import run_consumer_loop
from .health_server import run_health_server


def main() -> None:
    from helpers.log_helper import setup_logging

    setup_logging()
    application_service = SP.modular_client.application_service()
    ssm = SP.modular_client.assume_role_ssm_service()
    event_ingest_service = SP.event_ingest_service
    sts = SP.sts
    platform_service = SP.platform_service

    run_health_server(port=EventConsumerEnv.PORT.as_int())
    run_consumer_loop(
        application_service=application_service,
        ssm=ssm,
        event_ingest_service=event_ingest_service,
        sts=sts,
        platform_service=platform_service,
    )


if __name__ == '__main__':
    main()
