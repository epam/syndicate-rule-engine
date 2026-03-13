#!/usr/bin/env python3
"""
Event-Driven SQS loader — emulates events (AWS / MAESTRO) and publishes them to SQS.
Configuration: config.yaml (tenants and events).
"""

import argparse
import json
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Union, cast

import boto3
import yaml

VERSION = "1.0.0"
AWS_DETAIL_TYPE = "AWS API Call via CloudTrail"
SQS_BATCH_SIZE = 10  # AWS send_message_batch limit


def load_config(path: Union[str, Path]) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return cast(dict[str, Any], yaml.safe_load(f))  # type: ignore[no-untyped-call]


def load_events(path: Union[str, Path]) -> dict[str, Any]:
    """Load events from a separate YAML file (e.g. events.yml)."""
    with open(path, "r", encoding="utf-8") as f:
        data = cast(dict[str, Any], yaml.safe_load(f))  # type: ignore[no-untyped-call]
    return data.get("events") or {}


def build_aws_event(
    account_id: str, aws_region: str, event_source: str, event_name: str
) -> dict[str, Any]:
    return {
        "detail-type": AWS_DETAIL_TYPE,
        "detail": {
            "userIdentity": {"accountId": account_id},
            "awsRegion": aws_region,
            "eventSource": event_source,
            "eventName": event_name,
        },
    }


def build_maestro_event(
    tenant_name: str,
    region_name: str,
    cloud: str,
    event_source: str,
    event_name: str,
) -> dict[str, Any]:
    return {
        "tenantName": tenant_name,
        "regionName": region_name,
        "eventMetadata": {
            "cloud": cloud,
            "eventSource": event_source,
            "eventName": event_name,
        },
    }


def _expand_events_by_cloud(
    events_config: dict[str, Any], cloud: str
) -> list[tuple[str, str]]:
    """Returns list of (event_source, event_name) for the given cloud from config.events."""
    out: list[tuple[str, str]] = []
    cloud_events = events_config.get(cloud)
    if not cloud_events:
        return out
    # events[cloud] = list of dicts, each dict: event_source -> [event_name]
    for item in (
        cloud_events if isinstance(cloud_events, list) else [cloud_events]
    ):
        if isinstance(item, dict):
            for event_source, event_names in item.items():
                for name in (
                    event_names
                    if isinstance(event_names, list)
                    else [event_names]
                ):
                    out.append((event_source, name))
    return out


def _chunk_events_random(
    event_list: list[Any], min_per_msg: int = 1, max_per_msg: int = 5
) -> list[list[Any]]:
    """Split event list into chunks of random size (min_per_msg to max_per_msg) per message."""
    if not event_list:
        return []
    chunks = []
    remaining = list(event_list)
    while remaining:
        size = min(random.randint(min_per_msg, max_per_msg), len(remaining))
        chunks.append(remaining[:size])
        remaining = remaining[size:]
    return chunks


def build_payloads(
    config: dict[str, Any], events_config: dict[str, Any]
) -> list[dict[str, Any]]:
    """One or more payloads per (tenant, region); each payload has 1-5 events (random chunk of the batch).
    Config: aws_vendor, maestro_vendor. events_config: from events.yml (AWS/AZURE/GOOGLE).
    """
    payloads = []

    # AWS: for each (account_id, region), chunk events into messages of 1–5 events
    aws_vendor = config.get("aws_vendor") or {}
    aws_cfg = aws_vendor.get("AWS")
    if aws_cfg:
        account_ids = aws_cfg.get("account_ids") or []
        regions = aws_cfg.get("regions") or ["eu-central-1"]
        aws_events_flat = _expand_events_by_cloud(events_config, "AWS")
        for account_id in account_ids:
            for aws_region in regions:
                event_list = [
                    build_aws_event(account_id, aws_region, src, name)
                    for src, name in aws_events_flat
                ]
                for chunk in _chunk_events_random(event_list):
                    payloads.append(
                        {
                            "version": VERSION,
                            "vendor": "AWS",
                            "events": chunk,
                        }
                    )

    # MAESTRO: per cloud, for each (tenant_name, region), chunk events into messages of 1–5 events
    maestro_vendor = config.get("maestro_vendor") or {}
    for cloud, cloud_cfg in maestro_vendor.items():
        if not isinstance(cloud_cfg, dict):
            continue
        tenant_names = cloud_cfg.get("tenant_names") or []
        regions = cloud_cfg.get("regions") or ["global"]
        maestro_events_flat = _expand_events_by_cloud(events_config, cloud)
        for tenant_name in tenant_names:
            for region_name in regions:
                event_list = [
                    build_maestro_event(
                        tenant_name, region_name, cloud, src, name
                    )
                    for src, name in maestro_events_flat
                ]
                for chunk in _chunk_events_random(event_list):
                    payloads.append(
                        {
                            "version": VERSION,
                            "vendor": "MAESTRO",
                            "events": chunk,
                        }
                    )

    if not payloads:
        raise ValueError(
            "Configure aws_vendor.AWS and/or maestro_vendor and provide events.yml."
        )

    return payloads


def _send_batch(
    queue_url: str,
    bodies: list[str],
    region: str,
    delay_seconds: int = 0,
    *,
    is_fifo: bool = False,
) -> list[dict[str, Any]]:
    """Send up to SQS_BATCH_SIZE messages in one API call. Returns Failed entries if any."""
    if not bodies:
        return []
    client = boto3.client("sqs", region_name=region)  # type: ignore[no-untyped-call]
    entries = []
    for i, body in enumerate(bodies):
        entry: dict[str, Any] = {"Id": str(i), "MessageBody": body}
        if is_fifo:
            entry["MessageGroupId"] = "event-driven-loader"
            entry["MessageDeduplicationId"] = str(uuid.uuid4())
        entries.append(entry)
    response = client.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = response.get("Failed") or []
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    return failed


def send_to_sqs(
    config: dict[str, Any],
    payloads: list[dict[str, Any]],
    dry_run: bool = False,
    workers: int = 1,
) -> None:
    sqs_cfg = config.get("sqs") or {}
    queue_url = sqs_cfg.get("queue_url")
    region = sqs_cfg.get("region") or "eu-central-1"
    if not queue_url:
        raise ValueError("Config must specify sqs.queue_url.")

    message_count = config.get("message_count", 1)
    delay_seconds = config.get("delay_seconds", 0)

    if dry_run:
        print("DRY RUN — messages will not be sent.")
        for i, p in enumerate(payloads[:5]):
            print(f"--- Payload {i + 1} ---")
            print(json.dumps(p, indent=2, ensure_ascii=False))
        if len(payloads) > 5:
            print(f"... and {len(payloads) - 5} more payload(s).")
        return

    is_fifo = ".fifo" in (queue_url or "")
    print(f"Sending to: {queue_url}", file=sys.stderr)

    # Build all message bodies, then split into batches of SQS_BATCH_SIZE
    bodies = []
    for _ in range(message_count):
        for p in payloads:
            bodies.append(json.dumps(p, ensure_ascii=False))
    batches = [
        bodies[i : i + SQS_BATCH_SIZE]
        for i in range(0, len(bodies), SQS_BATCH_SIZE)
    ]

    all_failed: list[dict[str, Any]] = []
    if workers <= 1:
        for batch in batches:
            all_failed.extend(
                _send_batch(
                    queue_url, batch, region, delay_seconds, is_fifo=is_fifo
                )
            )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    _send_batch,
                    queue_url,
                    batch,
                    region,
                    delay_seconds,
                    is_fifo=is_fifo,
                )
                for batch in batches
            ]
            for fut in as_completed(futures):
                all_failed.extend(fut.result())

    if all_failed:
        print(
            f"Failed {len(all_failed)} message(s):",
            json.dumps(all_failed, indent=2),
            file=sys.stderr,
        )
        raise RuntimeError(
            f"SQS rejected {len(all_failed)} message(s). See stderr for details."
        )
    print(f"Sent {len(bodies)} message(s) to SQS ({len(batches)} batch(es)).")


def main() -> int:
    parser = argparse.ArgumentParser(description="Event-Driven SQS loader")
    parser.add_argument(
        "-c",
        "--config",
        default="scripts/ed_high_loader/config.qa.yml",
        help="Path to config (sqs, aws_vendor, maestro_vendor).",
    )
    parser.add_argument(
        "-e",
        "--events",
        default="scripts/ed_high_loader/events.yml",
        help="Path to events file (default: scripts/ed_high_loader/events.yml).",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Do not send to SQS, only print payloads",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of worker threads for sending (default: 1).",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        return 1
    events_path = Path(args.events)
    if not events_path.exists():
        print(f"Error: events file not found: {events_path}", file=sys.stderr)
        return 1

    try:
        config = load_config(config_path)
        events_config = load_events(events_path)
        payloads = build_payloads(config, events_config)
        if args.workers < 1:
            print("Error: --workers must be >= 1.", file=sys.stderr)
            return 1
        send_to_sqs(
            config, payloads, dry_run=args.dry_run, workers=args.workers
        )
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
