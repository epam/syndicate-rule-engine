#!/usr/bin/env python3
"""Generate Powerpipe controls and benchmarks from ecc-kubernetes-metadata YAML."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
MOD_DIR = SCRIPT_DIR.parent
GENERATED_DIR = MOD_DIR / "generated"
DEFAULT_METADATA_DIR = (
    SCRIPT_DIR.resolve().parents[4] / "metadata/ecc-kubernetes-metadata/metadata/on-prem"
)

CIS_LINE_RE = re.compile(r"^v([\d.]+)\s*\(([^)]+)\)\s*$")
CONTROL_ID_SAFE_RE = re.compile(r"[^a-zA-Z0-9_]+")
CLUSTER_SECTIONS = {
    "API Server",
    "etcd",
    "Controller Manager",
    "Scheduler",
    "Kubelet",
}


def policy_from_filename(path: Path) -> str:
    name = path.name
    if name.endswith("_metadata.yml"):
        return name[: -len("_metadata.yml")]
    return path.stem


def safe_name(value: str) -> str:
    value = value.replace("-", "_")
    value = CONTROL_ID_SAFE_RE.sub("_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value.lower()


def cis_section(control_id: str) -> str:
    parts = control_id.strip().split(".")
    if len(parts) >= 2:
        return f"{parts[0]}.{parts[1]}"
    return parts[0] if parts else "0"


def parse_cis_entries(standards: list[str] | None) -> list[dict[str, str]]:
    if not standards:
        return []
    out = []
    for entry in standards:
        match = CIS_LINE_RE.match(str(entry).strip())
        if not match:
            continue
        version, controls = match.group(1), match.group(2)
        for control_id in (c.strip() for c in controls.split(",")):
            if control_id:
                out.append(
                    {
                        "version": version,
                        "control_id": control_id,
                        "section": cis_section(control_id),
                    }
                )
    return out


def load_rules(metadata_dir: Path) -> list[dict]:
    rules = []
    for path in sorted(metadata_dir.glob("ecc-k8s-*_metadata.yml")):
        with path.open(encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        meta = doc.get("metadata") or {}
        policy = policy_from_filename(path)
        standards = meta.get("standard") or {}
        cis = parse_cis_entries(standards.get("CIS Kubernetes Benchmark"))
        cis_gke = parse_cis_entries(standards.get("CIS GKE Benchmark"))
        article = (meta.get("article") or "").strip().splitlines()
        title_hint = article[0].strip() if article else policy
        rules.append(
            {
                "policy": policy,
                "control_name": safe_name(policy),
                "service": meta.get("service") or "Kubernetes",
                "service_section": meta.get("service_section") or "General",
                "title_hint": title_hint,
                "cis": cis,
                "cis_gke": cis_gke,
                "cluster_level": meta.get("service_section") in CLUSTER_SECTIONS,
            }
        )
    return rules


def sql_cluster_rule(rule_name: str) -> str:
    return f"""\
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, '{rule_name}') as reason,
  namespace
from sre_finding
where job_id = '${{var.job_id}}'
  and rule_name = '{rule_name}'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${{var.job_id}}'
    and rule_name = '{rule_name}'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${{var.job_id}}'
    and policy = '{rule_name}'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${{var.job_id}}'
  and policy = '{rule_name}'
  and nullif(error_type, '') is not null
"""


def sql_workload_rule(rule_name: str) -> str:
    return f"""\
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, '{rule_name}') as reason,
  namespace
from sre_finding
where job_id = '${{var.job_id}}'
  and rule_name = '{rule_name}'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${{var.job_id}}'
          and policy = '{rule_name}'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${{var.job_id}}'
  and policy = '{rule_name}'
  and nullif(error_type, '') is not null
"""


def render_control(rule: dict) -> str:
    policy = rule["policy"]
    control_id = rule["control_name"]
    sql = sql_cluster_rule(policy) if rule["cluster_level"] else sql_workload_rule(policy)
    cis_tags = ""
    for entry in rule["cis"]:
        if entry["version"] == "1.7.0":
            cis_tags = f"""
  tags = merge(local.common_tags, {{
    cis_item_id = "{entry["control_id"]}"
    cis_version = "v1.7.0"
  }})"""
            break
    if not cis_tags:
        cis_tags = "\n  tags = local.common_tags"

    return f"""
control "{control_id}" {{
  title       = "{policy}"
  description = {json.dumps(rule["title_hint"])}
  query       = query.{control_id}
{cis_tags}
}}

query "{control_id}" {{
  sql = <<-EOQ
{sql}EOQ
}}
"""


def render_benchmark(name: str, title: str, description: str, children: list[str], extra_tags: str = "") -> str:
    child_refs = ",\n    ".join(children)
    tags = "merge(local.common_tags, { type = \"Benchmark\" }"
    if extra_tags:
        tags += f", {extra_tags}"
    tags += ")"
    return f"""
benchmark "{name}" {{
  title       = {json.dumps(title)}
  description = {json.dumps(description)}
  children = [
    {child_refs}
  ]
  tags = {tags}
}}
"""


def section_benchmark_name(prefix: str, section: str) -> str:
    return f"{prefix}_{safe_name(section)}"


def write_controls(rules: list[dict]) -> None:
    lines = ["# Generated controls — do not edit manually\n"]
    for rule in rules:
        lines.append(render_control(rule))
    (GENERATED_DIR / "controls.pp").write_text("\n".join(lines), encoding="utf-8")


def write_all_controls(rules: list[dict]) -> None:
    by_service: dict[str, list[dict]] = defaultdict(list)
    for rule in rules:
        by_service[rule["service"]].append(rule)

    sections = []
    for service in sorted(by_service):
        sec_name = section_benchmark_name("all_controls", service)
        controls = [f"control.{r['control_name']}" for r in sorted(by_service[service], key=lambda r: r["policy"])]
        sections.append(
            render_benchmark(
                sec_name,
                service,
                f"All SRE Custodian controls for {service} resources.",
                controls,
            )
        )

    root_children = [f"benchmark.{section_benchmark_name('all_controls', s)}" for s in sorted(by_service)]
    root = render_benchmark(
        "all_controls",
        "All Controls",
        "All Kubernetes compliance controls from SRE Custodian scan results.",
        root_children,
        '{ category = "Compliance", service = "Kubernetes" }',
    )
    content = "# Generated all_controls benchmark\n" + root + "\n".join(sections)
    (GENERATED_DIR / "benchmark_all_controls.pp").write_text(content, encoding="utf-8")


def write_cis_v170(rules: list[dict]) -> None:
    mapped = []
    for rule in rules:
        for entry in rule["cis"]:
            if entry["version"] == "1.7.0":
                mapped.append({**rule, "cis_control_id": entry["control_id"], "cis_section": entry["section"]})
                break

    by_section: dict[str, list[dict]] = defaultdict(list)
    for rule in mapped:
        by_section[rule["cis_section"]].append(rule)

    section_blocks = []
    section_refs = []
    for section in sorted(by_section):
        sec_name = section_benchmark_name("cis_v170", section)
        section_refs.append(f"benchmark.{sec_name}")
        controls = []
        for rule in sorted(by_section[section], key=lambda r: r["cis_control_id"]):
            controls.append(
                f'control.{rule["control_name"]}  # CIS {rule["cis_control_id"]}'
            )
        section_title = {
            "1.2": "1.2 API Server",
            "5.2": "5.2 Pod Security Standards",
        }.get(section, f"{section} CIS Controls")
        section_blocks.append(
            render_benchmark(
                sec_name,
                section_title,
                f"CIS Kubernetes Benchmark v1.7.0 section {section}.",
                [c.split()[0] for c in controls],
                '{ cis = "true", cis_version = "v1.7.0" }',
            )
        )

    root = render_benchmark(
        "cis_v170",
        "CIS v1.7.0",
        "CIS Kubernetes Benchmark v1.7.0 compliance from SRE Custodian scan results.",
        section_refs,
        '{ cis = "true", cis_version = "v1.7.0", category = "Compliance", service = "Kubernetes" }',
    )
    content = "# Generated CIS v1.7.0 benchmark\n" + root + "\n".join(section_blocks)
    (GENERATED_DIR / "benchmark_cis_v170.pp").write_text(content, encoding="utf-8")


def write_cis_v120(rules: list[dict]) -> None:
    mapped = []
    for rule in rules:
        for entry in rule.get("cis_gke") or []:
            if entry["version"] == "1.2.0":
                mapped.append({**rule, "cis_control_id": entry["control_id"], "cis_section": entry["section"]})
                break

    by_section: dict[str, list[dict]] = defaultdict(list)
    for rule in mapped:
        by_section[rule["cis_section"]].append(rule)

    section_blocks = []
    section_refs = []
    for section in sorted(by_section):
        sec_name = section_benchmark_name("cis_v120", section)
        section_refs.append(f"benchmark.{sec_name}")
        controls = [f"control.{r['control_name']}" for r in sorted(by_section[section], key=lambda r: r["cis_control_id"])]
        section_blocks.append(
            render_benchmark(
                sec_name,
                f"{section} CIS Controls",
                f"CIS Kubernetes Benchmark v1.2.0 (Kubernetes v1.20) section {section}.",
                controls,
                '{ cis = "true", cis_version = "v1.20" }',
            )
        )

    root = render_benchmark(
        "cis_v120",
        "CIS Kubernetes v1.20",
        "CIS Kubernetes Benchmark v1.2.0 compliance from SRE Custodian scan results.",
        section_refs,
        '{ cis = "true", cis_version = "v1.20", category = "Compliance", service = "Kubernetes" }',
    )
    content = "# Generated CIS v1.20 benchmark\n" + root + "\n".join(section_blocks)
    (GENERATED_DIR / "benchmark_cis_v120.pp").write_text(content, encoding="utf-8")


def write_nsa_cisa(rules: list[dict]) -> None:
    by_section: dict[str, list[dict]] = defaultdict(list)
    for rule in rules:
        by_section[rule["service_section"]].append(rule)

    section_blocks = []
    section_refs = []
    for section in sorted(by_section):
        sec_name = section_benchmark_name("nsa_cisa", section)
        section_refs.append(f"benchmark.{sec_name}")
        controls = [f"control.{r['control_name']}" for r in sorted(by_section[section], key=lambda r: r["policy"])]
        section_blocks.append(
            render_benchmark(
                sec_name,
                section,
                f"NSA and CISA Kubernetes Hardening Guidance — {section}.",
                controls,
                '{ nsa_cisa = "true" }',
            )
        )

    root = render_benchmark(
        "nsa_cisa_v1",
        "NSA and CISA Kubernetes Hardening Guidance v1.0",
        "Kubernetes hardening controls from SRE Custodian scan results grouped by service section.",
        section_refs,
        '{ nsa_cisa = "true", category = "Compliance", service = "Kubernetes" }',
    )
    content = "# Generated NSA/CISA benchmark\n" + root + "\n".join(section_blocks)
    (GENERATED_DIR / "benchmark_nsa_cisa.pp").write_text(content, encoding="utf-8")


def write_mapping_json(rules: list[dict]) -> None:
    (GENERATED_DIR / "mapping.json").write_text(
        json.dumps(rules, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    import os

    metadata_dir = Path(os.environ.get("METADATA_DIR", DEFAULT_METADATA_DIR))
    if not metadata_dir.is_dir():
        raise SystemExit(f"Metadata directory not found: {metadata_dir}")

    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    rules = load_rules(metadata_dir)
    if not rules:
        raise SystemExit(f"No ecc-k8s rules found in {metadata_dir}")

    write_mapping_json(rules)
    write_controls(rules)
    write_all_controls(rules)
    write_cis_v170(rules)
    write_cis_v120(rules)
    write_nsa_cisa(rules)
    print(f"Generated {len(rules)} controls in {GENERATED_DIR}")


if __name__ == "__main__":
    main()
