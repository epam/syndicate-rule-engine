# steampipe-mod-custodian-compliance

Powerpipe mod for SRE Custodian Kubernetes compliance benchmarks.

> **NOTE:** This implementation is not correct for benchmarks in all cases — see control logic and known limitations in the docs below.

**Full documentation:** [../README.md](../README.md)  
**Mod reference:** [docs/index.md](./docs/index.md)

## Quick start

```bash
cp powerpipe.ppvars.example powerpipe.ppvars
# Set job_id of a successful K8s scan

powerpipe benchmark run custodian_compliance.benchmark.cis_v170 --var-file powerpipe.ppvars
```

## Regenerate controls

```bash
python3 scripts/generate_mapping.py
# METADATA_DIR=... for a custom path to ecc-kubernetes-metadata
```

## Benchmarks

- `custodian_compliance.benchmark.all_controls`
- `custodian_compliance.benchmark.cis_v170`
- `custodian_compliance.benchmark.cis_v120`
- `custodian_compliance.benchmark.nsa_cisa_v1`
