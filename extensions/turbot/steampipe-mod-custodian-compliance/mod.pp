mod "custodian_compliance" {
  title         = "Custodian Compliance (SRE)"
  description   = "Kubernetes compliance dashboards and benchmarks from SRE Custodian scan results via the syndicate-rule-engine Steampipe plugin."
  color         = "#0089D6"
  documentation = file("./docs/index.md")
  categories    = ["kubernetes", "compliance", "cis"]
}