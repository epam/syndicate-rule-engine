locals {
  common_tags = {
    category = "Compliance"
    plugin   = "syndicate-rule-engine"
    service  = "Kubernetes"
    type     = "Control"
  }
}

variable "job_id" {
  type        = string
  description = "SRE scan job UUID (K8s platform job with SUCCEEDED status)."
  default     = ""
}

variable "customer_id" {
  type        = string
  description = "Optional customer scope for system users (maps to SRE API customer_id)."
  default     = ""
}
