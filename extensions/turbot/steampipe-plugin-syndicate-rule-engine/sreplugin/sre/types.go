package sre

import "encoding/json"

type Customer struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
}

type Tenant struct {
	Name         string `json:"name"`
	CustomerName string `json:"customer_name"`
	AccountID    string `json:"account_id"`
	IsActive     bool   `json:"is_active"`
}

type K8sPlatform struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	TenantName  string `json:"tenant_name"`
	Customer    string `json:"customer"`
	Description string `json:"description"`
	Type        string `json:"type"`
	Region      string `json:"region"`
}

type Job struct {
	ID           string   `json:"id"`
	JobType      string   `json:"job_type"`
	TenantName   string   `json:"tenant_name"`
	CustomerName string   `json:"customer_name"`
	Status       string   `json:"status"`
	PlatformID   string   `json:"platform_id"`
	SubmittedAt  string   `json:"submitted_at"`
	Rulesets     []string `json:"rulesets"`
}

type ReportWrap struct {
	Data ReportData `json:"data"`
}

type ReportData struct {
	Format       string          `json:"format"`
	JobID        string          `json:"job_id"`
	JobType      string          `json:"job_type"`
	TenantName   string          `json:"tenant_name"`
	CustomerName string          `json:"customer_name"`
	Content      json.RawMessage `json:"content"`
}

type FindingRule struct {
	Name        string                       `json:"name"`
	Description string                       `json:"description"`
	Severity    string                       `json:"severity"`
	Resources   map[string][]FindingResource `json:"resources"`
}

type FindingResource struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
}

type ResourceReportItem struct {
	JobID         string                 `json:"job_id"`
	PlatformID    string                 `json:"platform_id"`
	Region        string                 `json:"region"`
	ResourceType  string                 `json:"resource_type"`
	Data          map[string]interface{} `json:"data"`
	ViolatedRules []ViolatedRule         `json:"violated_rules"`
}

func (item ResourceReportItem) ToResult() ResourceResult {
	rules := make([]ViolatedRule, len(item.ViolatedRules))
	copy(rules, item.ViolatedRules)
	return ResourceResult{
		JobID:         item.JobID,
		PlatformID:    item.PlatformID,
		ResourceType:  item.ResourceType,
		Region:        item.Region,
		Data:          item.Data,
		ViolatedRules: rules,
	}
}

type ResourceResult struct {
	JobID         string                 `json:"job_id"`
	PlatformID    string                 `json:"platform_id"`
	ResourceType  string                 `json:"resource_type"`
	Region        string                 `json:"region"`
	Data          map[string]interface{} `json:"data"`
	ViolatedRules []ViolatedRule         `json:"violated_rules"`
	LastFound     int64                  `json:"last_found"`
}

type ViolatedRule struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Severity    string `json:"severity"`
}

type RuleResultItem struct {
	Policy           string `json:"policy"`
	Region           string `json:"region"`
	Succeeded        bool   `json:"succeeded"`
	ScannedResources *int   `json:"scanned_resources"`
	FailedResources  *int   `json:"failed_resources"`
	ErrorType        string `json:"error_type"`
	Reason           string `json:"reason"`
}

