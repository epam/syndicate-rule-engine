package common

import "github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"

type TenantRow struct {
	Name         string
	CustomerID   string
	CustomerName string
	AccountID    string
	IsActive     bool
}

// TenantRow maps API customer_name to customer_id column (API has no separate customer_id).
func TenantRowFrom(t sre.Tenant) TenantRow {
	return TenantRow{
		Name:         t.Name,
		CustomerID:   t.CustomerName,
		CustomerName: t.CustomerName,
		AccountID:    t.AccountID,
		IsActive:     t.IsActive,
	}
}

type PlatformRow struct {
	ID          string
	Name        string
	TenantName  string
	CustomerID  string
	Customer    string
	Description string
	Type        string
	Region      string
}

func PlatformRowFrom(p sre.K8sPlatform) PlatformRow {
	return PlatformRow{
		ID:          p.ID,
		Name:        p.Name,
		TenantName:  p.TenantName,
		CustomerID:  p.Customer,
		Customer:    p.Customer,
		Description: p.Description,
		Type:        p.Type,
		Region:      p.Region,
	}
}

type JobRow struct {
	ID           string
	JobType      string
	TenantName   string
	CustomerID   string
	CustomerName string
	Status       string
	PlatformID   string
	SubmittedAt  string
	Rulesets     []string
}

// JobRow maps API customer_name to customer_id column (API has no separate customer_id).
func JobRowFrom(job sre.Job) JobRow {
	return JobRow{
		ID:           job.ID,
		JobType:      job.JobType,
		TenantName:   job.TenantName,
		CustomerID:   job.CustomerName,
		CustomerName: job.CustomerName,
		Status:       job.Status,
		PlatformID:   job.PlatformID,
		SubmittedAt:  job.SubmittedAt,
		Rulesets:     job.Rulesets,
	}
}
