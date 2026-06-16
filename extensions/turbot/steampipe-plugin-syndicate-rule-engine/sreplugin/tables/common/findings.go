package common

import "github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"

type FindingRow struct {
	JobID        string
	CustomerID   string
	RuleName     string
	Description  string
	Severity     string
	Region       string
	ResourceID   string
	ResourceName string
	Namespace    string
	Kind         string
}

func FlattenFindings(jobID, customerID string, findings map[string]sre.FindingRule) []FindingRow {
	rows := make([]FindingRow, 0)
	for ruleName, rule := range findings {
		for region, resources := range rule.Resources {
			if len(resources) == 0 {
				rows = append(rows, FindingRow{
					JobID:       jobID,
					CustomerID:  customerID,
					RuleName:    ruleName,
					Description: rule.Description,
					Severity:    rule.Severity,
					Region:      region,
				})
				continue
			}
			for _, r := range resources {
				rows = append(rows, FindingRow{
					JobID:        jobID,
					CustomerID:   customerID,
					RuleName:     ruleName,
					Description:  rule.Description,
					Severity:     rule.Severity,
					Region:       region,
					ResourceID:   r.ID,
					ResourceName: r.Name,
					Namespace:    r.Namespace,
					Kind:         r.Kind,
				})
			}
		}
	}
	return rows
}
