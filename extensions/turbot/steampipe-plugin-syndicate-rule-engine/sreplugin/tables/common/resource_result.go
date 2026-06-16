package common

import "github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"

type ResourceResultRow struct {
	JobID          string
	CustomerID     string
	PlatformID     string
	ResourceType   string
	Region         string
	ResourceID     string
	ResourceName   string
	Namespace      string
	ViolatedRules  interface{}
	ViolationCount int
	Status         string
}

func ResourceResultRowFrom(item sre.ResourceResult, customerID string) ResourceResultRow {
	id, name, namespace, _ := sre.ResourceFields(item.Data)
	status := "ok"
	if len(item.ViolatedRules) > 0 {
		status = "alarm"
	}
	return ResourceResultRow{
		JobID:          item.JobID,
		CustomerID:     customerID,
		PlatformID:     item.PlatformID,
		ResourceType:   item.ResourceType,
		Region:         item.Region,
		ResourceID:     id,
		ResourceName:   name,
		Namespace:      namespace,
		ViolatedRules:  item.ViolatedRules,
		ViolationCount: len(item.ViolatedRules),
		Status:         status,
	}
}
