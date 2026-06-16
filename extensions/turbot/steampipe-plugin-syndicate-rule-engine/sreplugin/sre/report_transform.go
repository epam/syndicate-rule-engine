package sre

import "encoding/json"

func parseResourceReportItems(raw json.RawMessage) ([]ResourceReportItem, error) {
	return decodeList[ResourceReportItem](raw)
}

func findingsFromResourceItems(items []ResourceReportItem) map[string]FindingRule {
	findings := map[string]FindingRule{}
	for _, item := range items {
		region := item.Region
		if region == "" {
			region = "global"
		}
		resource := findingResourceFromData(item.Data)
		for _, rule := range item.ViolatedRules {
			if rule.Name == "" {
				continue
			}
			entry := findings[rule.Name]
			if entry.Resources == nil {
				entry.Resources = map[string][]FindingResource{}
			}
			if entry.Name == "" {
				entry.Name = rule.Name
				entry.Description = rule.Description
				entry.Severity = rule.Severity
			}
			entry.Resources[region] = append(entry.Resources[region], resource)
			findings[rule.Name] = entry
		}
	}
	return findings
}

func findingResourceFromData(data map[string]interface{}) FindingResource {
	id, name, namespace, kind := ResourceFields(data)
	return FindingResource{
		ID:        id,
		Name:      name,
		Namespace: namespace,
		Kind:      kind,
	}
}

func resourceResultsFromItems(items []ResourceReportItem) []ResourceResult {
	out := make([]ResourceResult, 0, len(items))
	for _, item := range items {
		out = append(out, item.ToResult())
	}
	return out
}
