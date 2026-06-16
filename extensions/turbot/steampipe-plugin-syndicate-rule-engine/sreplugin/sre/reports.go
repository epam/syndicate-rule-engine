package sre

import "context"

func (c *Client) platformJobItems(ctx context.Context, jobID, customerID, jobType string) ([]ResourceReportItem, error) {
	raw, err := c.fetchCached(ctx, jobReportPath("resources", jobID, customerID, jobType), customerID)
	if err != nil {
		return nil, err
	}
	return parseResourceReportItems(raw)
}

func (c *Client) GetFindings(ctx context.Context, jobID, customerID, jobType, platformID string) (map[string]FindingRule, error) {
	if platformID != "" {
		items, err := c.platformJobItems(ctx, jobID, customerID, jobType)
		if err != nil {
			return nil, err
		}
		return findingsFromResourceItems(items), nil
	}

	var wrap ReportWrap
	if err := c.get(ctx, jobReportPath("findings", jobID, customerID, jobType), customerID, &wrap); err != nil {
		return nil, err
	}
	return decodeMapContent[FindingRule](wrap.Data.Content)
}

func (c *Client) GetRuleResults(ctx context.Context, jobID, customerID, jobType string) ([]RuleResultItem, error) {
	raw, err := c.fetchCached(ctx, jobReportPath("rules", jobID, customerID, jobType), customerID)
	if err != nil {
		return nil, err
	}
	return decodeList[RuleResultItem](raw)
}

func (c *Client) GetResourceResults(ctx context.Context, jobID, customerID, jobType, platformID string) ([]ResourceResult, error) {
	if platformID != "" {
		items, err := c.platformJobItems(ctx, jobID, customerID, jobType)
		if err != nil {
			return nil, err
		}
		return resourceResultsFromItems(items), nil
	}

	raw, err := c.fetchCached(ctx, jobReportPath("resources", jobID, customerID, jobType), customerID)
	if err != nil {
		return nil, err
	}
	return decodeList[ResourceResult](raw)
}
