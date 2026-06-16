package reports

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/common"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

type ruleResultRow struct {
	JobID            string
	CustomerID       string
	Policy           string
	Region           string
	Succeeded        bool
	ScannedResources *int
	FailedResources  *int
	ErrorType        string
	Reason           string
}

func TableRuleResult() *plugin.Table {
	return &plugin.Table{
		Name:             "sre_rule_result",
		Description:      "Per-rule execution statistics from SRE rules report (scanned/failed resource counts).",
		DefaultTransform: transform.FromGo(),
		List: &plugin.ListConfig{
			KeyColumns: common.JobScopeKeyColumns(),
			Hydrate:    listRuleResult,
		},
		Columns: []*plugin.Column{
			common.ColString("job_id"),
			common.ColString("customer_id"),
			common.ColString("policy"),
			common.ColString("region"),
			common.ColBool("succeeded"),
			common.ColInt("scanned_resources"),
			common.ColInt("failed_resources"),
			common.ColString("error_type"),
			common.ColString("reason"),
		},
	}
}

func listRuleResult(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	client, err := common.ClientFromQuery(ctx, d)
	if err != nil {
		return nil, err
	}
	jobID, customerID, job, err := common.JobFromQuery(ctx, client, d)
	if err != nil {
		return nil, err
	}
	items, err := client.GetRuleResults(ctx, jobID, customerID, job.JobType)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		d.StreamListItem(ctx, ruleResultRow{
			JobID:            jobID,
			CustomerID:       customerID,
			Policy:           item.Policy,
			Region:           item.Region,
			Succeeded:        item.Succeeded,
			ScannedResources: item.ScannedResources,
			FailedResources:  item.FailedResources,
			ErrorType:        item.ErrorType,
			Reason:           item.Reason,
		})
	}
	return nil, nil
}
