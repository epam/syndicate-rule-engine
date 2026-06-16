package reports

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/common"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func TableResourceResult() *plugin.Table {
	return &plugin.Table{
		Name:             "sre_resource_result",
		Description:      "Per-resource scan results with violated rules from SRE.",
		DefaultTransform: transform.FromGo(),
		List: &plugin.ListConfig{
			KeyColumns: common.JobScopeKeyColumns(),
			Hydrate:    listResourceResult,
		},
		Columns: []*plugin.Column{
			common.ColString("job_id"),
			common.ColString("customer_id"),
			common.ColString("platform_id"),
			common.ColString("resource_type"),
			common.ColString("region"),
			common.ColString("resource_id"),
			common.ColString("resource_name"),
			common.ColString("namespace"),
			common.ColJSON("violated_rules"),
			common.ColInt("violation_count"),
			common.ColString("status"),
		},
	}
}

func listResourceResult(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	client, err := common.ClientFromQuery(ctx, d)
	if err != nil {
		return nil, err
	}
	jobID, customerID, job, err := common.JobFromQuery(ctx, client, d)
	if err != nil {
		return nil, err
	}
	items, err := client.GetResourceResults(ctx, jobID, customerID, job.JobType, job.PlatformID)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		d.StreamListItem(ctx, common.ResourceResultRowFrom(item, customerID))
	}
	return nil, nil
}
