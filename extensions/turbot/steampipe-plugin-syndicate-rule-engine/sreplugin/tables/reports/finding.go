package reports

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/common"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func TableFinding() *plugin.Table {
	return &plugin.Table{
		Name:             "sre_finding",
		Description:      "Violated resources per Custodian rule from SRE findings report.",
		DefaultTransform: transform.FromGo(),
		List: &plugin.ListConfig{
			KeyColumns: common.JobScopeKeyColumns(),
			Hydrate:    listFinding,
		},
		Columns: []*plugin.Column{
			common.ColString("job_id"),
			common.ColString("customer_id"),
			common.ColString("rule_name"),
			common.ColString("description"),
			common.ColString("severity"),
			common.ColString("region"),
			common.ColString("resource_id"),
			common.ColString("resource_name"),
			common.ColString("namespace"),
			common.ColString("kind"),
		},
	}
}

func listFinding(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	client, err := common.ClientFromQuery(ctx, d)
	if err != nil {
		return nil, err
	}
	jobID, customerID, job, err := common.JobFromQuery(ctx, client, d)
	if err != nil {
		return nil, err
	}
	findings, err := client.GetFindings(ctx, jobID, customerID, job.JobType, job.PlatformID)
	if err != nil {
		return nil, err
	}
	for _, row := range common.FlattenFindings(jobID, customerID, findings) {
		d.StreamListItem(ctx, row)
	}
	return nil, nil
}
