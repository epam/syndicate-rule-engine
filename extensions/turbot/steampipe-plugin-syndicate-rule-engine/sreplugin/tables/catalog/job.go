package catalog

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/common"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func TableJob() *plugin.Table {
	return &plugin.Table{
		Name:             "sre_job",
		Description:      "SRE scan jobs (filter by customer_id, tenant_name and platform_id for dashboard selection).",
		DefaultTransform: transform.FromGo(),
		List: &plugin.ListConfig{
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "customer_id", Require: plugin.Optional},
				{Name: "tenant_name", Require: plugin.Optional},
				{Name: "platform_id", Require: plugin.Optional},
			},
			Hydrate: listJob,
		},
		Columns: []*plugin.Column{
			common.ColString("id"),
			common.ColString("job_type"),
			common.ColString("tenant_name"),
			common.ColString("customer_id"),
			common.ColString("customer_name"),
			common.ColString("status"),
			common.ColString("platform_id"),
			common.ColString("submitted_at"),
			common.ColJSON("rulesets"),
		},
	}
}

func listJob(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	customerID := common.CustomerID(d)
	tenantName := common.QualString(d, "tenant_name")
	platformID := common.QualString(d, "platform_id")
	return nil, common.StreamMappedList(ctx, d,
		func(ctx context.Context, c *sre.Client) ([]sre.Job, error) {
			return c.ListJobs(ctx, customerID, tenantName, platformID, common.JobStatusSucceeded)
		},
		common.JobRowFrom,
	)
}
