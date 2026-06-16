package catalog

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/common"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func TablePlatform() *plugin.Table {
	return &plugin.Table{
		Name:             "sre_platform",
		Description:      "Registered K8s platforms in SRE.",
		DefaultTransform: transform.FromGo(),
		List: &plugin.ListConfig{
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "customer_id", Require: plugin.Optional},
				{Name: "tenant_name", Require: plugin.Optional},
			},
			Hydrate: listPlatform,
		},
		Columns: []*plugin.Column{
			common.ColString("id"),
			common.ColString("name"),
			common.ColString("tenant_name"),
			common.ColString("customer_id"),
			common.ColString("customer"),
			common.ColString("description"),
			common.ColString("type"),
			common.ColString("region"),
		},
	}
}

func listPlatform(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	customerID := common.CustomerID(d)
	tenantName := common.QualString(d, "tenant_name")
	return nil, common.StreamMappedList(ctx, d,
		func(ctx context.Context, c *sre.Client) ([]sre.K8sPlatform, error) {
			return c.ListPlatforms(ctx, customerID, tenantName)
		},
		common.PlatformRowFrom,
	)
}
