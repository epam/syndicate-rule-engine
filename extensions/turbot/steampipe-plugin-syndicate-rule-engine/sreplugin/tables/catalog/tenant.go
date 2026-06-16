package catalog

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/common"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func TableTenant() *plugin.Table {
	return &plugin.Table{
		Name:             "sre_tenant",
		Description:      "SRE tenants available to the authenticated user.",
		DefaultTransform: transform.FromGo(),
		List: &plugin.ListConfig{
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "customer_id", Require: plugin.Optional},
			},
			Hydrate: listTenant,
		},
		Columns: []*plugin.Column{
			common.ColString("name", "Tenant name."),
			common.ColString("customer_id", "Customer ID (required for system users)."),
			common.ColString("customer_name"),
			common.ColString("account_id"),
			common.ColBool("is_active"),
		},
	}
}

func listTenant(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	customerID := common.CustomerID(d)
	return nil, common.StreamMappedList(ctx, d,
		func(ctx context.Context, c *sre.Client) ([]sre.Tenant, error) {
			return c.ListTenants(ctx, customerID)
		},
		common.TenantRowFrom,
	)
}
