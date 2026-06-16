package catalog

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/common"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func TableCustomer() *plugin.Table {
	return &plugin.Table{
		Name:             "sre_customer",
		Description:      "SRE customers (for system users selecting scope in the dashboard).",
		DefaultTransform: transform.FromGo(),
		List:             &plugin.ListConfig{Hydrate: listCustomer},
		Columns: []*plugin.Column{
			common.ColString("name", "Customer ID."),
			common.ColString("display_name"),
		},
	}
}

func listCustomer(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	return nil, common.StreamMappedList(ctx, d,
		func(ctx context.Context, c *sre.Client) ([]sre.Customer, error) {
			return c.ListCustomers(ctx)
		},
		func(c sre.Customer) sre.Customer { return c },
	)
}
