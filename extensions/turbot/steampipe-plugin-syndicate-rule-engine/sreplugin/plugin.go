package sreplugin

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/connection"
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/catalog"
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/tables/reports"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func Plugin(ctx context.Context) *plugin.Plugin {
	return &plugin.Plugin{
		Name: "syndicate-rule-engine",
		ConnectionConfigSchema: &plugin.ConnectionConfigSchema{
			NewInstance: func() interface{} { return &connection.Config{} },
		},
		TableMap: map[string]*plugin.Table{
			"sre_customer":           catalog.TableCustomer(),
			"sre_tenant":             catalog.TableTenant(),
			"sre_platform":           catalog.TablePlatform(),
			"sre_job":                catalog.TableJob(),
			"sre_finding":         reports.TableFinding(),
			"sre_resource_result": reports.TableResourceResult(),
			"sre_rule_result":     reports.TableRuleResult(),
		},
	}
}
