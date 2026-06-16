package common

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/connection"
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func ClientFromQuery(ctx context.Context, d *plugin.QueryData) (*sre.Client, error) {
	return connection.Client(ctx, d)
}
