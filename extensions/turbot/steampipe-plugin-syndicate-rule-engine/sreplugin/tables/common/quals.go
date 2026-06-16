package common

import (
	"fmt"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/connection"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// CustomerID returns the customer_id qual or the connection default (required for system users).
func CustomerID(d *plugin.QueryData) string {
	if id := QualString(d, "customer_id"); id != "" {
		return id
	}
	return connection.StrVal(connection.GetConfig(d.Connection).CustomerID)
}

func QualString(d *plugin.QueryData, name string) string {
	if d == nil || d.EqualsQuals == nil {
		return ""
	}
	if q := d.EqualsQuals[name]; q != nil {
		return q.GetStringValue()
	}
	return ""
}

func RequireQualString(d *plugin.QueryData, name string) (string, error) {
	v := QualString(d, name)
	if v == "" {
		return "", fmt.Errorf("required qual %q is missing", name)
	}
	return v, nil
}
