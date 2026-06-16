package common

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func ColString(name string, description ...string) *plugin.Column {
	col := &plugin.Column{Name: name, Type: proto.ColumnType_STRING}
	if len(description) > 0 {
		col.Description = description[0]
	}
	return col
}

func ColBool(name string, description ...string) *plugin.Column {
	col := &plugin.Column{Name: name, Type: proto.ColumnType_BOOL}
	if len(description) > 0 {
		col.Description = description[0]
	}
	return col
}

func ColJSON(name string) *plugin.Column {
	return &plugin.Column{Name: name, Type: proto.ColumnType_JSON}
}

func ColInt(name string) *plugin.Column {
	return &plugin.Column{Name: name, Type: proto.ColumnType_INT}
}

func JobScopeKeyColumns() plugin.KeyColumnSlice {
	return plugin.KeyColumnSlice{
		{Name: "job_id", Require: plugin.Required},
		{Name: "customer_id", Require: plugin.Optional},
	}
}

const JobStatusSucceeded = "SUCCEEDED"
