package main

import (
	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{PluginFunc: sreplugin.Plugin})
}
