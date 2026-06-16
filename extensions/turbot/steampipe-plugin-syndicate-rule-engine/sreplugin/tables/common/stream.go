package common

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func StreamMappedList[T, R any](
	ctx context.Context,
	d *plugin.QueryData,
	fetch func(context.Context, *sre.Client) ([]T, error),
	mapFn func(T) R,
) error {
	client, err := ClientFromQuery(ctx, d)
	if err != nil {
		return err
	}
	items, err := fetch(ctx, client)
	if err != nil {
		return err
	}
	for _, item := range items {
		d.StreamListItem(ctx, mapFn(item))
	}
	return nil
}
