package connection

import (
	"context"
	"fmt"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

const clientCacheKey = "sre-client"

type Config struct {
	APIURL     *string `hcl:"api_url,optional"`
	Username   *string `hcl:"username,optional"`
	Password   *string `hcl:"password,optional"`
	CustomerID *string `hcl:"customer_id,optional"`
}

func GetConfig(c *plugin.Connection) Config {
	if c != nil && c.Config != nil {
		switch cfg := c.Config.(type) {
		case Config:
			return cfg
		case *Config:
			if cfg != nil {
				return *cfg
			}
		}
	}
	return Config{}
}

func Client(ctx context.Context, d *plugin.QueryData) (*sre.Client, error) {
	if cached, ok := d.ConnectionCache.Get(ctx, clientCacheKey); ok {
		client, ok := cached.(*sre.Client)
		if ok {
			return client, nil
		}
		return nil, fmt.Errorf("invalid cached client type %T", cached)
	}

	cfg := GetConfig(d.Connection)
	client := sre.NewClient(sre.ClientConfig{
		APIURL:   StrVal(cfg.APIURL),
		Username: StrVal(cfg.Username),
		Password: StrVal(cfg.Password),
	})
	if err := d.ConnectionCache.Set(ctx, clientCacheKey, client); err != nil {
		return nil, err
	}
	return client, nil
}

func StrVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
